//go:build integration

/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// secureTransportCnf requires TLS on every TCP connection, as a user does with
// spec.mysql.parameters, and gives the server the certificate to offer it.
const secureTransportCnf = `require_secure_transport=ON
ssl_ca=/pki/ca.crt
ssl_cert=/pki/server.crt
ssl_key=/pki/server.key
`

// TestRunWithRequireSecureTransport verifies the instance manager keeps its
// control connection on the dedicated listener (the MySQL admin interface or
// MariaDB's extra_port) when require_secure_transport=ON. The server then
// refuses plaintext TCP on those listeners too, so the manager has to speak TLS
// there: in plaintext, MySQL never becomes ready and MariaDB stays on the
// socket.
func TestRunWithRequireSecureTransport(t *testing.T) {
	for _, img := range logicalImages(t) {
		t.Run(img.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			img.cnf += secureTransportCnf
			if img.flavor == engine.FlavorMariaDB {
				img.cnf += "extra_port=33062\nextra_max_connections=4\n"
			}
			img.files = pkiContainerFiles(t)
			node := startLogicalNode(ctx, t, img, nil)

			hosts := strings.Fields(node.sql(ctx, t,
				"SELECT HOST FROM information_schema.PROCESSLIST WHERE USER = 'control';"))
			if len(hosts) == 0 {
				t.Fatal("no control connection is open")
			}
			// A TCP connection is listed as host:port, a socket one as a bare
			// "localhost".
			for _, host := range hosts {
				if !strings.Contains(host, ":") {
					t.Errorf("control connection comes from %q, want a TCP host:port", host)
				}
			}
		})
	}
}
