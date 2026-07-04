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

package pool

import "github.com/cnmsql/cnmsql/pkg/management/mysql/version"

// ControlParams describes how the instance manager reaches its local mysqld for
// control and monitoring.
type ControlParams struct {
	// User and Password must belong to a privileged account (CONNECTION_ADMIN /
	// SUPER) so it can use the reserved connection slot when client connections
	// are exhausted.
	User     string
	Password string
	// Socket is the local unix socket, used on servers without the admin
	// interface.
	Socket string
	// AdminAddress and AdminPort locate the administrative interface; empty
	// values fall back to loopback / the default admin port.
	AdminAddress string
	AdminPort    int
}

// ControlConfig builds the connection config the instance manager uses to reach
// its local mysqld for control and monitoring. On MySQL 8.0.14+ it targets the
// administrative interface, which is exempt from max_connections; on older
// servers it falls back to the unix socket and relies on the reserved
// privileged connection slot. Either way the pool is capped at a single
// connection so that reserved slot is never wasted.
func ControlConfig(v version.Version, p ControlParams) Config {
	cfg := Config{
		User:         p.User,
		Password:     p.Password,
		MaxOpenConns: 1,
	}

	// TODO(M-MDB.2): replace with eng.HasAdminInterface(v) once the call site
	// (instance/runner.go) passes the bool. pool cannot import engine directly
	// because engine → replication → pool creates a cycle.
	if v.HasAdminInterface() {
		addr := p.AdminAddress
		if addr == "" {
			addr = "127.0.0.1"
		}
		port := p.AdminPort
		if port == 0 {
			port = 33062
		}
		cfg.Host = addr
		cfg.Port = port
		return cfg
	}

	cfg.Socket = p.Socket
	return cfg
}
