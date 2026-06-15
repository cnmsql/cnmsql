/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package pool

import "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"

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
