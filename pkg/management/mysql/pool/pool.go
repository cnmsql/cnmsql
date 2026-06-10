/*
Copyright 2026 The CNMySQL Authors.

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

// Package pool manages the instance manager's connection to its local mysqld.
// The manager always talks to mysqld over the local unix socket (or loopback
// TCP), so this connection is not TLS-protected; mTLS applies to replication
// and to the operator control API, handled elsewhere.
package pool

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"sort"
	"strings"
	"time"

	// Register the MySQL driver for database/sql.
	_ "github.com/go-sql-driver/mysql"
)

// Connection is the subset of *sql.DB used across the management packages. It
// lets callers be tested with fakes or sqlmock.
type Connection interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PingContext(ctx context.Context) error
}

// Config describes how to reach the local mysqld.
type Config struct {
	// Socket is the unix socket path. When set it takes precedence over Host.
	Socket string
	// Host and Port are used for loopback TCP when Socket is empty.
	Host string
	Port int
	// User and Password authenticate to mysqld.
	User     string
	Password string
	// Database is the default schema; empty means none.
	Database string
	// Params are extra DSN parameters merged with the defaults.
	Params map[string]string
	// MaxOpenConns caps the pool size. The instance manager's control pool
	// should set this to 1: on servers without the admin interface (pre-8.0.14)
	// a single privileged connection reliably owns the one reserved
	// SUPER/CONNECTION_ADMIN slot that survives max_connections exhaustion,
	// instead of wasting it across idle connections. Defaults to 2.
	MaxOpenConns int
}

// DefaultMaxOpenConns is used when Config.MaxOpenConns is zero.
const DefaultMaxOpenConns = 2

// defaultParams are always applied unless overridden by Config.Params.
func defaultParams() map[string]string {
	return map[string]string{
		"parseTime":            "true",
		"interpolateParams":    "true",
		"timeout":              "10s",
		"readTimeout":          "30s",
		"writeTimeout":         "30s",
		"multiStatements":      "false",
		"rejectReadOnly":       "false",
		"allowNativePasswords": "true",
	}
}

// DSN builds a go-sql-driver/mysql data source name from the config. It is pure
// and unit-testable; it does not open a connection.
func (c Config) DSN() (string, error) {
	if c.User == "" {
		return "", fmt.Errorf("pool: user is required")
	}
	if c.Socket == "" && c.Host == "" {
		return "", fmt.Errorf("pool: either socket or host is required")
	}

	var net, addr string
	if c.Socket != "" {
		net, addr = "unix", c.Socket
	} else {
		port := c.Port
		if port == 0 {
			port = 3306
		}
		net, addr = "tcp", fmt.Sprintf("%s:%d", c.Host, port)
	}

	auth := url.QueryEscape(c.User)
	if c.Password != "" {
		auth += ":" + url.QueryEscape(c.Password)
	}

	params := defaultParams()
	maps.Copy(params, c.Params)

	var query strings.Builder
	first := true
	for _, k := range sortedKeys(params) {
		if first {
			query.WriteByte('?')
			first = false
		} else {
			query.WriteByte('&')
		}
		query.WriteString(k)
		query.WriteByte('=')
		query.WriteString(params[k])
	}

	return fmt.Sprintf("%s@%s(%s)/%s%s", auth, net, addr, c.Database, query.String()), nil
}

// Open opens a *sql.DB to the local mysqld and verifies connectivity within the
// given timeout.
func Open(ctx context.Context, c Config) (*sql.DB, error) {
	dsn, err := c.DSN()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("pool: opening connection: %w", err)
	}

	maxOpen := c.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = DefaultMaxOpenConns
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pool: pinging mysqld: %w", err)
	}

	return db, nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
