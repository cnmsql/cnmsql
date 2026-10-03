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

package slowlog

import (
	"io"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggerName names the slow log's records.
const LoggerName = "mysqld.slowlog"

// NewRecordLogger returns the logger slow query records go through: JSON in
// the instance manager's format (level, RFC 3339 ts, logger, msg), written to
// w, and never sampled. The manager's own logger keeps 100 identical
// messages per second and then 1 in 100, which would drop most records of a
// busy server, since they all share one message.
func NewRecordLogger(w io.Writer) logr.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.Lock(zapcore.AddSync(w)), zapcore.InfoLevel)
	return zapr.NewLogger(zap.New(core)).WithName(LoggerName)
}
