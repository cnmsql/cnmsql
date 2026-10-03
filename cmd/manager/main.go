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

// Command manager is the in-pod instance manager for cnmsql. It runs as PID1
// inside every MySQL pod, supervises mysqld, bootstraps and joins instances,
// drives GTID replication, and exposes a control API to the operator.
package main

import (
	"context"
	"errors"
	"os"

	"github.com/go-logr/logr"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/cnmsql/cnmsql/internal/cmd/manager"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/instance"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog"
)

// newLogger builds the manager's logger. It writes through slowlog.Output, the
// stderr writer the slow query records also use, so the two never interleave
// inside one line.
func newLogger() logr.Logger {
	return zap.New(zap.UseDevMode(false), zap.WriteTo(slowlog.Output))
}

func main() {
	ctrl.SetLogger(newLogger())
	setupLog := ctrl.Log.WithName("setup")

	cmd := manager.NewRootCommand()
	cmd.SetContext(logf.IntoContext(context.Background(), ctrl.Log.WithName("instance-manager")))
	if err := cmd.Execute(); err != nil {
		setupLog.Error(err, "Command failed")
		if errors.Is(err, instance.ErrNonRetryable) {
			os.Exit(instance.ExitCodeNonRetryable)
		}
		os.Exit(1)
	}
}
