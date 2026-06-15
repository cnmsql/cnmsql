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

// Command manager is the in-pod instance manager for cloudnative-mysql. It runs as PID1
// inside every MySQL pod, supervises mysqld, bootstraps and joins instances,
// drives GTID replication, and exposes a control API to the operator.
package main

import (
	"context"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager"
)

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	setupLog := ctrl.Log.WithName("setup")

	cmd := manager.NewRootCommand()
	cmd.SetContext(logf.IntoContext(context.Background(), ctrl.Log.WithName("instance-manager")))
	if err := cmd.Execute(); err != nil {
		setupLog.Error(err, "Command failed")
		os.Exit(1)
	}
}
