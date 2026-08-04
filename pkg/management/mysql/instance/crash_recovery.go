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

package instance

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
)

// forceRecoveryMarker is the file written to the data directory to record that
// the instance started with innodb_force_recovery so subsequent restarts can
// escalate the level, and so the operator can surface it in status.
const forceRecoveryMarker = ".cnmsql_force_recovery"

// crashRecoveryLogPatterns are substrings that, when found in the mysqld error
// output, indicate InnoDB page corruption that innodb_force_recovery can
// address.
var crashRecoveryLogPatterns = []string{
	"InnoDB: Database page corruption",
	"InnoDB: Page checksum mismatch",
	"InnoDB: unable to read a page",
	"InnoDB: Corruption in the file system",
	"Database page corruption on disk",
}

// shouldAttemptForceRecovery reports whether the mysqld error output indicates
// InnoDB corruption that innodb_force_recovery can address.
func shouldAttemptForceRecovery(stderr string) bool {
	for _, pattern := range crashRecoveryLogPatterns {
		if strings.Contains(stderr, pattern) {
			return true
		}
	}
	return false
}

// readForceRecoveryMarker reads the force-recovery marker from the data
// directory, returning the level that was applied and whether the marker
// existed. Level 0 means no force recovery has been attempted.
func readForceRecoveryMarker(dataDir string) (level int, exists bool) {
	data, err := os.ReadFile(markerPath(dataDir))
	if err != nil {
		return 0, false
	}
	var n int
	if _, err := fmt.Sscanf(string(data), "%d", &n); err != nil {
		return 0, true
	}
	return n, true
}

// writeForceRecoveryMarker records the force-recovery level in the data
// directory so a subsequent restart can escalate or clear it.
func writeForceRecoveryMarker(dataDir string, level int) error {
	content := fmt.Sprintf("%d\n", level)
	return os.WriteFile(markerPath(dataDir), []byte(content), 0o644)
}

// clearForceRecoveryMarker removes the force-recovery marker after a successful
// clean start so the instance resumes normal operation on the next restart.
func clearForceRecoveryMarker(dataDir string) {
	_ = os.Remove(markerPath(dataDir))
}

// nextForceRecoveryLevel returns the next innodb_force_recovery level to try,
// escalating 1 → 2 → 3 across successive attempts. Returns 0 when all safe
// levels are exhausted (the instance must be re-cloned).
func nextForceRecoveryLevel(current int) int {
	switch current {
	case 0:
		return 1
	case 1:
		return 2
	case 2:
		return 3
	default:
		return 0
	}
}

func markerPath(dataDir string) string {
	return dataDir + "/" + forceRecoveryMarker
}

// escalateForceRecovery writes or escalates the force-recovery marker when
// mysqld fails to start. On the first failure (hadMarker=false, level=0) it
// writes level 1; on subsequent failures it escalates 1→2→3. When all safe
// levels are exhausted (level 3 was tried and failed) it stops escalating —
// the controller's auto-reinit will re-clone the instance from a healthy
// primary.
func escalateForceRecovery(log logr.Logger, dataDir string, hadMarker bool, currentLevel int) {
	next := 0
	if !hadMarker {
		next = 1
	} else {
		next = nextForceRecoveryLevel(currentLevel)
	}
	if next == 0 {
		log.Info("All innodb_force_recovery levels exhausted; instance must be re-cloned",
			"lastLevel", currentLevel)
		return
	}
	log.Info("Writing innodb_force_recovery marker for next restart",
		"level", next, "previousLevel", currentLevel)
	if err := writeForceRecoveryMarker(dataDir, next); err != nil {
		log.Error(err, "Failed to write force-recovery marker")
	}
}
