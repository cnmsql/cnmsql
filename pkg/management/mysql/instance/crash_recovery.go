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

// CorruptionSentinel prefixes the Pod termination message written when mysqld
// cannot start because its InnoDB data is damaged. Kubernetes surfaces whatever
// the container writes to its termination-message path in the Pod's
// lastState.terminated.message, which is how the operator distinguishes
// irrecoverable data damage from an ordinary failed start (a slow boot, a bad
// config, an OOM kill) without needing to read the data volume.
//
// The operator matches on this exact string; changing it changes the contract
// with internal/controller's auto-reinit.
const CorruptionSentinel = "CNMSQL_INNODB_CORRUPTION"

// terminationLogPath is the default Kubernetes termination-message path. A Pod
// spec that overrides terminationMessagePath would need this changed to match;
// the operator does not override it.
const terminationLogPath = "/dev/termination-log"

// corruptionMarker records, on the data volume, that this instance's InnoDB
// data was diagnosed as corrupt. It survives Pod restarts, so a crash-looping
// instance keeps reporting the diagnosis even on restarts where mysqld fails
// too early to print it again.
const corruptionMarker = ".cnmsql_innodb_corruption"

// innodbCorruptionPatterns are phrases that, in mysqld's output, mean InnoDB
// found damaged data rather than failing to start for an environmental reason.
//
// They are stored lowercased and matched case-insensitively against the bare
// message, without the subsystem prefix. That prefix is not stable across the
// engines this operator supports: MySQL 8.0 tags the line "[InnoDB] Database
// page corruption ...", while MySQL 5.7 and MariaDB write "InnoDB: Database page
// corruption ...". Matching the phrase alone covers all of them.
//
// These track upstream's English error text, so a wording or locale change makes
// a pattern miss. That fails safe: the instance falls back to the operator's
// generic crash-loop handling instead of taking a wrong action.
var innodbCorruptionPatterns = []string{
	"database page corruption",
	"page checksum mismatch",
	"unable to read page",
	"unable to read a page",
	"corruption in the file system",
	"cannot open datafile",
	"corrupted page identifier",
	"your database may be corrupt",
	"table is corrupt",
	"tablespace is corrupt",
}

// indicatesInnoDBCorruption reports whether mysqld's output shows InnoDB data
// damage. It is deliberately narrow: the caller acts on a positive result by
// asking the operator to discard this instance's data and re-clone it, so a
// false positive is expensive. Anything that merely means "mysqld did not come
// up" must not match.
func indicatesInnoDBCorruption(mysqldOutput string) bool {
	return matchesCorruptionPattern(strings.ToLower(mysqldOutput))
}

// matchesCorruptionPattern reports whether already-lowercased text contains any
// corruption phrase.
func matchesCorruptionPattern(lowered string) bool {
	for _, pattern := range innodbCorruptionPatterns {
		if strings.Contains(lowered, pattern) {
			return true
		}
	}
	return false
}

// reportCorruption records an InnoDB corruption diagnosis so it survives both
// the process exit and subsequent Pod restarts: it writes the sentinel to the
// Pod's termination message (where the operator reads it) and drops a marker on
// the data volume (so a later restart that fails before printing anything still
// reports the diagnosis).
//
// Every step is best-effort. Failing to record the diagnosis must never mask the
// startup error the caller is about to return; the instance then falls back to
// the operator's generic crash-loop handling.
func reportCorruption(log logr.Logger, dataDir, mysqldOutput string) {
	log.Error(nil, "mysqld cannot start: InnoDB reports corrupt data. "+
		"The operator will re-clone this instance from a healthy primary; "+
		"a primary with no healthy replica needs manual salvage",
		"evidence", corruptionEvidence(mysqldOutput))

	if err := os.WriteFile(markerPath(dataDir), []byte(CorruptionSentinel+"\n"), 0o644); err != nil {
		log.Error(err, "Could not record the corruption marker on the data volume")
	}
	writeTerminationMessage(log, corruptionEvidence(mysqldOutput))
}

// writeTerminationMessage publishes the corruption diagnosis to the Pod's
// termination message. Truncated to stay under the 4 KiB Kubernetes retains.
func writeTerminationMessage(log logr.Logger, evidence string) {
	msg := fmt.Sprintf("%s: mysqld cannot start, InnoDB data is corrupt: %s\n",
		CorruptionSentinel, evidence)
	if len(msg) > 3072 {
		msg = msg[:3072]
	}
	if err := os.WriteFile(terminationLogPath, []byte(msg), 0o644); err != nil {
		// Expected outside Kubernetes (unit tests, local runs), where the path
		// does not exist. The marker on the data volume still carries the
		// diagnosis, so this is informational only.
		log.V(1).Info("Could not write the Pod termination message",
			"path", terminationLogPath, "error", err.Error())
	}
}

// corruptionEvidence extracts the matching lines from mysqld's output so the
// logged diagnosis and the termination message name the actual error rather than
// just asserting corruption.
func corruptionEvidence(mysqldOutput string) string {
	var matched []string
	for line := range strings.SplitSeq(mysqldOutput, "\n") {
		if matchesCorruptionPattern(strings.ToLower(line)) {
			matched = append(matched, strings.TrimSpace(line))
		}
	}
	if len(matched) == 0 {
		return "no matching diagnostic line"
	}
	// The first few lines carry the diagnosis; later ones repeat it per page.
	if len(matched) > 3 {
		matched = matched[:3]
	}
	return strings.Join(matched, " | ")
}

// hasCorruptionMarker reports whether a previous start on this data volume
// diagnosed InnoDB corruption.
func hasCorruptionMarker(dataDir string) bool {
	_, err := os.Stat(markerPath(dataDir))
	return err == nil
}

// clearCorruptionMarker removes the marker after mysqld starts cleanly, so a
// data volume that was replaced (or an instance whose failure turned out to be
// environmental) stops reporting a stale diagnosis.
func clearCorruptionMarker(dataDir string) {
	_ = os.Remove(markerPath(dataDir))
}

func markerPath(dataDir string) string {
	return dataDir + "/" + corruptionMarker
}
