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

// Package backupworker holds the contract between the operator and the worker
// Jobs it renders (backup upload, logical restore): the environment a worker
// reads and the termination message it leaves when it fails.
package backupworker

import (
	"encoding/json"
	"os"
)

// Failure reasons a logical backup worker reports on top of the ones the
// source instance returns (webserver.DumpReason*).
const (
	// ReasonInstanceManagerOutdated: the source's instance manager has no
	// /cluster/dump yet (the operator upgrade has not reached it).
	ReasonInstanceManagerOutdated = "InstanceManagerOutdated"
	// ReasonDumpFailed: the dump client failed, or the stream was cut short.
	ReasonDumpFailed = "DumpFailed"
)

// Failure reasons a logical restore worker reports on top of the ones the
// target instance returns (webserver.LoadReason*).
const (
	// ReasonIncompatible: the dump does not fit the target cluster (flavor,
	// format, a selected database missing from it).
	ReasonIncompatible = "Incompatible"
	// ReasonDownloadFailed: the dump or its manifest could not be read from
	// the object store.
	ReasonDownloadFailed = "DownloadFailed"
	// ReasonDumpCorrupt: the dump does not match its manifest's checksum, has
	// no completion footer, or does not decode.
	ReasonDumpCorrupt = "DumpCorrupt"
	// ReasonTargetUnreachable: the connection or the TLS handshake to the
	// target instance failed, before the instance saw the request.
	ReasonTargetUnreachable = "TargetUnreachable"
)

// TerminationMessage is what the worker writes to its container's termination
// message when it fails with a known reason. The operator reads it back to fail
// the Backup with that reason and message instead of the Job's generic one.
type TerminationMessage struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
	// Unchanged is set by a restore worker that failed before the target
	// changed anything, so the operator can say the data is untouched.
	Unchanged bool `json:"unchanged,omitempty"`
}

// MaxTerminationMessageBytes is the kubelet's limit on a termination message.
const MaxTerminationMessageBytes = 4096

// TerminationLogPath is the kubelet's default terminationMessagePath.
const TerminationLogPath = "/dev/termination-log"

// WriteTerminationMessage writes msg to path, shortening its message until it
// fits the kubelet's limit. It is best effort: the worker is exiting anyway.
func WriteTerminationMessage(path string, msg TerminationMessage) {
	payload, err := json.Marshal(msg)
	for err == nil && len(payload) > MaxTerminationMessageBytes && len(msg.Message) > 0 {
		msg.Message = msg.Message[:len(msg.Message)/2]
		payload, err = json.Marshal(msg)
	}
	if err != nil {
		return
	}
	_ = os.WriteFile(path, payload, 0o644)
}
