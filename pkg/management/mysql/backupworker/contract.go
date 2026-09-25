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

// Package backupworker holds the contract between the operator and the backup
// worker Job it renders: the environment the worker reads and the termination
// message it leaves when it fails.
package backupworker

// EnvDumpPassword carries the cnmsql_dump password into a logical backup
// worker, from the cluster's <cluster>-dump Secret.
const EnvDumpPassword = "CNMSQL_DUMP_PASSWORD"

// Failure reasons a logical backup worker reports on top of the ones the
// source instance returns (webserver.DumpReason*).
const (
	// ReasonInstanceManagerOutdated: the source's instance manager has no
	// /cluster/dump yet (the operator upgrade has not reached it).
	ReasonInstanceManagerOutdated = "InstanceManagerOutdated"
	// ReasonDumpFailed: the dump client failed, or the stream was cut short.
	ReasonDumpFailed = "DumpFailed"
)

// TerminationMessage is what the worker writes to its container's termination
// message when it fails with a known reason. The operator reads it back to fail
// the Backup with that reason and message instead of the Job's generic one.
type TerminationMessage struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// MaxTerminationMessageBytes is the kubelet's limit on a termination message.
const MaxTerminationMessageBytes = 4096
