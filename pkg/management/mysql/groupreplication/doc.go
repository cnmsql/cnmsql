/*
Copyright 2026 The CloudNative MySQL Authors.

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

// Package groupreplication drives MySQL Group Replication from the instance
// manager: bootstrapping a group exactly once, starting and stopping the local
// member, performing a planned switchover via group_replication_set_as_primary,
// and reading performance_schema.replication_group_members so the operator can
// observe the group's elected primary and quorum.
//
// It is the Group Replication counterpart of the asynchronous-replication
// package: a thin, version-aware executor and reader over a pool.Connection. The
// dangerous operations — bootstrap (exactly-once) and group_replication_force_members
// (quorum recovery) — live here behind explicit methods so the callers that gate
// them (the in-Pod reconciler and the operator) remain the only place policy is
// decided.
//
// This package is introduced as a skeleton in milestone M-GR.1 and carries no
// runtime wiring yet; later phases connect it to the in-Pod GR role strategy and
// the operator's observe loop.
package groupreplication
