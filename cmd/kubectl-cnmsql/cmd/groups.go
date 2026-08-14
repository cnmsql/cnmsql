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

package cmd

import "github.com/spf13/cobra"

// Command group IDs, used to organize subcommands in --help output. Each
// subcommand is assigned one via its GroupID field (see NewRootCommand) and the
// groups are registered on the root command with AddGroup.
const (
	groupCluster         = "cluster"
	groupDatabase        = "db"
	groupTroubleshooting = "troubleshooting"
	groupMisc            = "misc"
)

// commandGroups defines the help groupings and their order.
var commandGroups = []cobra.Group{
	{ID: groupCluster, Title: "Cluster Administration Commands:"},
	{ID: groupDatabase, Title: "Database Administration Commands:"},
	{ID: groupTroubleshooting, Title: "Troubleshooting Commands:"},
	{ID: groupMisc, Title: "Miscellaneous Commands:"},
}
