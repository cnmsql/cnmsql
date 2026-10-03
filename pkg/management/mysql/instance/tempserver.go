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

// temporaryServerArgs are the arguments every short-lived bootstrap server
// (initdb, join, import, restore, PITR) starts with. --defaults-file must come
// first. The slow log is off: these servers read the cluster's my.cnf, and a
// dump import with long_query_time=0 would otherwise fill the Job container
// with a log nothing reads (design 037).
func temporaryServerArgs(configFile, dataDir, socket string) []string {
	var args []string
	if configFile != "" {
		args = append(args, "--defaults-file="+configFile)
	}
	return append(args,
		"--datadir="+dataDir,
		"--socket="+socket,
		"--skip-networking",
		"--slow-query-log=OFF",
	)
}
