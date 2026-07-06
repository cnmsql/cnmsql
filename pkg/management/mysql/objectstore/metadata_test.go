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

package objectstore

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBackupMetadataAnchorGTIDRoundTrip(t *testing.T) {
	in := BackupMetadata{
		BackupID:    "b1",
		ClusterName: "c1",
		AnchorGTID:  "0-1-42",
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"anchorGTID":"0-1-42"`) {
		t.Fatalf("anchorGTID not serialized: %s", raw)
	}

	var out BackupMetadata
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.AnchorGTID != "0-1-42" {
		t.Fatalf("AnchorGTID round-trip = %q, want %q", out.AnchorGTID, "0-1-42")
	}

	// omitempty: a legacy backup without the field deserializes to empty and a
	// zero-value anchor is not emitted (back-compatible with old readers).
	raw2, _ := json.Marshal(BackupMetadata{BackupID: "b2"})
	if strings.Contains(string(raw2), "anchorGTID") {
		t.Fatalf("empty anchorGTID should be omitted: %s", raw2)
	}
	var legacy BackupMetadata
	if err := json.Unmarshal([]byte(`{"backupID":"old"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.AnchorGTID != "" {
		t.Fatalf("legacy AnchorGTID = %q, want empty", legacy.AnchorGTID)
	}
}
