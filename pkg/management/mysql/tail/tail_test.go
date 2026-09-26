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

package tail

import "testing"

func TestWriterKeepsTheLastBytes(t *testing.T) {
	w := NewWriter(10)
	// More than the cap across several writes: only the last 10 bytes stay.
	for _, chunk := range []string{"aaaa", "bbbb", "cccc", "dddd"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := w.String(), "bbccccdddd"; got != want {
		t.Fatalf("tail = %q, want %q", got, want)
	}
	if _, _ = w.Write([]byte("0123456789ABC")); string(w.Bytes()) != "3456789ABC" {
		t.Fatalf("tail = %q after a write longer than the cap", w.Bytes())
	}
}
