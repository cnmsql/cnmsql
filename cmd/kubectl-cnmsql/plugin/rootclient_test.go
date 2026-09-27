package plugin

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestReadyWriterStripsMarkerAcrossWrites(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	w := newReadyWriter(&out)
	// The marker arrives split over several writes, surrounded by output.
	chunks := []string{"hello ", passwordReady[:4], passwordReady[4:9], passwordReady[9:] + "mysql> "}
	for _, c := range chunks {
		if _, err := w.Write([]byte(c)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	select {
	case <-w.ready:
	default:
		t.Fatal("ready was not closed after the marker")
	}
	if got := out.String(); got != "hello mysql> " {
		t.Errorf("output = %q, want the marker removed", got)
	}
}

func TestReadyWriterFlushesWithoutMarker(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	w := newReadyWriter(&out)
	_, _ = w.Write([]byte("sh: not found\x1b]77"))
	w.Flush()
	if got := out.String(); got != "sh: not found\x1b]77" {
		t.Errorf("output = %q, want everything flushed", got)
	}
}

func TestGatedReaderWaitsForReady(t *testing.T) {
	t.Parallel()
	ready := make(chan struct{})
	r := newGatedReader(context.Background(), ready, "s3cret", strings.NewReader("SELECT 1;"))

	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	select {
	case got := <-done:
		t.Fatalf("read %q before the remote side was ready", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(ready)
	if got := <-done; got != "s3cret\nSELECT 1;" {
		t.Errorf("stdin = %q, want the password line then the input", got)
	}
}

func TestGatedReaderStopsOnCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := newGatedReader(ctx, make(chan struct{}), "s3cret", nil)
	if _, err := r.Read(make([]byte, 8)); err == nil {
		t.Fatal("Read() succeeded after cancellation")
	}
}

func TestRootClientScriptKeepsSecretsOffArgv(t *testing.T) {
	t.Parallel()
	if !strings.Contains(rootClientScript, `exec "$0" "$@"`) {
		t.Error("client arguments must be passed positionally, not interpolated")
	}
	if !strings.Contains(rootClientScript, "7717;cnmsql-password") {
		t.Error("the script must print the marker the plugin waits for")
	}
}

// TestRootClientCommandDefaultsToUTF8MB4 pins the charset the client is told
// to negotiate: without --default-character-set=utf8mb4 a bare instance image
// negotiates latin1 and UTF-8 SQL is double-encoded on insert.
func TestRootClientCommandDefaultsToUTF8MB4(t *testing.T) {
	t.Parallel()

	const flag = "--default-character-set=utf8mb4"
	cluster := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "test"}}
	tests := []struct {
		name       string
		cluster    *mysqlv1alpha1.Cluster
		args       []string
		wantBinary string
	}{
		{
			name: "mysql", cluster: cluster, wantBinary: "mysql",
			args: []string{"mydb"},
		},
		{
			name: "mariadb", wantBinary: "mariadb",
			cluster: &mysqlv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "test"},
				Spec:       mysqlv1alpha1.ClusterSpec{Flavor: mysqlv1alpha1.FlavorMariaDB},
			},
			args: []string{"mydb"},
		},
		{
			// The client honours the last occurrence of a flag, so the caller
			// must be able to override the default.
			name: "caller can override the default", cluster: cluster, wantBinary: "mysql",
			args: []string{"--default-character-set=latin1", "mydb"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := rootClientCommand(RootClientOptions{Cluster: tt.cluster, Args: tt.args})
			prefix := []string{"sh", "-c", rootClientScript, tt.wantBinary,
				"--socket=" + SocketPath, "--user=root", flag}
			want := append(slices.Clone(prefix), tt.args...)
			if !slices.Equal(cmd, want) {
				t.Errorf("command = %q, want %q", cmd, want)
			}
		})
	}
}

// TestRootClientScriptRunsLocally drives the in-pod wrapper through a local
// shell with the same writer/reader pair RootClient uses, standing in a shell
// for the database client: it must receive the password in MYSQL_PWD, the
// client arguments verbatim, and the caller's input after the password line.
func TestRootClientScriptRunsLocally(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell")
	}
	ctx := context.Background()
	var out bytes.Buffer
	stdout := newReadyWriter(&out)
	client := `printf 'pw=%s args=%s|%s\n' "$MYSQL_PWD" "$1" "$2"; cat`
	cmd := exec.CommandContext(ctx, "sh", "-c", rootClientScript,
		"sh", "-c", client, "fake-client", "it's; $(rm -rf /)")
	cmd.Stdin = newGatedReader(ctx, stdout.ready, "p'a ss$word", strings.NewReader("SELECT 1;\n"))
	cmd.Stdout = stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("running the wrapper: %v", err)
	}
	stdout.Flush()
	want := "pw=p'a ss$word args=it's; $(rm -rf /)|\nSELECT 1;\n"
	if got := out.String(); got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}
