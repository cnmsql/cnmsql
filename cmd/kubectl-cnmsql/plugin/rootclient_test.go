package plugin

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/rootclient"
)

func TestReadyWriterStripsMarkerAcrossWrites(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	w := newReadyWriter(&out)
	// The marker arrives split over several writes, surrounded by output.
	marker := rootclient.PasswordReady
	chunks := []string{"hello ", marker[:4], marker[4:9], marker[9:] + "mysql> "}
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
			prefix := []string{InstanceManager, "instance", "client", "--", tt.wantBinary,
				"--socket=" + SocketPath, "--user=root", flag}
			want := append(slices.Clone(prefix), tt.args...)
			if !slices.Equal(cmd, want) {
				t.Errorf("command = %q, want %q", cmd, want)
			}
		})
	}
}

// TestRootClientHandshake drives the in-pod side of the handshake with the
// same writer/reader pair RootClient uses: the password must only be sent
// once the marker is printed, arrive whole, and leave the caller's input for
// the client, with the marker hidden from the user.
func TestRootClientHandshake(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	stdout := newReadyWriter(&out)
	stdin := newGatedReader(context.Background(), stdout.ready, "p'a ss$word", strings.NewReader("SELECT 1;\n"))
	password, err := rootclient.ReadPassword(stdin, stdout)
	if err != nil {
		t.Fatalf("ReadPassword() error = %v", err)
	}
	if password != "p'a ss$word" {
		t.Errorf("password = %q", password)
	}
	if rest, _ := io.ReadAll(stdin); string(rest) != "SELECT 1;\n" {
		t.Errorf("left for the client = %q, want the caller's input", rest)
	}
	stdout.Flush()
	if out.Len() != 0 {
		t.Errorf("user saw %q, want the marker hidden", out.String())
	}
}
