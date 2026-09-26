package credentials

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const ns = "default"

func secret(name, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, ResourceVersion: "1"},
		Data:       map[string][]byte{"password": []byte(password)},
	}
}

// fakeWatches makes every Secret watch return the watcher for its field
// selector's name, created on demand, so tests can push events.
func fakeWatches(cs *fake.Clientset) func(name string) *watch.FakeWatcher {
	var mu sync.Mutex // get runs on both the test goroutine and watch goroutines
	watchers := map[string]*watch.FakeWatcher{}
	get := func(name string) *watch.FakeWatcher {
		mu.Lock()
		defer mu.Unlock()
		if w, ok := watchers[name]; ok {
			return w
		}
		w := watch.NewFakeWithChanSize(10, false)
		watchers[name] = w
		return w
	}
	cs.PrependWatchReactor("secrets", func(action k8stesting.Action) (bool, watch.Interface, error) {
		sel := action.(k8stesting.WatchAction).GetWatchRestrictions().Fields
		name, _ := sel.RequiresExactMatch("metadata.name")
		return true, get(name), nil
	})
	return get
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProviderLoad(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control", Dump: "demo-dump"})
	if err := p.Load(context.Background(), Control); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.Password(Control); got != "c1" {
		t.Fatalf("control = %q", got)
	}
	if _, err := p.Password(Dump); !errors.Is(err, ErrNotLoaded) {
		t.Fatalf("dump err = %v, want ErrNotLoaded", err)
	}
	if _, err := p.Password(Root); !errors.Is(err, ErrUnknown) {
		t.Fatalf("root err = %v, want ErrUnknown", err)
	}
}

func TestProviderLoadRetriesUntilContextEnds(t *testing.T) {
	cs := fake.NewClientset() // secret missing
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.backoffBase = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Load(ctx, Control); err == nil {
		t.Fatal("Load succeeded without the secret")
	}
	if n := len(cs.Actions()); n < 2 {
		t.Fatalf("Load tried %d times, want retries", n)
	}
}

func TestProviderWatchRotates(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	watches := fakeWatches(cs)
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.resync = time.Hour
	ctx := t.Context()
	if err := p.Load(ctx, Control); err != nil {
		t.Fatal(err)
	}
	go p.Run(ctx)
	watches("demo-control").Modify(secret("demo-control", "c2"))
	waitFor(t, func() bool { v, _ := p.Password(Control); return v == "c2" })
}

func TestProviderFiltersForeignSecret(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	watches := fakeWatches(cs)
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.resync = time.Hour
	ctx := t.Context()
	_ = p.Load(ctx, Control)
	go p.Run(ctx)
	w := watches("demo-control")
	w.Modify(secret("someone-else", "evil"))
	w.Modify(secret("demo-control", "c2"))
	waitFor(t, func() bool { v, _ := p.Password(Control); return v == "c2" })
}

func TestProviderIgnoresEmptyPassword(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	ctx := context.Background()
	if err := p.Load(ctx, Control); err != nil {
		t.Fatal(err)
	}
	if p.store(ctx, Control, secret("demo-control", "")) {
		t.Fatal("store accepted an empty password")
	}
	if v, _ := p.Password(Control); v != "c1" {
		t.Fatalf("control = %q after an empty update, want c1 kept", v)
	}
}

func TestProviderResyncCatchesMissedUpdate(t *testing.T) {
	cs := fake.NewClientset(secret("demo-control", "c1"))
	fakeWatches(cs) // watches never deliver
	p := NewProvider(cs, ns, map[Account]string{Control: "demo-control"})
	p.resync = 10 * time.Millisecond
	ctx := t.Context()
	_ = p.Load(ctx, Control)
	go p.Run(ctx)
	_, err := cs.CoreV1().Secrets(ns).Update(ctx, secret("demo-control", "c2"), metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { v, _ := p.Password(Control); return v == "c2" })
}

func TestProviderRunPicksUpLateSecret(t *testing.T) {
	cs := fake.NewClientset()
	watches := fakeWatches(cs)
	p := NewProvider(cs, ns, map[Account]string{Dump: "demo-dump"})
	p.resync = time.Hour
	ctx := t.Context()
	go p.Run(ctx)
	watches("demo-dump").Add(secret("demo-dump", "d1"))
	waitFor(t, func() bool { v, _ := p.Password(Dump); return v == "d1" })
}

// A watch the server ends at once with an error (410 Gone on an expired
// resourceVersion) while the Secret cannot be re-read (it was deleted) must
// back off and drop the stale resourceVersion, not spin against the API server.
func TestProviderWatchBacksOffWhenSecretIsGone(t *testing.T) {
	cs := fake.NewClientset() // secret deleted
	var mu sync.Mutex
	var versions []string
	cs.PrependWatchReactor("secrets", func(action k8stesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		versions = append(versions, action.(k8stesting.WatchActionImpl).WatchRestrictions.ResourceVersion)
		mu.Unlock()
		w := watch.NewFakeWithChanSize(1, false)
		w.Error(&metav1.Status{Code: 410, Reason: metav1.StatusReasonExpired})
		return true, w, nil
	})
	p := NewProvider(cs, ns, map[Account]string{Root: "demo-root"})
	p.backoffBase = 20 * time.Millisecond
	p.versions[Root] = "1"
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p.watch(ctx, Root)

	mu.Lock()
	defer mu.Unlock()
	if n := len(versions); n > 5 {
		t.Fatalf("watched %d times in 100ms, want a backoff between attempts", n)
	}
	if len(versions) < 2 || versions[0] != "1" || versions[1] != "" {
		t.Fatalf("watch resourceVersions = %q, want \"1\" then \"\" once the secret cannot be re-read", versions)
	}
}
