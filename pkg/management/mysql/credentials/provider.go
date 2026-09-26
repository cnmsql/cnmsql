package credentials

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	passwordKey    = "password"
	defaultResync  = 5 * time.Minute
	defaultBackoff = time.Second
	maxBackoff     = 30 * time.Second
)

// Provider reads account passwords from named Secrets and keeps them current.
// It never lists Secrets: the instance Role grants only get and watch by name.
type Provider struct {
	client    kubernetes.Interface
	namespace string
	secrets   map[Account]string

	resync      time.Duration
	backoffBase time.Duration

	mu       sync.RWMutex
	values   map[Account]string
	versions map[Account]string
}

// NewProvider reads the given account → Secret name mapping in namespace.
func NewProvider(c kubernetes.Interface, namespace string, secrets map[Account]string) *Provider {
	return &Provider{
		client: c, namespace: namespace, secrets: secrets,
		resync: defaultResync, backoffBase: defaultBackoff,
		values: map[Account]string{}, versions: map[Account]string{},
	}
}

// Password returns the account's last known password.
func (p *Provider) Password(a Account) (string, error) {
	if _, ok := p.secrets[a]; !ok {
		return "", ErrUnknown
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.values[a]
	if !ok {
		return "", ErrNotLoaded
	}
	return v, nil
}

// Load reads each required account's Secret, retrying with backoff until all
// have been read once or ctx ends.
func (p *Provider) Load(ctx context.Context, required ...Account) error {
	log := logf.FromContext(ctx).WithName("credentials")
	for _, a := range required {
		if _, ok := p.secrets[a]; !ok {
			return fmt.Errorf("credentials: %s: %w", a, ErrUnknown)
		}
		if err := retry(ctx, p.backoffBase, func() error {
			err := p.refresh(ctx, a)
			if err != nil {
				log.Info("Could not read credential Secret, retrying", "account", string(a),
					"secret", p.secrets[a], "error", err.Error())
			}
			return err
		}); err != nil {
			return fmt.Errorf("credentials: reading %s secret: %w", a, err)
		}
	}
	return nil
}

// retry calls fn until it succeeds or ctx ends, waiting base, then doubling up
// to maxBackoff between attempts. It returns fn's last error on cancellation.
func retry(ctx context.Context, base time.Duration, fn func() error) error {
	delay := base
	for {
		err := fn()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		delay = min(delay*2, maxBackoff)
	}
}

// Run keeps every account current until ctx ends: one watch per Secret,
// restarted when it closes, and a re-get of all of them every resync period.
// Errors are logged; the last value read stays in place.
func (p *Provider) Run(ctx context.Context) {
	for a := range p.secrets {
		go p.watch(ctx, a)
	}
	ticker := time.NewTicker(p.resync)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for a := range p.secrets {
				if err := p.refresh(ctx, a); err != nil {
					logf.FromContext(ctx).WithName("credentials").V(1).Info("Could not re-read credential Secret",
						"account", string(a), "error", err.Error())
				}
			}
		}
	}
}

func (p *Provider) watch(ctx context.Context, a Account) {
	name := p.secrets[a]
	log := logf.FromContext(ctx).WithName("credentials").WithValues("account", string(a), "secret", name)
	delay := p.backoffBase
	for ctx.Err() == nil {
		p.mu.RLock()
		rv := p.versions[a]
		p.mu.RUnlock()
		w, err := p.client.CoreV1().Secrets(p.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector:   fields.OneTermEqualSelector("metadata.name", name).String(),
			ResourceVersion: rv,
		})
		if err != nil {
			log.V(1).Info("Could not watch credential Secret", "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay = min(delay*2, maxBackoff)
			continue
		}
		failed := p.consume(ctx, a, w)
		w.Stop()
		// The watch closed or expired (410 Gone): re-read to get a fresh
		// resourceVersion before watching again. When that fails too (the
		// Secret is gone), forget the old resourceVersion, which may have
		// expired: watching from it would fail again at once.
		if err := p.refresh(ctx, a); err != nil {
			failed = true
			p.mu.Lock()
			delete(p.versions, a)
			p.mu.Unlock()
		}
		if !failed {
			delay = p.backoffBase
			continue
		}
		// Back off so a watch the server ends at once cannot spin against it.
		log.V(1).Info("Credential Secret watch ended with an error, retrying")
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, maxBackoff)
	}
}

// consume stores the Secret updates of w until it closes or ctx ends. It
// reports whether the watch ended on an error event.
func (p *Provider) consume(ctx context.Context, a Account, w watch.Interface) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-w.ResultChan():
			if !ok {
				return false
			}
			if ev.Type == watch.Error {
				return true
			}
			if s, isSecret := ev.Object.(*corev1.Secret); isSecret && (ev.Type == watch.Added || ev.Type == watch.Modified) {
				p.store(ctx, a, s)
			}
		}
	}
}

func (p *Provider) refresh(ctx context.Context, a Account) error {
	s, err := p.client.CoreV1().Secrets(p.namespace).Get(ctx, p.secrets[a], metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !p.store(ctx, a, s) {
		return fmt.Errorf("secret %s has no %q key", s.Name, passwordKey)
	}
	return nil
}

// store records s's password for a. It ignores Secrets with another name (a
// watch that does not honour its field selector) and empty passwords (keeping
// the last good value), and reports whether it stored anything.
func (p *Provider) store(ctx context.Context, a Account, s *corev1.Secret) bool {
	if s.Name != p.secrets[a] {
		return false
	}
	v := string(s.Data[passwordKey])
	if v == "" {
		logf.FromContext(ctx).WithName("credentials").Info("Ignored credential Secret without a password",
			"account", string(a), "secret", s.Name)
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.values[a]; ok && old != v {
		logf.FromContext(ctx).WithName("credentials").Info("Picked up rotated credential Secret",
			"account", string(a), "secret", s.Name)
	}
	p.values[a] = v
	p.versions[a] = s.ResourceVersion
	return true
}
