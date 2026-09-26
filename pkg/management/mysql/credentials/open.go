package credentials

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Mode selects where a manager command reads its passwords.
type Mode string

const (
	ModeSecrets Mode = "secrets"
	ModeEnv     Mode = "env"
)

// Options configures Open.
type Options struct {
	Mode        Mode
	Namespace   string
	ClusterName string
	RestConfig  *rest.Config
	Cluster     client.Reader
	Secrets     kubernetes.Interface
}

// AddFlags registers --credentials-source.
func AddFlags(fs *pflag.FlagSet, o *Options) {
	fs.StringVar((*string)(&o.Mode), "credentials-source", string(ModeSecrets),
		"Where to read MySQL passwords: secrets (the Cluster's credential Secrets through the Kubernetes API) "+
			"or env (MYSQL_{ROOT,APP,CONTROL,BACKUP}_PASSWORD, for tests and standalone runs)")
}

// SecretNames maps each account to its Secret on cluster.
func SecretNames(c *mysqlv1alpha1.Cluster) map[Account]string {
	names := map[Account]string{
		Root:    c.RootSecretName(),
		Control: c.ControlSecretName(),
		Backup:  c.BackupSecretName(),
		Dump:    c.DumpSecretName(),
	}
	if app := c.AppSecretName(); app != "" {
		names[App] = app
	}
	return names
}

// Open returns the command's credential source with the required accounts
// already read. In secrets mode it is a *Provider; long-running callers start
// its Run.
func Open(ctx context.Context, o Options, required ...Account) (Source, error) {
	switch o.Mode {
	case ModeEnv:
		s := FromEnv()
		for _, a := range required {
			if _, ok := s[a]; !ok {
				return nil, fmt.Errorf("credentials: %s must be set", envNames[a])
			}
		}
		return s, nil
	case ModeSecrets, "":
	default:
		return nil, fmt.Errorf("credentials: unknown --credentials-source %q", o.Mode)
	}
	if o.ClusterName == "" || o.Namespace == "" {
		return nil, fmt.Errorf("credentials: --cluster-name and POD_NAMESPACE are required to read credential Secrets")
	}
	if err := o.complete(); err != nil {
		return nil, err
	}
	cluster := &mysqlv1alpha1.Cluster{}
	// The API server may still be starting; retry like Load does.
	if err := retry(ctx, defaultBackoff, func() error {
		return o.Cluster.Get(ctx, types.NamespacedName{Namespace: o.Namespace, Name: o.ClusterName}, cluster)
	}); err != nil {
		return nil, fmt.Errorf("credentials: reading cluster %s: %w", o.ClusterName, err)
	}
	p := NewProvider(o.Secrets, o.Namespace, SecretNames(cluster))
	if err := p.Load(ctx, required...); err != nil {
		return nil, err
	}
	return p, nil
}

func (o *Options) complete() error {
	if o.Cluster != nil && o.Secrets != nil {
		return nil
	}
	cfg := o.RestConfig
	if cfg == nil {
		var err error
		if cfg, err = rest.InClusterConfig(); err != nil {
			return fmt.Errorf("credentials: loading in-cluster config: %w", err)
		}
	}
	if o.Secrets == nil {
		cs, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return err
		}
		o.Secrets = cs
	}
	if o.Cluster == nil {
		scheme := runtime.NewScheme()
		if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
			return err
		}
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			return err
		}
		o.Cluster = c
	}
	return nil
}
