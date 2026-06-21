package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/bootstrap"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller"
	webhookv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/internal/webhook/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/executablehash"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(mysqlv1alpha1.AddToScheme(scheme))
	utilruntime.Must(monitoringv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var operatorImage string

	// zap options are bound to the persistent flags below, so verbosity can be
	// raised at runtime (e.g. --zap-log-level=debug for the V(1) reconcile trace).
	zapOpts := zap.Options{}
	zapFlagSet := flag.NewFlagSet("zap", flag.ContinueOnError)
	zapOpts.BindFlags(zapFlagSet)

	root := &cobra.Command{
		Use:           "manager",
		SilenceUsage:  true,
		SilenceErrors: true,
		// Install the logger for every command in the tree (the operator and all
		// `instance` subcommands share this binary); otherwise subcommands run
		// without a logger and controller-runtime silently drops their logs.
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var tlsOpts []func(*tls.Config)

			disableHTTP2 := func(c *tls.Config) {
				setupLog.Info("Disabling HTTP/2")
				c.NextProtos = []string{"http/1.1"}
			}

			if !enableHTTP2 {
				tlsOpts = append(tlsOpts, disableHTTP2)
			}

			webhookTLSOpts := tlsOpts
			webhookServerOptions := webhook.Options{
				TLSOpts: webhookTLSOpts,
			}

			if len(webhookCertPath) > 0 {
				setupLog.Info("Initializing webhook certificate watcher using provided certificates",
					"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

				webhookServerOptions.CertDir = webhookCertPath
				webhookServerOptions.CertName = webhookCertName
				webhookServerOptions.KeyName = webhookCertKey
			}

			webhookServer := webhook.NewServer(webhookServerOptions)

			metricsServerOptions := metricsserver.Options{
				BindAddress:   metricsAddr,
				SecureServing: secureMetrics,
				TLSOpts:       tlsOpts,
			}

			if secureMetrics {
				metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
			}

			if len(metricsCertPath) > 0 {
				setupLog.Info("Initializing metrics certificate watcher using provided certificates",
					"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

				metricsServerOptions.CertDir = metricsCertPath
				metricsServerOptions.CertName = metricsCertName
				metricsServerOptions.KeyName = metricsCertKey
			}

			managerOptions := ctrl.Options{
				Scheme:                 scheme,
				Metrics:                metricsServerOptions,
				WebhookServer:          webhookServer,
				HealthProbeBindAddress: probeAddr,
				LeaderElection:         enableLeaderElection,
				LeaderElectionID:       "e924591d.cloudnative-mysql.io",
			}

			// WATCH_NAMESPACE selects the operator topology: empty means cluster-wide
			// (watch every namespace), set means namespaced (watch only that namespace,
			// so multiple operator instances can cohabit in one cluster). In namespaced
			// packaging the value is injected from the pod's own namespace.
			if watchNamespace := os.Getenv("WATCH_NAMESPACE"); watchNamespace != "" {
				setupLog.Info("Operator running namespaced", "namespace", watchNamespace)
				managerOptions.Cache = cache.Options{
					DefaultNamespaces: map[string]cache.Config{watchNamespace: {}},
				}
				// Keep the leader-election lease in the watched namespace so cohabiting
				// operators do not contend on a single cluster-wide lease.
				managerOptions.LeaderElectionNamespace = watchNamespace
			} else {
				setupLog.Info("Operator running cluster-wide")
			}

			mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), managerOptions)
			if err != nil {
				setupLog.Error(err, "Failed to start manager")
				return err
			}

			operatorHash, err := executablehash.Get()
			if err != nil {
				setupLog.Error(err, "Failed to compute operator executable hash (will not detect stale instance managers)")
				operatorHash = ""
			}

			clusterReconciler := &controller.ClusterReconciler{
				Client:                 mgr.GetClient(),
				Scheme:                 mgr.GetScheme(),
				APIReader:              mgr.GetAPIReader(),
				Recorder:               mgr.GetEventRecorderFor("cluster-controller"), //nolint:staticcheck
				OperatorImageName:      operatorImage,
				OperatorExecutableHash: operatorHash,
			}
			if err := clusterReconciler.SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "Failed to create controller", "controller", "cluster")
				return err
			}
			// Publish per-cluster Group Replication status (quorum, member states)
			// on the operator's existing /metrics endpoint.
			controller.RegisterGRMetrics(mgr.GetClient())
			if err := (&controller.BackupReconciler{
				Client:            mgr.GetClient(),
				Scheme:            mgr.GetScheme(),
				Recorder:          mgr.GetEventRecorderFor("backup-controller"), //nolint:staticcheck
				OperatorImageName: operatorImage,
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "Failed to create controller", "controller", "backup")
				return err
			}
			if err := (&controller.ScheduledBackupReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorderFor("scheduledbackup-controller"), //nolint:staticcheck
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "Failed to create controller", "controller", "scheduledbackup")
				return err
			}
			if err := (&controller.DatabaseReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorderFor("database-controller"), //nolint:staticcheck
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "Failed to create controller", "controller", "database")
				return err
			}
			if os.Getenv("ENABLE_WEBHOOKS") != "false" {
				if err := webhookv1alpha1.SetupClusterWebhookWithManager(mgr); err != nil {
					setupLog.Error(err, "Failed to set up webhook", "webhook", "cluster-status")
					return err
				}
			}
			// +kubebuilder:scaffold:builder

			if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
				setupLog.Error(err, "Failed to set up health check")
				return err
			}
			if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
				setupLog.Error(err, "Failed to set up ready check")
				return err
			}

			setupLog.Info("Starting manager")
			if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
				setupLog.Error(err, "Failed to run manager")
				return err
			}
			return nil
		},
	}

	root.PersistentFlags().StringVar(&metricsAddr, "metrics-bind-address", "0",
		"The address the metrics endpoint binds to. "+
			"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	root.PersistentFlags().StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	root.PersistentFlags().BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	root.PersistentFlags().BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	root.PersistentFlags().StringVar(&webhookCertPath, "webhook-cert-path", "",
		"The directory that contains the webhook certificate.")
	root.PersistentFlags().StringVar(&webhookCertName, "webhook-cert-name", "tls.crt",
		"The name of the webhook certificate file.")
	root.PersistentFlags().StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	root.PersistentFlags().StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	root.PersistentFlags().StringVar(&metricsCertName, "metrics-cert-name", "tls.crt",
		"The name of the metrics server certificate file.")
	root.PersistentFlags().StringVar(&metricsCertKey, "metrics-cert-key", "tls.key",
		"The name of the metrics server key file.")
	root.PersistentFlags().BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	root.PersistentFlags().StringVar(&operatorImage, "operator-image", "",
		"Operator image name (used for bootstrap-controller init container)")
	root.PersistentFlags().AddGoFlagSet(zapFlagSet)

	root.AddCommand(bootstrap.NewCommand())
	root.AddCommand(instance.NewCommand())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "command failed:", err)
		os.Exit(1)
	}
}
