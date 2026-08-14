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

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/spf13/cobra"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

// operatorNameLabel/value identify the operator's own resources (stamped on the
// manager Deployment and its Pods).
const (
	operatorNameLabel = "app.kubernetes.io/name"
	operatorNameValue = "cnmsql"
)

func newReportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Report on the operator or a cluster for troubleshooting",
		Long: `Collects diagnostic information into a Zip file for support.

  # Report on a cluster
  kubectl cnmsql report cluster <cluster-name>

  # Report on the operator
  kubectl cnmsql report operator`,
	}
	cmd.AddCommand(newReportClusterCommand(), newReportOperatorCommand())
	return cmd
}

func newReportClusterCommand() *cobra.Command {
	const filePlaceholder = "report_cluster_<name>_<timestamp>.zip"
	var (
		file, output string
		includeLogs  bool
		logTimeStamp bool
	)
	cmd := &cobra.Command{
		Use:   "cluster CLUSTER",
		Short: "Report cluster resources, pods, events, logs (opt-in)",
		Long:  "Collects combined information on the cluster into a Zip file",
		Args:  cobra.ExactArgs(1),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			if len(args) != 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			return completeCluster(cmd.Context(), toComplete)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			clusterName := args[0]
			now := time.Now().UTC()
			if file == filePlaceholder {
				file = plugin.ReportName("cluster", now, clusterName) + ".zip"
			}
			return runReportCluster(cmd.Context(), clusterName,
				plugin.ReportFormat(output), file, includeLogs, logTimeStamp, now)
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", filePlaceholder, "Output file")
	cmd.Flags().StringVarP(&output, "output", "o", string(plugin.ReportFormatYAML),
		"Output format for manifests (yaml or json)")
	cmd.Flags().BoolVarP(&includeLogs, "logs", "l", false, "include pod logs")
	cmd.Flags().BoolVarP(&logTimeStamp, "timestamps", "t", false,
		"Prepend human-readable timestamp to each log line")
	return cmd
}

func newReportOperatorCommand() *cobra.Command {
	const filePlaceholder = "report_operator_<timestamp>.zip"
	var (
		file          string
		output        string
		stopRedaction bool
		includeLogs   bool
		logTimeStamp  bool
	)
	cmd := &cobra.Command{
		Use:   "operator",
		Short: "Report operator deployment, pod, events, logs (opt-in)",
		Long:  "Collects combined information on the operator into a Zip file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			now := time.Now().UTC()
			if file == filePlaceholder {
				file = plugin.ReportName("operator", now) + ".zip"
			}
			return runReportOperator(cmd.Context(), plugin.ReportFormat(output),
				file, stopRedaction, includeLogs, logTimeStamp, now)
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", filePlaceholder, "Output file")
	cmd.Flags().StringVarP(&output, "output", "o", string(plugin.ReportFormatYAML),
		"Output format for manifests (yaml or json)")
	cmd.Flags().BoolVarP(&stopRedaction, "stop-redaction", "S", false,
		"Do not redact secrets")
	cmd.Flags().BoolVarP(&includeLogs, "logs", "l", false, "include pod logs")
	cmd.Flags().BoolVarP(&logTimeStamp, "timestamps", "t", false,
		"Prepend human-readable timestamp to each log line")
	return cmd
}

// clusterReport holds the resources collected for `report cluster`.
type clusterReport struct {
	cluster   mysqlv1alpha1.Cluster
	pods      corev1.PodList
	jobs      batchv1.JobList
	pvcs      corev1.PersistentVolumeClaimList
	services  corev1.ServiceList
	pdbs      policyv1.PodDisruptionBudgetList
	events    corev1.EventList
	backups   mysqlv1alpha1.BackupList
	scheduled mysqlv1alpha1.ScheduledBackupList
}

func (cr clusterReport) writeToZip(zipper *zip.Writer, format plugin.ReportFormat, folder string) error {
	manifests := path.Join(folder, "manifests")
	if _, err := zipper.Create(manifests + "/"); err != nil {
		return err
	}
	objects := []struct {
		content any
		name    string
	}{
		{cr.cluster, "cluster"},
		{cr.pods, "cluster-pods"},
		{cr.jobs, "cluster-jobs"},
		{cr.pvcs, "cluster-pvcs"},
		{cr.services, "cluster-services"},
		{cr.pdbs, "cluster-pdbs"},
		{cr.events, "events"},
		{cr.backups, "backups"},
		{cr.scheduled, "scheduledbackups"},
	}
	for _, o := range objects {
		if err := plugin.AddContentToZip(o.content, o.name, manifests, format, zipper); err != nil {
			return err
		}
	}
	return nil
}

func runReportCluster(
	ctx context.Context, clusterName string, format plugin.ReportFormat,
	file string, includeLogs, logTimeStamp bool, now time.Time,
) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	matchCluster := client.MatchingLabels{mysqlv1alpha1.ClusterLabelName: clusterName}
	ns := client.InNamespace(cluster.Namespace)

	var pods corev1.PodList
	if err := env.Client.List(ctx, &pods, matchCluster, ns); err != nil {
		return fmt.Errorf("could not get cluster pods: %w", err)
	}
	var jobs batchv1.JobList
	if err := env.Client.List(ctx, &jobs, matchCluster, ns); err != nil {
		return fmt.Errorf("could not get cluster jobs: %w", err)
	}
	var pvcs corev1.PersistentVolumeClaimList
	if err := env.Client.List(ctx, &pvcs, matchCluster, ns); err != nil {
		return fmt.Errorf("could not get cluster pvcs: %w", err)
	}
	var services corev1.ServiceList
	if err := env.Client.List(ctx, &services, matchCluster, ns); err != nil {
		return fmt.Errorf("could not get cluster services: %w", err)
	}
	var pdbs policyv1.PodDisruptionBudgetList
	if err := env.Client.List(ctx, &pdbs, matchCluster, ns); err != nil {
		return fmt.Errorf("could not get cluster PDBs: %w", err)
	}
	var events corev1.EventList
	if err := env.Client.List(ctx, &events, ns); err != nil {
		return fmt.Errorf("could not get events: %w", err)
	}
	var backups mysqlv1alpha1.BackupList
	_ = env.Client.List(ctx, &backups, ns)
	filterBackupsByCluster(&backups, clusterName)
	var scheduled mysqlv1alpha1.ScheduledBackupList
	_ = env.Client.List(ctx, &scheduled, ns)
	filterScheduledByCluster(&scheduled, clusterName)

	rep := clusterReport{
		cluster: *cluster, pods: pods, jobs: jobs, pvcs: pvcs,
		services: services, pdbs: pdbs, events: events, backups: backups, scheduled: scheduled,
	}
	sections := []plugin.ZipFileWriter{
		func(z *zip.Writer, folder string) error { return rep.writeToZip(z, format, folder) },
	}
	if includeLogs {
		sections = append(sections, func(z *zip.Writer, folder string) error {
			return streamClusterLogsToZip(ctx, env, clusterName, cluster.Namespace, folder, logTimeStamp, z)
		})
	}
	if err := plugin.WriteZippedReport(sections, file, plugin.ReportName("cluster", now, clusterName)); err != nil {
		return fmt.Errorf("could not write report: %w", err)
	}
	_, _ = fmt.Fprintf(plugin.Out, "Successfully written report to %q (format: %q)\n", file, format)
	return nil
}

func filterBackupsByCluster(list *mysqlv1alpha1.BackupList, clusterName string) {
	kept := list.Items[:0]
	for i := range list.Items {
		if list.Items[i].Spec.Cluster.Name == clusterName {
			kept = append(kept, list.Items[i])
		}
	}
	list.Items = kept
}

func filterScheduledByCluster(list *mysqlv1alpha1.ScheduledBackupList, clusterName string) {
	kept := list.Items[:0]
	for i := range list.Items {
		if list.Items[i].Spec.Cluster.Name == clusterName {
			kept = append(kept, list.Items[i])
		}
	}
	list.Items = kept
}

func streamClusterLogsToZip(
	ctx context.Context, env *plugin.Env, clusterName, namespace, folder string,
	logTimeStamp bool, zipper *zip.Writer,
) error {
	logsFolder := path.Join(folder, "logs")
	if _, err := zipper.Create(logsFolder + "/"); err != nil {
		return err
	}
	pods, err := env.ListPods(ctx, &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	})
	if err != nil {
		return err
	}
	for i := range pods {
		name := pods[i].Name
		writer, err := zipper.Create(path.Join(logsFolder, name) + ".log")
		if err != nil {
			return err
		}
		opts := &corev1.PodLogOptions{}
		if logTimeStamp {
			opts.Timestamps = true
		}
		if err := copyPodLogs(ctx, env, namespace, name, opts, writer); err != nil {
			_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get logs for %q: %v\n", name, err)
		}
	}
	return nil
}

// operatorReport holds the resources collected for `report operator`.
type operatorReport struct {
	deployment        appsv1.Deployment
	pods              []corev1.Pod
	secrets           []plugin.NamedObject
	configs           []plugin.NamedObject
	events            corev1.EventList
	webhookService    corev1.Service
	validatingWebhook admissionregistrationv1.ValidatingWebhookConfigurationList
	mutatingWebhook   admissionregistrationv1.MutatingWebhookConfigurationList
}

func runReportOperator(
	ctx context.Context, format plugin.ReportFormat,
	file string, stopRedaction, includeLogs, logTimeStamp bool, now time.Time,
) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	secretRedactor, configMapRedactor := configureRedactors(stopRedaction)

	deployment, err := getOperatorDeployment(ctx, env)
	if err != nil {
		return fmt.Errorf("%w; specify the operator namespace with -n", err)
	}
	ns := deployment.Namespace

	podList := tryListOperatorPods(ctx, env, ns)
	secrets, configs := collectOperatorConfigurations(ctx, env, ns, secretRedactor, configMapRedactor)
	events := tryListEvents(ctx, env, ns)
	webhookSvc, validating, mutating := collectWebhooks(ctx, env, ns, stopRedaction)

	rep := operatorReport{
		deployment:        deployment,
		pods:              podList.Items,
		secrets:           secrets,
		configs:           configs,
		events:            events,
		webhookService:    webhookSvc,
		validatingWebhook: validating,
		mutatingWebhook:   mutating,
	}
	sections := []plugin.ZipFileWriter{
		func(z *zip.Writer, folder string) error { return rep.writeToZip(z, format, folder) },
	}
	if includeLogs {
		sections = append(sections, func(z *zip.Writer, folder string) error {
			return streamOperatorLogsToZip(ctx, env, podList.Items, folder, logTimeStamp, z)
		})
	}
	if err := plugin.WriteZippedReport(sections, file, plugin.ReportName("operator", now)); err != nil {
		return fmt.Errorf("could not write report: %w", err)
	}
	_, _ = fmt.Fprintf(plugin.Out, "Successfully written report to %q (format: %q)\n", file, format)
	return nil
}

func (or operatorReport) writeToZip(zipper *zip.Writer, format plugin.ReportFormat, folder string) error {
	manifests := path.Join(folder, "manifests")
	if _, err := zipper.Create(manifests + "/"); err != nil {
		return err
	}
	if err := plugin.AddContentToZip(or.deployment, "deployment", manifests, format, zipper); err != nil {
		return err
	}
	if len(or.pods) > 0 {
		podList := corev1.PodList{Items: or.pods}
		if err := plugin.AddContentToZip(podList, "operator-pods", manifests, format, zipper); err != nil {
			return err
		}
	}
	if len(or.events.Items) > 0 {
		if err := plugin.AddContentToZip(or.events, "events", manifests, format, zipper); err != nil {
			return err
		}
	}
	if len(or.validatingWebhook.Items) > 0 {
		if err := plugin.AddContentToZip(or.validatingWebhook, "validating-webhook-configuration",
			manifests, format, zipper); err != nil {
			return err
		}
	}
	if len(or.mutatingWebhook.Items) > 0 {
		if err := plugin.AddContentToZip(or.mutatingWebhook, "mutating-webhook-configuration",
			manifests, format, zipper); err != nil {
			return err
		}
	}
	if or.webhookService.Name != "" {
		if err := plugin.AddContentToZip(or.webhookService, "webhook-service", manifests, format, zipper); err != nil {
			return err
		}
	}
	if len(or.configs) > 0 {
		if err := plugin.AddObjectsToZip(or.configs, manifests, format, zipper); err != nil {
			return err
		}
	}
	if len(or.secrets) > 0 {
		if err := plugin.AddObjectsToZip(or.secrets, manifests, format, zipper); err != nil {
			return err
		}
	}
	return nil
}

func configureRedactors(
	stopRedaction bool,
) (func(corev1.Secret) corev1.Secret, func(corev1.ConfigMap) corev1.ConfigMap) {
	if stopRedaction {
		_, _ = fmt.Fprintln(plugin.Out, "WARNING: secret redaction is OFF. Use with caution")
		return plugin.PassSecret, plugin.PassConfigMap
	}
	return plugin.RedactSecret, plugin.RedactConfigMap
}

// errNoOperatorDeployment is returned when no Deployment labeled as the
// operator is found in the searched namespace(s).
var errNoOperatorDeployment = fmt.Errorf("could not find the operator deployment")

// getOperatorDeployment finds the operator Deployment. When a namespace is set
// (the persistent -n flag) it looks there; otherwise it searches all
// namespaces for a Deployment labeled app.kubernetes.io/name=cnmsql.
func getOperatorDeployment(ctx context.Context, env *plugin.Env) (appsv1.Deployment, error) {
	selector := operatorLabelSelector()
	if env.Namespace != "" {
		d, err := env.Clientset.AppsV1().Deployments(env.Namespace).List(ctx, selector)
		if err != nil {
			return appsv1.Deployment{}, err
		}
		if len(d.Items) > 0 {
			return d.Items[0], nil
		}
		return appsv1.Deployment{}, errNoOperatorDeployment
	}
	d, err := env.Clientset.AppsV1().Deployments("").List(ctx, selector)
	if err != nil {
		return appsv1.Deployment{}, err
	}
	if len(d.Items) > 0 {
		return d.Items[0], nil
	}
	return appsv1.Deployment{}, errNoOperatorDeployment
}

func operatorLabelSelector() metav1.ListOptions {
	return metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{operatorNameLabel: operatorNameValue}).String(),
	}
}

func tryListOperatorPods(ctx context.Context, env *plugin.Env, ns string) corev1.PodList {
	pods, err := env.Clientset.CoreV1().Pods(ns).List(ctx, operatorLabelSelector())
	if err != nil {
		_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get operator pods: %v\n", err)
		return corev1.PodList{}
	}
	return *pods
}

func collectOperatorConfigurations(
	ctx context.Context, env *plugin.Env, ns string,
	secretRedactor func(corev1.Secret) corev1.Secret,
	configMapRedactor func(corev1.ConfigMap) corev1.ConfigMap,
) ([]plugin.NamedObject, []plugin.NamedObject) {
	var secrets []plugin.NamedObject
	if secretList, err := env.Clientset.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range secretList.Items {
			s := &secretList.Items[i]
			secrets = append(secrets, plugin.NamedObject{
				Name:   s.Name + "(secret)",
				Object: secretRedactor(*s),
			})
		}
	} else {
		_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get operator secrets: %v\n", err)
	}
	var configs []plugin.NamedObject
	if cmList, err := env.Clientset.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range cmList.Items {
			cm := &cmList.Items[i]
			configs = append(configs, plugin.NamedObject{
				Name:   cm.Name,
				Object: configMapRedactor(*cm),
			})
		}
	} else {
		_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get operator configmaps: %v\n", err)
	}
	return secrets, configs
}

func tryListEvents(ctx context.Context, env *plugin.Env, ns string) corev1.EventList {
	var events corev1.EventList
	if err := env.Client.List(ctx, &events, client.InNamespace(ns)); err != nil {
		_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get events: %v\n", err)
	}
	return events
}

func collectWebhooks(
	ctx context.Context, env *plugin.Env, ns string, stopRedaction bool,
) (corev1.Service, admissionregistrationv1.ValidatingWebhookConfigurationList,
	admissionregistrationv1.MutatingWebhookConfigurationList) {
	var (
		webhookService corev1.Service
		validating     admissionregistrationv1.ValidatingWebhookConfigurationList
		mutating       admissionregistrationv1.MutatingWebhookConfigurationList
	)
	vList, err := env.Clientset.AdmissionregistrationV1().
		ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get validating webhooks: %v\n", err)
		return webhookService, validating, mutating
	}
	for i := range vList.Items {
		vwc := &vList.Items[i]
		if !validatingReferencesNamespace(vwc.Webhooks, ns) {
			continue
		}
		if !stopRedaction {
			redactValidatingWebhook(vwc)
		}
		validating.Items = append(validating.Items, *vwc)
	}
	mList, err := env.Clientset.AdmissionregistrationV1().
		MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get mutating webhooks: %v\n", err)
		return webhookService, validating, mutating
	}
	for i := range mList.Items {
		mwc := &mList.Items[i]
		if !mutatingReferencesNamespace(mwc.Webhooks, ns) {
			continue
		}
		if !stopRedaction {
			redactMutatingWebhook(mwc)
		}
		mutating.Items = append(mutating.Items, *mwc)
	}
	webhookService = discoverWebhookService(ctx, env, validating.Items)
	return webhookService, validating, mutating
}

func discoverWebhookService(
	ctx context.Context, env *plugin.Env,
	configs []admissionregistrationv1.ValidatingWebhookConfiguration,
) corev1.Service {
	for i := range configs {
		for _, wh := range configs[i].Webhooks {
			if wh.ClientConfig.Service == nil {
				continue
			}
			svc, err := env.Clientset.CoreV1().Services(wh.ClientConfig.Service.Namespace).
				Get(ctx, wh.ClientConfig.Service.Name, metav1.GetOptions{})
			if err == nil {
				return *svc
			}
		}
	}
	return corev1.Service{}
}

func validatingReferencesNamespace(webhooks []admissionregistrationv1.ValidatingWebhook, ns string) bool {
	for i := range webhooks {
		if svc := webhooks[i].ClientConfig.Service; svc != nil && svc.Namespace == ns {
			return true
		}
	}
	return false
}

func mutatingReferencesNamespace(webhooks []admissionregistrationv1.MutatingWebhook, ns string) bool {
	for i := range webhooks {
		if svc := webhooks[i].ClientConfig.Service; svc != nil && svc.Namespace == ns {
			return true
		}
	}
	return false
}

func redactValidatingWebhook(vwc *admissionregistrationv1.ValidatingWebhookConfiguration) {
	for i := range vwc.Webhooks {
		vwc.Webhooks[i].ClientConfig = plugin.RedactWebhookClientConfig(vwc.Webhooks[i].ClientConfig)
	}
}

func redactMutatingWebhook(mwc *admissionregistrationv1.MutatingWebhookConfiguration) {
	for i := range mwc.Webhooks {
		mwc.Webhooks[i].ClientConfig = plugin.RedactWebhookClientConfig(mwc.Webhooks[i].ClientConfig)
	}
}

func streamOperatorLogsToZip(
	ctx context.Context, env *plugin.Env, pods []corev1.Pod, folder string,
	logTimeStamp bool, zipper *zip.Writer,
) error {
	logsFolder := path.Join(folder, "logs")
	if _, err := zipper.Create(logsFolder + "/"); err != nil {
		return err
	}
	for i := range pods {
		name := pods[i].Name
		writer, err := zipper.Create(path.Join(logsFolder, name) + ".log")
		if err != nil {
			return err
		}
		opts := &corev1.PodLogOptions{}
		if logTimeStamp {
			opts.Timestamps = true
		}
		if err := copyPodLogs(ctx, env, pods[i].Namespace, name, opts, writer); err != nil {
			_, _ = fmt.Fprintf(plugin.Out, "WARNING: could not get logs for %q: %v\n", name, err)
		}
	}
	return nil
}

// copyPodLogs streams a pod's logs into w.
func copyPodLogs(
	ctx context.Context, env *plugin.Env, namespace, name string,
	opts *corev1.PodLogOptions, w io.Writer,
) error {
	req := env.Clientset.CoreV1().Pods(namespace).GetLogs(name, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("opening log stream for %q: %w", name, err)
	}
	defer func() { _ = stream.Close() }()
	if _, err := io.Copy(w, stream); err != nil {
		return fmt.Errorf("reading log stream for %q: %w", name, err)
	}
	return nil
}
