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

package plugin

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// ReportFormat is the manifest format used inside a report ZIP (yaml or json).
type ReportFormat string

const (
	// ReportFormatYAML writes manifests as YAML inside the ZIP.
	ReportFormatYAML ReportFormat = "yaml"
	// ReportFormatJSON writes manifests as JSON inside the ZIP.
	ReportFormatJSON ReportFormat = "json"
)

// sortableTimestampFormat renders a sortable timestamp YYYYMMDD_hhmmss.
const sortableTimestampFormat = "20060102_150405"

// ReportName returns a filesystem-safe, timestamped report name:
// report_<kind>[_<object>]_YYYYMMDD_hhmmss.
func ReportName(kind string, timestamp time.Time, objName ...string) string {
	var b strings.Builder
	b.WriteString("report_")
	b.WriteString(kind)
	if len(objName) > 0 && objName[0] != "" {
		b.WriteString("_")
		b.WriteString(objName[0])
	}
	b.WriteString("_")
	b.WriteString(timestamp.Format(sortableTimestampFormat))
	return b.String()
}

// ZipFileWriter abstracts a function that writes a section into a ZIP under
// the given folder.
type ZipFileWriter func(zipper *zip.Writer, folder string) error

// WriteZippedReport writes the report sections into a new ZIP file. Each
// section writes under the top-level folder. It refuses to overwrite an
// existing file.
func WriteZippedReport(sections []ZipFileWriter, file, folder string) (err error) {
	if _, err := os.Stat(file); err == nil {
		return fmt.Errorf("file %q already exists, will not overwrite", file)
	}
	f, err := os.Create(filepath.Clean(file))
	if err != nil {
		return fmt.Errorf("could not create zip file: %w", err)
	}
	defer func() {
		if errF := f.Sync(); errF != nil && err == nil {
			err = fmt.Errorf("could not flush the zip file: %w", errF)
		}
		if errF := f.Close(); errF != nil && err == nil {
			err = fmt.Errorf("could not close the zip file: %w", errF)
		}
	}()
	zipper := zip.NewWriter(f)
	defer func() {
		if errZ := zipper.Close(); errZ != nil && err == nil {
			err = fmt.Errorf("could not close the zip: %w", errZ)
		}
	}()
	if _, err := zipper.Create(folder + "/"); err != nil {
		return err
	}
	for _, section := range sections {
		if err = section(zipper, folder); err != nil {
			return err
		}
	}
	return err
}

// AddContentToZip marshals content into a named entry inside folder using the
// given format.
func AddContentToZip(content any, name, folder string, format ReportFormat, zipper *zip.Writer) error {
	fileName := filepath.Join(folder, name) + "." + string(format)
	writer, err := zipper.Create(fileName)
	if err != nil {
		return fmt.Errorf("could not add %q to zip: %w", fileName, err)
	}
	return printToWriter(content, format, writer)
}

// NamedObject pairs a display name with the object written into the ZIP.
type NamedObject struct {
	Name   string
	Object any
}

// AddObjectsToZip writes each named object as its own ZIP entry.
func AddObjectsToZip(objects []NamedObject, folder string, format ReportFormat, zipper *zip.Writer) error {
	for _, obj := range objects {
		fileName := filepath.Join(folder, obj.Name) + "." + string(format)
		writer, err := zipper.Create(fileName)
		if err != nil {
			return fmt.Errorf("could not add object %q to zip: %w", obj.Name, err)
		}
		if err := printToWriter(obj.Object, format, writer); err != nil {
			return fmt.Errorf("could not print %q: %w", fileName, err)
		}
	}
	return nil
}

func printToWriter(o any, format ReportFormat, writer io.Writer) error {
	switch format {
	case ReportFormatJSON:
		data, err := yaml.YAMLToJSON(mustYAML(o))
		if err != nil {
			return err
		}
		_, err = writer.Write(data)
		if err != nil {
			return err
		}
		_, err = io.WriteString(writer, "\n")
		return err
	case ReportFormatYAML:
		data, err := yaml.Marshal(o)
		if err != nil {
			return err
		}
		_, err = writer.Write(data)
		return err
	default:
		return fmt.Errorf("unsupported report format %q", format)
	}
}

// RedactSecret returns a copy of secret with the Data keys preserved but the
// values blanked, so the structure is visible without leaking material.
func RedactSecret(secret corev1.Secret) corev1.Secret {
	redacted := secret
	redacted.Data = make(map[string][]byte, len(secret.Data))
	for k := range secret.Data {
		redacted.Data[k] = []byte("")
	}
	redacted.StringData = make(map[string]string, len(secret.StringData))
	for k := range secret.StringData {
		redacted.StringData[k] = ""
	}
	return redacted
}

// PassSecret returns secret unchanged (used when redaction is stopped).
func PassSecret(secret corev1.Secret) corev1.Secret { return secret }

// RedactConfigMap returns a copy of configMap with the Data keys preserved but
// the values blanked.
func RedactConfigMap(configMap corev1.ConfigMap) corev1.ConfigMap {
	redacted := configMap
	redacted.Data = make(map[string]string, len(configMap.Data))
	for k := range configMap.Data {
		redacted.Data[k] = ""
	}
	return redacted
}

// PassConfigMap returns configMap unchanged (used when redaction is stopped).
func PassConfigMap(configMap corev1.ConfigMap) corev1.ConfigMap { return configMap }

// RedactWebhookClientConfig blanks the CA bundle of a webhook client config.
// If a CA bundle is present it is replaced with "-" (base64 "LQ=="); a missing
// bundle is left untouched.
func RedactWebhookClientConfig(
	config admissionregistrationv1.WebhookClientConfig,
) admissionregistrationv1.WebhookClientConfig {
	if len(config.CABundle) != 0 {
		config.CABundle = []byte("-")
	}
	return config
}
