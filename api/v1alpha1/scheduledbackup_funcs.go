/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package v1alpha1

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// scheduleParser parses the 6-field cron expression used by ScheduledBackup.
// Unlike the 5-field UNIX default, the leading field is seconds, matching the
// documented spec.schedule format.
var scheduleParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
)

// ParseSchedule parses a ScheduledBackup cron expression (6 fields, including
// seconds). It is shared by the controller and the validation helper so both
// agree on the accepted syntax.
func ParseSchedule(schedule string) (cron.Schedule, error) {
	return scheduleParser.Parse(schedule)
}

// SetDefaults fills in the unset fields of the ScheduledBackup spec with their
// default values, mirroring the kubebuilder markers so the in-memory object is
// consistent before admission. It is idempotent.
func (s *ScheduledBackup) SetDefaults() {
	spec := &s.Spec
	if spec.Suspend == nil {
		spec.Suspend = ptr.To(false)
	}
	if spec.Immediate == nil {
		spec.Immediate = ptr.To(false)
	}
	if spec.BackupOwnerReference == "" {
		spec.BackupOwnerReference = "self"
	}
	if spec.Method == "" {
		spec.Method = BackupMethodXtrabackup
	}
	if spec.Target == "" {
		spec.Target = BackupTargetPreferStandby
	}
	if spec.Online == nil {
		spec.Online = ptr.To(true)
	}
}

// IsSuspended returns whether the schedule is paused.
func (s *ScheduledBackup) IsSuspended() bool {
	return s.Spec.Suspend != nil && *s.Spec.Suspend
}

// IsImmediate returns whether a backup should be taken as soon as the
// ScheduledBackup is created, in addition to the schedule.
func (s *ScheduledBackup) IsImmediate() bool {
	return s.Spec.Immediate != nil && *s.Spec.Immediate
}

// GetSchedule returns the cron expression driving the schedule.
func (s *ScheduledBackup) GetSchedule() string {
	return s.Spec.Schedule
}

// BackupName returns the deterministic name of the Backup for a scheduled time.
// Using a stable, time-derived name lets reconcile retries observe an already
// created Backup instead of producing duplicates. The suffix is a compact UTC
// timestamp so the name stays a valid DNS-1123 label.
func (s *ScheduledBackup) BackupName(t time.Time) string {
	return fmt.Sprintf("%s-%s", s.Name, t.UTC().Format("20060102150405"))
}

// CreateBackup builds a Backup for this ScheduledBackup with the given name.
// The backup inherits the cluster reference, method, target and online setting;
// the object store is resolved from the Cluster by the BackupReconciler, as for
// one-shot backups.
func (s *ScheduledBackup) CreateBackup(name string) *Backup {
	return &Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.Namespace,
		},
		Spec: BackupSpec{
			Cluster: s.Spec.Cluster,
			Method:  s.Spec.Method,
			Target:  s.Spec.Target,
			Online:  s.Spec.Online,
		},
	}
}

// Validate returns the list of validation errors for the ScheduledBackup spec.
// An empty list means the spec is valid. This is used by unit tests and (later)
// by a validating webhook.
func (s *ScheduledBackup) Validate() field.ErrorList {
	var allErrs field.ErrorList
	schedulePath := field.NewPath("spec").Child("schedule")

	if s.Spec.Schedule == "" {
		allErrs = append(allErrs, field.Required(schedulePath, "schedule is required"))
		return allErrs
	}
	if _, err := ParseSchedule(s.Spec.Schedule); err != nil {
		allErrs = append(allErrs, field.Invalid(
			schedulePath, s.Spec.Schedule,
			fmt.Sprintf("must be a valid 6-field cron expression (seconds included): %v", err)))
	}
	return allErrs
}
