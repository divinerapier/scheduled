/*
Copyright 2025.

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

package v1alpha1

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CronJobPhase string

const (
	CronJobPhasePending          CronJobPhase = "Pending"
	CronJobPhaseRunning          CronJobPhase = "Running"
	CronJobPhaseInvalidStartTime CronJobPhase = "InvalidStartTime"
	CronJobPhaseInvalidEndTime   CronJobPhase = "InvalidEndTime"
	CronJobPhaseEndBeforeStart   CronJobPhase = "EndBeforeStart"
	CronJobPhaseFailed           CronJobPhase = "Failed"
	CronJobPhaseCompleted        CronJobPhase = "Completed"
	CronJobPhaseUnknown          CronJobPhase = "Unknown"
)

type Interval struct {
	Seconds int32 `json:"seconds,omitempty"`
}

type TimePoint struct {
	Hour   uint8 `json:"hour,omitempty"`
	Minute uint8 `json:"minute,omitempty"`
}

type DailySchedule struct {
	TimePoints []TimePoint `json:"timePoints,omitempty"`
}

type DayOfWeek string

const (
	DayOfWeekMonday    DayOfWeek = "Monday"
	DayOfWeekTuesday   DayOfWeek = "Tuesday"
	DayOfWeekWednesday DayOfWeek = "Wednesday"
	DayOfWeekThursday  DayOfWeek = "Thursday"
	DayOfWeekFriday    DayOfWeek = "Friday"
	DayOfWeekSaturday  DayOfWeek = "Saturday"
	DayOfWeekSunday    DayOfWeek = "Sunday"
)

type WeeklySchedule struct {
	Day       DayOfWeek   `json:"day,omitempty"`
	TimePoint []TimePoint `json:"timePoint,omitempty"`
}

type MonthlySchedule struct {
	Day       uint32      `json:"day,omitempty"`
	TimePoint []TimePoint `json:"timePoint,omitempty"`
}

type ScheduleType struct {
	// +kubebuilder:validation:OneOf
	Cron *string `json:"cron,omitempty"`
	// +kubebuilder:validation:OneOf
	Interval *Interval `json:"interval,omitempty"`
	// +kubebuilder:validation:OneOf
	Daily *DailySchedule `json:"daily,omitempty"`
	// +kubebuilder:validation:OneOf
	Weekly *WeeklySchedule `json:"weekly,omitempty"`
	// +kubebuilder:validation:OneOf
	Monthly *MonthlySchedule `json:"monthly,omitempty"`
}

// ConcurrencyPolicy describes how the job will be handled.
// Only one of the following concurrent policies may be specified.
// If none of the following policies is specified, the default one
// is ForbidConcurrent.
// +kubebuilder:validation:Enum=Allow;Forbid;Replace
type ConcurrencyPolicy string

const (
	// AllowConcurrent allows CronJobs to run concurrently.
	AllowConcurrent ConcurrencyPolicy = "Allow"

	// ForbidConcurrent forbids concurrent runs, skipping next run if previous
	// hasn't finished yet.
	ForbidConcurrent ConcurrencyPolicy = "Forbid"

	// ReplaceConcurrent cancels currently running job and replaces it with a new one.
	ReplaceConcurrent ConcurrencyPolicy = "Replace"
)

type ScheduleRule struct {
	// +kubebuilder:default=Forbid
	ConcurrencyPolicy       ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`
	EndTime                 *metav1.Time      `json:"endTime,omitempty"`
	MaxExecutions           *int32            `json:"maxExecutions,omitempty"`
	MaxRetries              *int32            `json:"maxRetries,omitempty"`
	Schedule                []ScheduleType    `json:"schedule,omitempty"`
	StartTime               *metav1.Time      `json:"startTime,omitempty"`
	StartingDeadlineSeconds *int64            `json:"startingDeadlineSeconds,omitempty"`
}

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// CronJobSpec defines the desired state of CronJob.
type CronJobSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Schedule *ScheduleRule   `json:"schedule,omitempty"`
	Suspend  *bool           `json:"suspend,omitempty"`
	Spec     batchv1.JobSpec `json:"spec,omitempty"`

	FailedJobsHistoryLimit     *int32 `json:"failedJobsHistoryLimit,omitempty"`
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`
}

// CronJobStatus defines the observed state of CronJob.
type CronJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	ActiveJobs         []corev1.ObjectReference `json:"activeJobs,omitempty"`
	ExecutionCount     int32                    `json:"executionCount,omitempty"`
	Phase              CronJobPhase             `json:"phase,omitempty"`
	Message            string                   `json:"message,omitempty"`
	LastSuccessfulTime *metav1.Time             `json:"lastSuccessfulTime,omitempty"`
	LastScheduleTime   *metav1.Time             `json:"lastScheduleTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// CronJob is the Schema for the cronjobs API.
type CronJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CronJobSpec   `json:"spec,omitempty"`
	Status CronJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CronJobList contains a list of CronJob.
type CronJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CronJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CronJob{}, &CronJobList{})
}
