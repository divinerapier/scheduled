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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DelayedJobPhase string

const (
	DelayedJobPhasePending          DelayedJobPhase = "Pending"
	DelayedJobPhaseRunning          DelayedJobPhase = "Running"
	DelayedJobPhaseInvalidStartTime DelayedJobPhase = "InvalidStartTime"
	DelayedJobPhaseFailed           DelayedJobPhase = "Failed"
	DelayedJobPhaseCompleted        DelayedJobPhase = "Completed"
	DelayedJobPhaseUnknown          DelayedJobPhase = "Unknown"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DelayedJobSpec defines the desired state of DelayedJob.
type DelayedJobSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	StartTime *metav1.Time `json:"startTime,omitempty"`

	Spec batchv1.JobSpec `json:"spec,omitempty"`
}

// DelayedJobStatus defines the observed state of DelayedJob.
type DelayedJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	Phase          DelayedJobPhase `json:"phase,omitempty"`
	Message        string          `json:"message,omitempty"`
	LastUpdateTime *metav1.Time    `json:"lastUpdateTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DelayedJob is the Schema for the delayedjobs API.
type DelayedJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DelayedJobSpec   `json:"spec,omitempty"`
	Status DelayedJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DelayedJobList contains a list of DelayedJob.
type DelayedJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DelayedJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DelayedJob{}, &DelayedJobList{})
}
