use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::core::object::HasStatus as _;
use kube::{CELSchema, Resource as _};
use kube::{CustomResource, ResourceExt, api::ObjectMeta};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::IntoTime;

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum DelayedJobPhase {
    #[default]
    #[serde(rename = "Pending")]
    Pending,
    #[serde(rename = "Running")]
    Running,
    #[serde(rename = "InvalidStartTime")]
    InvalidStartTime,
    #[serde(rename = "Failed")]
    Failed,
    #[serde(rename = "Completed")]
    Completed,
    #[serde(rename = "Unknown")]
    Unknown,
}

impl DelayedJobPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            DelayedJobPhase::Pending => "Pending",
            DelayedJobPhase::Running => "Running",
            DelayedJobPhase::InvalidStartTime => "InvalidStartTime",
            DelayedJobPhase::Failed => "Failed",
            DelayedJobPhase::Completed => "Completed",
            DelayedJobPhase::Unknown => "Unknown",
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, CELSchema)]
pub struct DelayedJobStatus {
    pub phase: DelayedJobPhase,
    pub message: Option<String>,
    pub last_update_time: Option<Time>,
}

#[derive(Debug, Serialize, Deserialize, CustomResource, Default, Clone, CELSchema)]
#[kube(
    group = "scheduled.divinerapier.io",
    version = "v1alpha1",
    kind = "DelayedJob",
    namespaced,
    printcolumn = r#"{"name":"Phase", "type":"string", "description":"phase of the job", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"StartTime", "type":"string", "description":"start time of the job", "jsonPath":".spec.startTime"}"#,
    status = "DelayedJobStatus",
    shortname = "dj"
)]
#[cel_validate(rule = Rule::new("has(self.spec.backoffLimit) && self.spec.backoffLimit >= 0").message("Invalid backoff limit").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("has(self.spec.template.spec.containers) && self.spec.template.spec.containers.size() > 0").message("Invalid containers").reason(Reason::FieldValueRequired))]
#[cel_validate(rule = Rule::new("has(self.spec.template.spec.restartPolicy) && self.spec.template.spec.restartPolicy in ['Always', 'OnFailure', 'Never']").message("Invalid restart policy").reason(Reason::FieldValueInvalid))]
#[serde(rename_all = "camelCase")]
pub struct DelayedJobSpec {
    /// Specifies the time to start the job.
    pub start_time: Option<Time>,

    /// Specifies the job that will be created when executing a DelayedJob.
    pub spec: JobSpec,
}

impl DelayedJobSpec {
    pub fn new<T>(start_time: T, spec: JobSpec) -> Result<Self, chrono::ParseError>
    where
        T: IntoTime,
    {
        Ok(Self {
            start_time: start_time.into_time()?,
            spec,
        })
    }
}

impl DelayedJob {
    pub fn can_run(&self) -> bool {
        match self.status() {
            Some(status) => match status.phase {
                DelayedJobPhase::Completed
                | DelayedJobPhase::Failed
                | DelayedJobPhase::InvalidStartTime => {
                    return false;
                }
                _ => {
                    return true;
                }
            },
            None => return true,
        }
    }
    pub fn job(&self) -> Job {
        Job {
            metadata: ObjectMeta {
                name: Some(self.name_any()),
                namespace: Some(self.namespace().unwrap_or_default()),
                owner_references: Some(vec![self.controller_owner_ref(&()).unwrap()]),
                labels: Some(self.labels().clone()),
                annotations: {
                    let mut annotations = self.labels().clone();
                    annotations.extend(vec![
                        (
                            "scheduled.divinerapier.io/managed-by".to_string(),
                            "delayedjob".to_string(),
                        ),
                        (
                            "scheduled.divinerapier.io/controller-name".to_string(),
                            self.name_any(),
                        ),
                    ]);
                    Some(annotations)
                },
                ..Default::default()
            },
            spec: Some(self.spec.spec.clone()),
            status: None,
        }
    }
}
