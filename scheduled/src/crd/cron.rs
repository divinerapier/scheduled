use crate::crd::IntoTime;
use chrono::{DateTime, Duration, Local};
use k8s_openapi::api::batch::v1::{CronJob as K8sCronJob, CronJobSpec as K8sCronJobSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::core::object::HasStatus;
use kube::{CELSchema, Resource as _};
use kube::{CustomResource, ResourceExt, api::ObjectMeta};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum CronJobPhase {
    #[default]
    #[serde(rename = "Pending")]
    Pending,
    #[serde(rename = "Running")]
    Running,
    #[serde(rename = "InvalidStartTime")]
    InvalidStartTime,
    #[serde(rename = "InvalidEndTime")]
    InvalidEndTime,
    #[serde(rename = "EndBeforeStart")]
    EndBeforeStart,
    #[serde(rename = "Failed")]
    Failed,
    #[serde(rename = "Completed")]
    Completed,
    #[serde(rename = "Unknown")]
    Unknown,
}

impl CronJobPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            CronJobPhase::Pending => "Pending",
            CronJobPhase::Running => "Running",
            CronJobPhase::InvalidStartTime => "InvalidStartTime",
            CronJobPhase::InvalidEndTime => "InvalidEndTime",
            CronJobPhase::EndBeforeStart => "EndBeforeStart",
            CronJobPhase::Failed => "Failed",
            CronJobPhase::Completed => "Completed",
            CronJobPhase::Unknown => "Unknown",
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CronJobStatus {
    pub phase: CronJobPhase,
    pub message: Option<String>,
    pub last_update_time: Option<Time>,
}

#[derive(Debug, Serialize, Deserialize, CELSchema, CustomResource, Default, Clone)]
#[kube(
    group = "scheduled.divinerapier.io",
    version = "v1alpha1",
    kind = "CronJob",
    namespaced,
    printcolumn = r#"{"name":"StartTime", "type":"string", "description":"start time of the job", "jsonPath":".spec.startTime"}"#,
    printcolumn = r#"{"name":"EndTime", "type":"string", "description":"end time of the job", "jsonPath":".spec.endTime"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "description":"phase of the job", "jsonPath":".status.phase"}"#,
    status = "CronJobStatus",
    shortname = "scj"
)]
#[cel_validate(rule = Rule::new("(!has(self.startTime) && !has(self.endTime)) || (!has(self.startTime) && has(self.endTime)) || (has(self.startTime) && !has(self.endTime)) || (has(self.startTime) && has(self.endTime) && self.startTime < self.endTime)")
.message(Message::Message("Invalid time range".to_string()))
.reason(Reason::FieldValueForbidden))]
#[cel_validate(rule = Rule::new("has(self.spec.concurrencyPolicy) && (self.spec.concurrencyPolicy in ['Allow', 'Forbid', 'Replace'])").message("Invalid concurrency policy").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("has(self.spec.failedJobsHistoryLimit) && self.spec.failedJobsHistoryLimit >= 0").message("Invalid failed jobs history limit").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("has(self.spec.successfulJobsHistoryLimit) && self.spec.successfulJobsHistoryLimit >= 0").message("Invalid successful jobs history limit").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("has(self.spec.jobTemplate.spec.backoffLimit) && self.spec.jobTemplate.spec.backoffLimit >= 0").message("Invalid backoff limit").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("has(self.spec.jobTemplate.spec.template.spec.containers) && self.spec.jobTemplate.spec.template.spec.containers.size() > 0").message("Invalid containers").reason(Reason::FieldValueRequired))]
#[cel_validate(rule = Rule::new("has(self.spec.jobTemplate.spec.template.spec.restartPolicy) && self.spec.jobTemplate.spec.template.spec.restartPolicy in ['Always', 'OnFailure', 'Never']").message("Invalid restart policy").reason(Reason::FieldValueInvalid))]
#[serde(rename_all = "camelCase")]
pub struct CronJobSpec {
    start_time: Option<Time>,
    end_time: Option<Time>,

    pub spec: K8sCronJobSpec,
}

impl CronJobSpec {
    pub fn new<S, E>(
        start_time: S,
        end_time: E,
        spec: K8sCronJobSpec,
    ) -> Result<Self, chrono::ParseError>
    where
        S: IntoTime,
        E: IntoTime,
    {
        Ok(Self {
            start_time: start_time.into_time()?,
            end_time: end_time.into_time()?,
            spec,
        })
    }
}

impl CronJob {
    pub fn cronjob(&self) -> K8sCronJob {
        K8sCronJob {
            metadata: ObjectMeta {
                namespace: Some(self.namespace().unwrap_or_default()),
                name: Some(self.name_any()),
                annotations: Some(self.annotations().clone()),
                labels: Some(self.labels().clone()),
                owner_references: Some(vec![self.controller_owner_ref(&()).unwrap()]),
                ..Default::default()
            },
            spec: Some(self.spec.spec.clone()),
            status: None,
        }
    }

    pub fn start_time(&self) -> Option<DateTime<Local>> {
        let start_time = self.spec.start_time.as_ref()?;
        Some(start_time.0.with_timezone(&Local))
    }

    pub fn end_time(&self) -> Option<DateTime<Local>> {
        let end_time = self.spec.end_time.as_ref()?;
        Some(end_time.0.with_timezone(&Local))
    }

    pub fn validate_effective_time(&self) -> Result<(), crate::Error> {
        let start_time = self.start_time();
        let end_time = self.end_time();
        let now = Local::now();

        if let (Some(start_time), Some(end_time)) = (start_time, end_time) {
            if start_time >= end_time {
                return Err(crate::Error::EndBeforeStart);
            }
            if end_time - start_time < Duration::minutes(5) {
                return Err(crate::Error::DurationTooShort(start_time, end_time));
            }
        }

        if let Some(start_time) = start_time {
            if now < start_time {
                return Err(crate::Error::WaitFor(start_time - now));
            }
        }

        if let Some(end_time) = end_time {
            if now > end_time {
                return Err(crate::Error::Expired(end_time));
            }
        }

        Ok(())
    }

    pub fn can_run(&self) -> bool {
        match self.status() {
            Some(status) => match status.phase {
                CronJobPhase::Completed
                | CronJobPhase::Failed
                | CronJobPhase::InvalidEndTime
                | CronJobPhase::InvalidStartTime
                | CronJobPhase::EndBeforeStart => {
                    return false;
                }
                _ => {
                    return true;
                }
            },
            None => return true,
        }
    }

    pub fn validate_cronjob(&self) -> Result<(), crate::Error> {
        let spec = &self.spec.spec;
        match spec.concurrency_policy.as_deref() {
            Some("Forbid") | Some("Allow") | Some("Replace") | None => {}
            _ => {
                return Err(crate::Error::InvalidConcurrencyPolicy);
            }
        }
        match spec.failed_jobs_history_limit {
            Some(limit) => {
                if limit < 0 {
                    return Err(crate::Error::InvalidFailedJobsHistoryLimit);
                }
            }
            None => {}
        }

        let Some(spec) = &spec.job_template.spec else {
            return Err(crate::Error::CronjobSpecNotFound);
        };

        if let Some(backoff_limit) = spec.backoff_limit {
            if backoff_limit < 0 {
                return Err(crate::Error::InvalidBackoffLimit);
            }
        }

        let Some(spec) = &spec.template.spec else {
            return Err(crate::Error::CronjobSpecNotFound);
        };

        if spec.containers.is_empty() {
            return Err(crate::Error::CronjobSpecNotFound);
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct CronJobBuilder {
    metadata: ObjectMeta,
    spec: K8sCronJobSpec,
}

impl CronJobBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.metadata.namespace = Some(namespace.into());
        self
    }

    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.metadata.name = Some(name.into());
        self
    }

    pub fn with_spec(mut self, spec: K8sCronJobSpec) -> Self {
        self.spec = spec;
        self
    }

    pub fn build(self) -> K8sCronJob {
        K8sCronJob {
            metadata: self.metadata,
            spec: Some(self.spec),
            ..Default::default()
        }
    }
}
