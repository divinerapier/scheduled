use crate::crd::IntoTime;
use chrono::{DateTime, Duration, Local};
use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::core::object::HasStatus;
use kube::{CELSchema, Resource as _};
use kube::{CustomResource, ResourceExt, api::ObjectMeta};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum ScheduledCronJobPhase {
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

impl ScheduledCronJobPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScheduledCronJobPhase::Pending => "Pending",
            ScheduledCronJobPhase::Running => "Running",
            ScheduledCronJobPhase::InvalidStartTime => "InvalidStartTime",
            ScheduledCronJobPhase::InvalidEndTime => "InvalidEndTime",
            ScheduledCronJobPhase::EndBeforeStart => "EndBeforeStart",
            ScheduledCronJobPhase::Failed => "Failed",
            ScheduledCronJobPhase::Completed => "Completed",
            ScheduledCronJobPhase::Unknown => "Unknown",
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledCronJobStatus {
    pub phase: ScheduledCronJobPhase,
    pub message: Option<String>,
    pub last_update_time: Option<Time>,
}

#[derive(Debug, Serialize, Deserialize, CELSchema, CustomResource, Default, Clone)]
#[kube(
    group = "batch.divinerapier.io",
    version = "v1alpha1",
    kind = "ScheduledCronJob",
    namespaced,
    printcolumn = r#"{"name":"StartTime", "type":"string", "description":"start time of the job", "jsonPath":".spec.startTime"}"#,
    printcolumn = r#"{"name":"EndTime", "type":"string", "description":"end time of the job", "jsonPath":".spec.endTime"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "description":"phase of the job", "jsonPath":".status.phase"}"#,
    status = "ScheduledCronJobStatus",
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
pub struct ScheduledCronJobSpec {
    start_time: Option<Time>,
    end_time: Option<Time>,

    pub spec: CronJobSpec,
}

impl ScheduledCronJobSpec {
    pub fn new<S, E>(
        start_time: S,
        end_time: E,
        spec: CronJobSpec,
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

impl ScheduledCronJob {
    pub fn cronjob(&self) -> CronJob {
        CronJob {
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
                ScheduledCronJobPhase::Completed
                | ScheduledCronJobPhase::Failed
                | ScheduledCronJobPhase::InvalidEndTime
                | ScheduledCronJobPhase::InvalidStartTime
                | ScheduledCronJobPhase::EndBeforeStart => {
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
    spec: CronJobSpec,
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

    pub fn with_spec(mut self, spec: CronJobSpec) -> Self {
        self.spec = spec;
        self
    }

    pub fn build(self) -> CronJob {
        CronJob {
            metadata: self.metadata,
            spec: Some(self.spec),
            ..Default::default()
        }
    }
}
