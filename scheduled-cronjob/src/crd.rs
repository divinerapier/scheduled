use chrono::{DateTime, Local};
use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec};
use kube::Resource as _;
use kube::core::object::HasStatus;
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

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledCronJobStatus {
    pub phase: ScheduledCronJobPhase,
    pub message: Option<String>,
    pub last_update_time: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, CustomResource, Default, Clone, JsonSchema)]
#[kube(
    group = "batch.divinerapier.io",
    version = "v1alpha1",
    kind = "ScheduledCronJob",
    namespaced
)]
#[kube(status = "ScheduledCronJobStatus")]
#[serde(rename_all = "camelCase")]
pub struct ScheduledCronJobSpec {
    start_time: Option<String>,
    end_time: Option<String>,
    pub spec: CronJobSpec,
}

impl ScheduledCronJobSpec {
    pub fn new<S, E>(start_time: S, end_time: E, spec: CronJobSpec) -> Self
    where
        S: Into<Option<String>>,
        E: Into<Option<String>>,
    {
        Self {
            start_time: start_time.into(),
            end_time: end_time.into(),
            spec,
        }
    }
}

impl ScheduledCronJob {
    pub fn cronjob(&self) -> CronJob {
        let namespace = self.namespace().unwrap_or_default();
        let name = self.name_any();
        let mut cronjob = CronJobBuilder::new()
            .with_namespace(namespace)
            .with_name(name)
            .with_spec(self.spec.spec.clone())
            .build();

        let oref = self.controller_owner_ref(&()).unwrap();
        cronjob.metadata.owner_references = Some(vec![oref]);
        cronjob
    }

    pub fn start_time(&self) -> Result<Option<DateTime<Local>>, chrono::ParseError> {
        parse_time(self.spec.start_time.as_deref())
    }

    pub fn end_time(&self) -> Result<Option<DateTime<Local>>, chrono::ParseError> {
        parse_time(self.spec.end_time.as_deref())
    }

    pub fn validate_effective_time(&self) -> Result<(), crate::Error> {
        let start_time = self
            .start_time()
            .map_err(|_| crate::Error::InvalidStartTime)?;
        let end_time = self.end_time().map_err(|_| crate::Error::InvalidEndTime)?;

        let now = Local::now();

        if let (Some(start_time), Some(end_time)) = (start_time, end_time) {
            if start_time > end_time {
                return Err(crate::Error::EndBeforeStart);
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
            Some("Forbid") | Some("Allow") | None => {}
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

        if let Some(backoff_limit) = spec.backoff_limit
            && backoff_limit < 0
        {
            return Err(crate::Error::InvalidBackoffLimit);
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

fn parse_time(time: Option<&str>) -> Result<Option<DateTime<Local>>, chrono::ParseError> {
    match time {
        Some(time) => Ok(Some(
            DateTime::parse_from_rfc3339(&time)?.with_timezone(&Local),
        )),
        None => Ok(None),
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

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct DelayedJobStatus {
    pub phase: Option<String>,
    pub message: Option<String>,
    pub last_update_time: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, CustomResource, Default, Clone, JsonSchema)]
#[kube(
    group = "batch.divinerapier.io",
    version = "v1alpha1",
    kind = "DelayedJob",
    namespaced
)]
#[kube(status = "DelayedJobStatus")]
#[serde(rename_all = "camelCase")]
pub struct DelayedJobSpec {
    start_time: Option<String>,
    end_time: Option<String>,
    pub spec: CronJobSpec,
}
