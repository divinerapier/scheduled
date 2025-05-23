use chrono::{DateTime, Datelike, Duration, Local, Timelike, Utc};
use derive_builder::Builder;
use k8s_openapi::api::batch::v1::{
    CronJob as K8sCronJob, CronJobSpec as K8sCronJobSpec, Job, JobSpec,
};
use k8s_openapi::api::core::v1::{EnvVar, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::core::object::HasStatus;
use kube::{CELSchema, Resource as _};
use kube::{CustomResource, ResourceExt, api::ObjectMeta};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::spec::ScheduleRule;

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

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CronJobStatus {
    /// The list of active jobs
    pub active_jobs: Vec<ObjectReference>,

    /// The number of times this CronJob has been executed
    pub execution_count: u32,

    /// The current phase of the CronJob
    pub phase: CronJobPhase,

    /// The message of the CronJob
    pub message: String,

    /// The time the last successful job was completed
    pub last_successful_time: Option<Time>,

    /// The time the last job was scheduled
    pub last_schedule_time: Option<Time>,
}

impl Default for CronJobStatus {
    fn default() -> Self {
        Self {
            active_jobs: vec![],
            execution_count: 0,
            phase: CronJobPhase::Pending,
            message: String::new(),
            last_successful_time: None,
            last_schedule_time: None,
        }
    }
}

impl CronJobStatus {
    pub fn update_execution_count(&mut self) -> &mut Self {
        self.execution_count += 1;
        self
    }

    pub fn update_phase(&mut self, phase: CronJobPhase) -> &mut Self {
        self.phase = phase;
        self
    }

    pub fn update_message(&mut self, message: String) -> &mut Self {
        self.message = message;
        self
    }

    pub fn update_last_successful_time(
        &mut self,
        last_successful_time: DateTime<Utc>,
    ) -> &mut Self {
        self.last_successful_time = Some(Time(last_successful_time));
        self
    }

    pub fn update_last_schedule_time(&mut self, last_schedule_time: DateTime<Utc>) -> &mut Self {
        self.last_schedule_time = Some(Time(last_schedule_time));
        self
    }
}

#[derive(Debug, Serialize, Deserialize, CELSchema, CustomResource, Default, Clone, Builder)]
#[kube(
    group = "scheduled.divinerapier.io",
    version = "v1alpha1",
    kind = "CronJob",
    namespaced,
    printcolumn = r#"{"name":"StartTime", "type":"string", "description":"start time of the job", "jsonPath":".spec.schedule.startTime"}"#,
    printcolumn = r#"{"name":"EndTime", "type":"string", "description":"end time of the job", "jsonPath":".spec.schedule.endTime"}"#,
    printcolumn = r#"{"name":"ConcurrencyPolicy", "type":"string", "description":"concurrency policy of the job", "jsonPath":".spec.schedule.concurrencyPolicy"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "description":"phase of the job", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"LastScheduleTime", "type":"string", "description":"last schedule time of the job", "jsonPath":".status.lastScheduleTime"}"#,
    printcolumn = r#"{"name":"LastSuccessfulTime", "type":"string", "description":"last successful time of the job", "jsonPath":".status.lastSuccessfulTime"}"#,
    status = "CronJobStatus"
)]
#[cel_validate(rule = Rule::new(r#"
        !has(self.schedule) || !has(self.schedule.startTime) || !has(self.schedule.endTime) ||
        (!has(self.schedule.startTime) && has(self.schedule.endTime)) || (has(self.schedule.startTime) && !has(self.schedule.endTime)) ||
        (has(self.schedule.startTime) && has(self.schedule.endTime) && self.schedule.startTime < self.schedule.endTime)
"#)
    .message(Message::Message("Invalid time range: startTime must be before endTime if both are specified or not specified together".to_string()))
    .reason(Reason::FieldValueForbidden))]
#[cel_validate(rule = Rule::new(r#"
        !has(self.schedule) ||
        (has(self.schedule) && has(self.schedule.concurrencyPolicy) && (self.schedule.concurrencyPolicy in ['Allow', 'Forbid', 'Replace']))"#)
    .message("Invalid concurrency policy. Valid values are: [Allow, Forbid, Replace]")
    .reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("!has(self.failedJobsHistoryLimit) || (has(self.failedJobsHistoryLimit) && self.failedJobsHistoryLimit >= 0)")
    .message("Invalid failed jobs history limit").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("!has(self.successfulJobsHistoryLimit) || (has(self.successfulJobsHistoryLimit) && self.successfulJobsHistoryLimit >= 0)")
    .message("Invalid successful jobs history limit")
    .reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("!has(self.spec.backoffLimit) || (has(self.spec.backoffLimit) && self.spec.backoffLimit >= 0)")
    .message("Invalid backoff limit")
    .reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("has(self.spec.template.spec.containers) && self.spec.template.spec.containers.size() > 0")
    .message("Invalid containers")
    .reason(Reason::FieldValueRequired))]
#[serde(rename_all = "camelCase")]
pub struct CronJobSpec {
    /// The number of failed finished jobs to retain. Value must be non-negative integer. Defaults to 1.
    #[builder(default = Some(1))]
    pub failed_jobs_history_limit: Option<i32>,

    /// The schedule in Cron format, see https://en.wikipedia.org/wiki/Cron.
    #[builder(default = None)]
    pub schedule: Option<ScheduleRule>,

    /// Specifies the job that will be created when executing a CronJob.
    pub spec: JobSpec,

    /// The number of successful finished jobs to retain. Value must be non-negative integer. Defaults to 3.
    #[builder(default = Some(3))]
    pub successful_jobs_history_limit: Option<i32>,

    /// This flag tells the controller to suspend subsequent executions, it does not apply to already started executions.
    /// Defaults to false.
    #[builder(default = None)]
    pub suspend: Option<bool>,
}

impl CronJobSpec {
    pub fn builder() -> CronJobSpecBuilder {
        CronJobSpecBuilder::default()
    }
}

impl CronJob {
    pub const ENV_SCHEDULED_JOB_NAMESPACE: &'static str = "SCHEDULED_JOB_NAMESPACE";
    pub const ENV_SCHEDULED_JOB_NAME: &'static str = "SCHEDULED_JOB_NAME";

    pub const LABEL_SCHEDULED_JOB_NAMESPACE: &'static str =
        "scheduled.divinerapier.io/cronjob-namespace";
    pub const LABEL_SCHEDULED_JOB_NAME: &'static str = "scheduled.divinerapier.io/cronjob-name";

    /// 本函数负责计算，基于最近一次成功调度时间之后的下一个调度时间。最近成功时间可以为空，表示还没有成功调度过。
    ///
    /// 如果，最近成功调度时间 `last_schedule_time` 为空，则返回创建时间和开始时间的较大值
    pub fn next_schedule_time_after(
        &mut self,
        now: DateTime<Local>,
        anchor: Option<DateTime<Local>>,
    ) -> Option<DateTime<Local>> {
        let execution_count = self.status.get_or_insert_default().execution_count as u32;

        if let Some(schedule) = self.spec.schedule.as_ref() {
            if let Some(max_executions) = schedule.max_executions {
                if execution_count == max_executions && self.active_jobs_count() > 0 {
                    return Some(now + Duration::seconds(5 * 50));
                }
                if execution_count >= max_executions {
                    return None;
                }
            }
        }

        // 获取最近一次调度成功时间，如果 anchor 不为空，则使用 anchor 作为基准时间
        // 否则，使用 status 中的 last_successful_time 作为基准时间
        let last_successful_time = anchor.or_else(|| {
            self.status()
                .as_ref()
                .map(|status| status.last_successful_time.clone())
                .flatten()
                .map(|t| t.0.with_timezone(&Local))
        });

        match last_successful_time {
            None => {
                // 如果最近一次调度时间为空，则使用创建时间和开始时间中的较大值最为首次运行时间
                let creation_time = self.creation_timestamp().map(|t| t.0.with_timezone(&Local));
                let start_time = self
                    .spec
                    .schedule
                    .as_ref()
                    .map(|s| s.start_time.clone())
                    .flatten()
                    .map(|t| t.0.with_timezone(&Local));
                let t = creation_time.max(start_time).or(Some(now));
                let next_time = match (t, self.spec.schedule.as_ref()) {
                    (Some(t), Some(schedule)) if schedule.is_after_end_time(t) => None,
                    (Some(t), _) => Some(t),
                    (None, _) => Some(now),
                };

                if next_time.is_none() {
                    return None;
                }

                if let Some(schedule) = self.spec.schedule.as_ref() {
                    if schedule.is_after_end_time(next_time.unwrap()) {
                        return None;
                    }
                }

                next_time
            }
            Some(last_successful_time) => {
                let last_time = last_successful_time;
                self.spec
                    .schedule
                    .as_ref()
                    .map(|s| s.next(execution_count, now, Some(last_time)))
                    .flatten()
            }
        }
    }

    /// 本函数负责计算，基于最近一次成功调度时间之后的下一个调度时间。最近成功时间可以为空，表示还没有成功调度过。
    ///
    /// 什么是最近成功调度时间？
    /// * 如果，最近成功调度时间 `last_schedule_time` 不为空，则直接使用
    /// * 否则，表示还未成功调度，使用创建时间和开始时间中的较大值
    ///
    /// 什么是下一次调度时间？
    ///
    /// 基于最近成功调度时间计算，根据调度策略计算出的下一次期望的调度时间。为了正确计算下一次的调度时间，需要考虑以下因素：
    ///
    /// 1. 创建时间
    /// 2. 上一次成功调度的时间
    /// 3. 策略中配置的开始时间
    /// 4. 当前时间
    /// 5. 策略中配置的结束时间
    /// 6. 策略中配置的最大等待时间
    /// 7. 策略中配置的最大执行次数
    /// 8. 策略中的并发策略
    /// 9. 周期调度策略
    ///
    /// * 如果没有调度成功过，使用创建时间和开始时间中的较大值最为首次运行时间
    /// * 如果已经调度成功过，使用上次成功调度时间作为基准时间，根据调度策略计算出下一次的调度时间
    ///
    /// 下一次的调度时间(next_time)需要满足
    ///
    /// 1. next_time > now
    /// 2. 如果设置了最大等待时间(starting_deadline_seconds), next_time < now < next_time + starting_deadline_seconds
    /// 3. 如果未设置最大等待时间(starting_deadline_seconds), next_time <= now < next_next_time
    ///
    /// 如果 next_time 已经到达，则需要根据并发策略计算下一次的调度时间
    ///
    ///
    pub fn next_schedule_time(&mut self, now: DateTime<Local>) -> Option<DateTime<Local>> {
        let next_time = self.next_schedule_time_after(now, None)?;
        // next_time 还没有到达，则返回时间给上层，上层会根据这个时间去等待重新调度
        if next_time > now {
            return Some(next_time);
        }

        // 如果设置了最大等待时间，则需要检查是否超过了最大等待时间

        let starting_deadline_seconds = self
            .spec
            .schedule
            .as_ref()
            .map(|schedule| schedule.starting_deadline_seconds)
            .flatten();

        tracing::info!(
            name = self.name_any(),
            namespace = self.namespace().unwrap_or_default(),
            starting_deadline_seconds = ?starting_deadline_seconds,
            next_time = ?next_time,
        );

        let next_time = match starting_deadline_seconds {
            // 如果 next_time + 最大等待时间 >= 当前时间，则说明应该运行了
            Some(seconds) if next_time + Duration::seconds(seconds) >= now => {
                tracing::warn!(
                    name = self.name_any(),
                    namespace = self.namespace().unwrap_or_default(),
                    seconds,
                    "Missed the deadline"
                );
                Some(next_time)
            }
            // next_time 已经到达，且没有配置最大等待时间，或者过期时间超过了配置的最大等待时间，这本次调度就是实实在在错过了，需要重新计算下一次的运行时间
            None | Some(_) => {
                // 前提: next_time <= now
                let mut next_time0 = next_time;
                let mut next_time1 = next_time;
                let mut missing_count = 0;
                let mut next_time = next_time;
                loop {
                    match self.next_schedule_time_after(now, Some(next_time)) {
                        Some(next_next_time) => {
                            next_time0 = next_time1;
                            next_time1 = next_time;
                            if next_time > now {
                                break;
                            }
                            missing_count += 1;
                            next_time = next_next_time;
                        }
                        None => {
                            next_time0 = next_time1;
                            break;
                        }
                    }
                }
                tracing::warn!(
                    name = self.name_any(),
                    namespace = self.namespace().unwrap_or_default(),
                    missing_count,
                );

                Some(next_time0)
            }
        };

        next_time
    }

    // pub fn update_schedule_time(&mut self, schedule_time: DateTime<Local>) {
    //     self.status
    //         .get_or_insert_default()
    //         .update_execution_count()
    //         .update_last_schedule_time(schedule_time.with_timezone(&Utc));
    // }

    pub fn active_jobs_count(&self) -> usize {
        self.status
            .as_ref()
            .map(|s| s.active_jobs.len())
            .unwrap_or(0)
    }

    pub fn active_jobs(&self) -> Option<&[ObjectReference]> {
        let status = self.status.as_ref()?;
        Some(&status.active_jobs)
    }

    pub fn active_jobs_mut(&mut self) -> Option<&mut Vec<ObjectReference>> {
        let status = self.status.as_mut()?;
        Some(&mut status.active_jobs)
    }

    pub fn contains_active_job(&self, uid: &str) -> bool {
        if let Some(status) = &self.status {
            if status
                .active_jobs
                .iter()
                .filter_map(|j| j.uid.as_ref())
                .any(|juid| juid == uid)
            {
                return true;
            }
        }
        false
    }

    pub fn delete_from_active_list(&mut self, job_uid: &str) -> bool {
        let status = self.status.get_or_insert_default();
        status.active_jobs.retain(|j| {
            if let Some(uid) = j.uid.as_ref() {
                uid != job_uid
            } else {
                false
            }
        });

        true
    }

    pub fn job_name(&self, time: &DateTime<Local>) -> String {
        let name = self.name_any();
        let year = time.year();
        let month = time.month();
        let day = time.day();
        let hour = time.hour();
        let minute = time.minute();
        let second = time.second();
        format!(
            "{}-{:02}-{:02}-{:02}-{:02}-{:02}-{:02}",
            name, year, month, day, hour, minute, second,
        )
    }

    pub fn batch_job(&self, schedule_time: &DateTime<Local>) -> Job {
        let cronjob_namespace = self.namespace().unwrap_or_default();
        let cronjob_name = self.name_any();
        let name = self.job_name(&schedule_time);
        // 构造 labels，包含原有 labels 和 cronjob 关联信息
        let mut labels = self.labels().clone();
        labels.insert(
            Self::LABEL_SCHEDULED_JOB_NAMESPACE.to_string(),
            cronjob_namespace.clone(),
        );
        labels.insert(
            Self::LABEL_SCHEDULED_JOB_NAME.to_string(),
            cronjob_name.clone(),
        );
        let mut job = Job {
            metadata: ObjectMeta {
                namespace: Some(cronjob_namespace.clone()),
                name: Some(name.clone()),
                annotations: Some(self.annotations().clone()),
                labels: Some(labels),
                owner_references: Some(vec![self.controller_owner_ref(&()).unwrap()]),
                creation_timestamp: Some(Time(schedule_time.with_timezone(&Utc))),
                ..Default::default()
            },
            spec: Some(self.spec.spec.clone()),
            status: None,
        };

        // Inject Job metadata as env vars into all containers
        if let Some(spec) = job.spec.as_mut() {
            if let Some(pod_spec) = spec.template.spec.as_mut() {
                let envs = vec![
                    EnvVar {
                        name: Self::ENV_SCHEDULED_JOB_NAMESPACE.to_string(),
                        value: Some(cronjob_namespace),
                        value_from: None,
                    },
                    EnvVar {
                        name: Self::ENV_SCHEDULED_JOB_NAME.to_string(),
                        value: Some(cronjob_name),
                        value_from: None,
                    },
                ];
                for container in &mut pod_spec.containers {
                    let container_env = container.env.get_or_insert_with(Vec::new);
                    container_env.extend(envs.clone());
                }
            }
        }

        if let Some(ref rule) = self.spec.schedule {
            if let Some(max_retries) = rule.max_retries {
                job.spec.get_or_insert_default().backoff_limit = Some(max_retries as i32);
            }

            if let Some(ref end_time) = rule.end_time {
                job.spec.get_or_insert_default().active_deadline_seconds = Some(
                    end_time
                        .0
                        .signed_duration_since(schedule_time)
                        .num_seconds() as i64,
                );
            }

            if job
                .spec
                .as_ref()
                .map(|spec| spec.ttl_seconds_after_finished)
                .flatten()
                .is_some()
            {
                job.spec.get_or_insert_default().ttl_seconds_after_finished = Some(24 * 60 * 60);
            }
        }

        job
    }

    pub fn start_time(&self) -> Option<DateTime<Local>> {
        let schedule = self.spec.schedule.as_ref()?;
        let start_time = schedule.start_time.as_ref()?;
        Some(start_time.0.with_timezone(&Local))
    }

    pub fn end_time(&self) -> Option<DateTime<Local>> {
        let schedule = self.spec.schedule.as_ref()?;
        let end_time = schedule.end_time.as_ref()?;
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

    pub fn is_deleted(&self) -> bool {
        self.metadata.deletion_timestamp.is_some()
    }

    pub fn validate_cronjob(&self) -> Result<(), crate::Error> {
        let spec = &self.spec;
        match spec.failed_jobs_history_limit {
            Some(limit) => {
                if limit < 0 {
                    return Err(crate::Error::InvalidFailedJobsHistoryLimit);
                }
            }
            None => {}
        }

        let spec = &spec.spec;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::spec::{Interval, Schedule, ScheduleType};
    use chrono::{DateTime, Local, TimeZone, Utc};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use std::default::Default;

    // Implement Default for CronJob
    impl Default for CronJob {
        fn default() -> Self {
            CronJob {
                metadata: kube::core::ObjectMeta::default(),
                spec: CronJobSpec::default(),
                status: Some(CronJobStatus::default()),
            }
        }
    }

    // fn create_test_cronjob(
    //     last_schedule_time: Option<DateTime<Local>>,
    //     max_executions: u32,
    //     execution_count: u32,
    //     rule: ScheduleRule,
    // ) -> CronJob {
    //     let mut cronjob = CronJob::default();
    //     let status = cronjob.status.get_or_insert_default();
    //     status.execution_count = execution_count;
    //     if let Some(time) = last_schedule_time {
    //         let utc_time = time.with_timezone(&Utc);
    //         status.last_schedule_time = Some(Time(utc_time));
    //     }

    //     cronjob.spec.schedule = Some(rule);

    //     cronjob
    // }

    use super::super::spec::ConcurrencyPolicy;

    struct TestCase {
        name: String,
        cronjob: CronJob,
        expectations: Vec<Option<DateTime<Local>>>,
    }

    #[derive(Default)]
    struct TestCaseBuilder {
        name: String,
        last_successful_time: Option<DateTime<Local>>,
        expectations: Vec<Option<DateTime<Local>>>,
        max_executions: Option<u32>,
        execution_count: u32,
        concurrency_policy: ConcurrencyPolicy,
        start_time: Option<Time>,
        end_time: Option<Time>,
        max_retries: Option<u32>,
        starting_deadline_seconds: Option<i64>,
        schedules: Vec<ScheduleType>,
    }

    impl TestCase {
        fn builder<S: Into<String>>(name: S) -> TestCaseBuilder {
            TestCaseBuilder::default().with_name(name.into())
        }
    }

    impl TestCaseBuilder {
        fn with_name(mut self, name: String) -> Self {
            self.name = name;
            self
        }

        fn with_schedule<T: Into<ScheduleType>>(mut self, schedule: T) -> Self {
            self.schedules.push(schedule.into());
            self
        }

        fn with_max_executions<T: Into<Option<u32>>>(mut self, max_executions: T) -> Self {
            self.max_executions = max_executions.into();
            self
        }

        fn with_concurrency_policy(mut self, concurrency_policy: ConcurrencyPolicy) -> Self {
            self.concurrency_policy = concurrency_policy;
            self
        }

        fn with_start_time<T: Into<Option<DateTime<Local>>>>(mut self, start_time: T) -> Self {
            self.start_time = start_time.into().map(|t| Time(t.with_timezone(&Utc)));
            self
        }

        fn with_end_time<T: Into<Option<DateTime<Local>>>>(mut self, end_time: T) -> Self {
            self.end_time = end_time.into().map(|t| Time(t.with_timezone(&Utc)));
            self
        }

        fn with_max_retries<T: Into<Option<u32>>>(mut self, max_retries: T) -> Self {
            self.max_retries = max_retries.into();
            self
        }

        fn with_starting_deadline_seconds<T: Into<Option<i64>>>(
            mut self,
            starting_deadline_seconds: T,
        ) -> Self {
            self.starting_deadline_seconds = starting_deadline_seconds.into();
            self
        }

        fn with_expectation<T: Into<Option<DateTime<Local>>>>(mut self, expectations: T) -> Self {
            self.expectations.push(expectations.into());
            self
        }

        fn with_last_successful_time<T: Into<Option<DateTime<Local>>>>(
            mut self,
            last_successful_time: T,
        ) -> Self {
            self.last_successful_time = last_successful_time.into();
            self
        }

        fn build(self) -> TestCase {
            let schedule = ScheduleRule::builder()
                .start_time(self.start_time)
                .end_time(self.end_time)
                .max_executions(self.max_executions)
                .starting_deadline_seconds(self.starting_deadline_seconds)
                .concurrency_policy(self.concurrency_policy)
                .max_retries(self.max_retries)
                .schedule(Schedule::from_iter(self.schedules))
                .build()
                .unwrap();

            let mut cronjob = CronJob::default();
            let status = cronjob.status.get_or_insert_default();
            status.execution_count = self.execution_count;
            status.last_successful_time = self
                .last_successful_time
                .map(|t| Time(t.with_timezone(&Utc)));

            cronjob.spec.schedule = Some(schedule);

            TestCase {
                name: self.name,
                cronjob,
                expectations: self.expectations,
            }
        }
    }

    fn assert_time_approximately_equal(
        actual: DateTime<Local>,
        expected: DateTime<Local>,
        tolerance_milliseconds: i64,
    ) -> bool {
        let diff = (actual - expected).num_milliseconds().abs();
        diff <= tolerance_milliseconds
    }

    #[test]
    fn test_next() {
        let test_cases = vec![
            TestCase::builder("max_executions_0")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(0)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(None)
                .with_end_time(None)
                .with_max_retries(0)
                .with_starting_deadline_seconds(None)
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("max_executions_1")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(1)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(None)
                .with_end_time(None)
                .with_max_retries(0)
                .with_starting_deadline_seconds(None)
                .with_expectation(Local::now())
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("max_executions_2")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(2)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(None)
                .with_end_time(None)
                .with_max_retries(0)
                .with_starting_deadline_seconds(None)
                .with_expectation(Local::now())
                .with_expectation(Local::now() + Duration::seconds(300))
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("max_executions_3")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(3)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(None)
                .with_end_time(None)
                .with_max_retries(0)
                .with_starting_deadline_seconds(None)
                .with_expectation(Local::now())
                .with_expectation(Local::now() + Duration::seconds(300))
                .with_expectation(Local::now() + Duration::seconds(600))
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("future_start_time")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(3)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(Local.with_ymd_and_hms(3000, 1, 1, 0, 0, 0).unwrap())
                .with_end_time(None)
                .with_expectation(Local.with_ymd_and_hms(3000, 1, 1, 0, 0, 0).unwrap())
                .with_expectation(Local.with_ymd_and_hms(3000, 1, 1, 0, 5, 0).unwrap())
                .with_expectation(Local.with_ymd_and_hms(3000, 1, 1, 0, 10, 0).unwrap())
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("exceed_end_time")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(3)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_end_time(Local.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap())
                .with_expectation(None)
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("between_start_end_time1")
                .with_schedule(Interval { seconds: 300 })
                .with_max_executions(3)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(Local::now() - Duration::seconds(1000))
                .with_end_time(Local::now() + Duration::seconds(1000))
                .with_expectation(Local::now() - Duration::seconds(100))
                .with_expectation(Local::now() + Duration::seconds(200))
                .with_expectation(Local::now() + Duration::seconds(500))
                .build(),
            TestCase::builder("between_start_end_time2")
                .with_schedule(Interval { seconds: 180 })
                .with_max_executions(10)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(Local::now() - Duration::minutes(1))
                .with_end_time(Local::now() + Duration::minutes(10))
                .with_expectation(Local::now() - Duration::minutes(1))
                .with_expectation(Local::now() + Duration::minutes(2))
                .with_expectation(Local::now() + Duration::minutes(5))
                .with_expectation(Local::now() + Duration::minutes(8))
                .with_expectation(None)
                .with_expectation(None)
                .build(),
            TestCase::builder("between_start_end_time3")
                .with_schedule(Interval { seconds: 180 })
                .with_max_executions(10)
                .with_concurrency_policy(ConcurrencyPolicy::Allow)
                .with_start_time(Local::now() - Duration::hours(1))
                .with_last_successful_time(Local::now() - Duration::minutes(20))
                .with_end_time(Local::now() + Duration::minutes(10))
                .with_expectation(Local::now() - Duration::minutes(2))
                .with_expectation(Local::now() + Duration::minutes(1))
                .with_expectation(Local::now() + Duration::minutes(4))
                .with_expectation(Local::now() + Duration::minutes(7))
                .with_expectation(Local::now() + Duration::minutes(10))
                .with_expectation(None)
                .with_expectation(None)
                .build(),
        ];

        let now = Local::now();

        for test_case in test_cases {
            let mut cronjob = test_case.cronjob;

            // let mut last_schedule_time = test_case.last_schedule_time;

            for (_i, expectation) in test_case.expectations.iter().enumerate() {
                let next_time = cronjob.next_schedule_time(now);
                match (next_time, expectation) {
                    (Some(actual), Some(expected)) => {
                        assert!(
                            (actual - expected).num_milliseconds().abs() <= 50,
                            "Test case #{} Time difference {} milliseconds exceeds tolerance of {} milliseconds. expected: {:?}, actual: {:?}, all: {:?}",
                            test_case.name,
                            (actual - expected).num_milliseconds().abs(),
                            50,
                            expected,
                            actual,
                            test_case.expectations
                        );
                    }
                    (None, None) => {}
                    _ => panic!(
                        "Test case '{}' failed\n\texpected {:?}\n\tgot {:?}\n\tall: {:?}",
                        test_case.name, *expectation, next_time, test_case.expectations
                    ),
                }
                // last_schedule_time = next_time;
                let x = cronjob
                    .status
                    .get_or_insert_default()
                    .update_execution_count();
                if let Some(next_time) = next_time {
                    x.update_last_successful_time(next_time.with_timezone(&Utc));
                }
            }
        }
    }
}
