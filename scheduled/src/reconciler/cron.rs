use std::{cmp::Ordering, collections::BTreeSet, sync::Arc, time::Duration};

use chrono::{DateTime, Local};
use k8s_openapi::{
    api::{batch::v1::Job, core::v1::ObjectReference},
    apimachinery::pkg::apis::meta::v1::Time,
};
use kube::{Resource, ResourceExt as _, runtime::controller::Action};
use tracing::{debug, error, info, warn};

use super::{
    conditions::{self, JobFinishedType},
    ext::OwnerReferenceExt as _,
};
use crate::{
    Context, CronJob, Error,
    crd::{CronJobPhase, spec::ConcurrencyPolicy},
};

pub async fn reconcile(job: Arc<CronJob>, ctx: Arc<Context>) -> Result<Action, Error> {
    let reconciler = CronJobReconciler::new(ctx);

    match reconciler.reconcile(job.clone()).await {
        Ok((action, mut job, update_status)) => {
            let name = job.name_any();
            let namespace = job.namespace().unwrap_or_default();
            debug!(
                name,
                namespace,
                ?action,
                "Reconciliation completed successfully"
            );
            if update_status {
                job.status.get_or_insert_default().last_schedule_time =
                    Some(Time(Local::now().to_utc()));
                reconciler.ctx.update_status(&job).await?;
            }
            Ok(action)
        }
        Err(e) => reconciler.handle_error(CronJob::clone(&job), e).await,
    }
}

struct CronJobReconciler {
    ctx: Arc<Context>,
}

impl CronJobReconciler {
    pub fn new(ctx: Arc<Context>) -> Self {
        Self { ctx }
    }

    async fn jobs_to_be_reconciled(&self, namespace: &str, uid: &str) -> Result<Vec<Job>, Error> {
        let jobs = self
            .ctx
            .list::<Job>(&namespace)
            .await?
            .into_iter()
            .filter(|job| match job.meta().owner_references {
                Some(ref owner_references) => owner_references
                    .iter()
                    .any(|owner_reference| owner_reference.uid.eq(uid)),
                None => false,
            })
            .collect::<Vec<Job>>();

        Ok(jobs)
    }

    /// 清理已经完成的 Job
    async fn cleanup_finished_jobs<'s>(
        &self,
        mut cronjob: CronJob,
        jobs: &'s [Job],
        update_status: &mut bool,
    ) -> Result<CronJob, Error> {
        // Remove old finished jobs based on history limits
        self.delete_finished_jobs(&mut cronjob, jobs, update_status)
            .await?;

        debug!("after delete_finished_jobs");

        let namespace = cronjob.meta().namespace.as_deref().unwrap_or_default();
        let name = cronjob.meta().name.as_deref().unwrap_or_default();
        let mut cronjob = self.get_cronjob(namespace, name).await?;

        // // Remove finished jobs from active_jobs
        // let (mut cronjob, children_jobs) = self
        //     .remove_finished_jobs_from_actives(cronjob, jobs, update_status)
        //     .await?;

        debug!("after remove_finished_jobs_from_actives");

        // Record missing active jobs if a job exists but is not in active_jobs
        self.record_missing_active_jobs(&mut cronjob, update_status)
            .await?;

        debug!("after record_missing_active_jobs");

        // 检查是否所有jobs都完成且没有下一次调度时间
        if cronjob.active_jobs_count() == 0 {
            let now = Local::now();
            if cronjob.next_schedule_time(now).is_none() {
                info!(
                    name,
                    namespace, "All jobs completed and no next schedule time, marking as completed"
                );
                let status = cronjob.status.get_or_insert_default();
                status.phase = CronJobPhase::Completed;
                status.message = "All jobs completed and schedule has ended".to_string();
                *update_status = true;
            }
        }

        Ok(cronjob)
    }

    async fn record_missing_active_jobs(
        &self,
        cronjob: &mut CronJob,
        update_status: &mut bool,
    ) -> Result<(), Error> {
        let name = cronjob.name_any();
        let namespace = cronjob.namespace().unwrap_or_default();

        if let Some(active_jobs) = cronjob.active_jobs().map(Vec::from) {
            info!(
                name,
                namespace,
                active_jobs_count = active_jobs.len(),
                "Checking active jobs for cleanup"
            );

            for job_ref in active_jobs {
                let job_namespace = job_ref.namespace.as_deref().unwrap_or_default();
                let job_name = job_ref.name.as_deref().unwrap_or_default();
                let job_uid = job_ref.uid.as_deref().unwrap_or_default();

                match self.ctx.get::<Job>(job_namespace, job_name).await {
                    Ok(job) => {
                        if conditions::is_job_finished(&job) {
                            info!(
                                name,
                                namespace,
                                job_name,
                                job_namespace,
                                "Removing finished job from active list"
                            );
                            cronjob.delete_from_active_list(job_uid);
                            *update_status = true;
                        } else {
                            debug!(
                                name,
                                namespace, job_name, job_namespace, "Job is still running"
                            );
                        }
                    }
                    Err(Error::NotFound) => {
                        info!(
                            name,
                            namespace,
                            job_name,
                            job_namespace,
                            "Job not found, removing from active list"
                        );
                        cronjob.delete_from_active_list(job_uid);
                        *update_status = true;
                    }
                    Err(e) => {
                        error!(name, namespace, job_name, job_namespace, error = ?e, "Error getting job");
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// 删除已经完成的 Job
    async fn delete_finished_jobs(
        &self,
        cronjob: &mut CronJob,
        jobs: &[Job],
        update_status: &mut bool,
    ) -> Result<(), Error> {
        let mut failed_jobs = vec![];
        let mut successful_jobs = vec![];
        let failed_jobs_history_limit = cronjob.spec.failed_jobs_history_limit.unwrap_or(1);
        let successful_jobs_history_limit = cronjob.spec.successful_jobs_history_limit.unwrap_or(3);

        let mut active_jobs_mut = cronjob.active_jobs_mut();

        for job in jobs {
            let finished = match conditions::get_job_finished_type(job) {
                Some(conditions::JobFinishedType::Complete) => {
                    successful_jobs.push(job);
                    true
                }
                Some(conditions::JobFinishedType::Failed) => {
                    failed_jobs.push(job);
                    true
                }
                None => false,
            };
            if !finished {
                continue;
            }
            if let Some(active_jobs_mut) = active_jobs_mut.as_deref_mut() {
                active_jobs_mut.retain(|j| {
                    j.uid.as_deref().unwrap_or_default()
                        == job.meta().uid.as_deref().unwrap_or_default()
                });
                *update_status = true;
            }
        }

        if successful_jobs_history_limit > 0 {
            *update_status = self
                .remove_oldest_jobs(
                    cronjob,
                    successful_jobs.as_mut_slice(),
                    successful_jobs_history_limit,
                )
                .await
                || *update_status;
        }

        if failed_jobs_history_limit > 0 {
            *update_status = self
                .remove_oldest_jobs(
                    cronjob,
                    failed_jobs.as_mut_slice(),
                    failed_jobs_history_limit,
                )
                .await
                || *update_status;
        }

        Ok(())
    }

    fn delete_from_active_list(&self, cronjob: &mut CronJob, uid: &str) -> bool {
        let status = cronjob.status.get_or_insert_default();

        status
            .active_jobs
            .retain(|j| j.uid.as_deref().unwrap_or_default() != uid);

        true
    }

    async fn delete_job(&self, cronjob: &mut CronJob, job: &Job) -> bool {
        let job_name = job.name_any();
        let job_namespace = job.namespace().unwrap_or_default();

        if let Err(e) = self
            .ctx
            .delete_background::<Job>(&job_namespace, &job_name)
            .await
        {
            error!(job_name, job_namespace, error = ?e, "Error deleting job");
            return false;
        }

        let _ = self.delete_from_active_list(cronjob, job.uid().as_ref().unwrap());
        let _ = self
            .ctx
            .create_scheduled_cronjob_event(
                &cronjob,
                "Normal",
                "SuccessfulDelete",
                &format!("Job {} deleted", job_name),
            )
            .await;
        true
    }

    async fn remove_oldest_jobs(
        &self,
        cronjob: &mut CronJob,
        jobs: &mut [&Job],
        max_jobs: i32,
    ) -> bool {
        let num_to_delete = if jobs.len() <= max_jobs as usize {
            0
        } else {
            jobs.len() - max_jobs as usize
        };

        if num_to_delete <= 0 {
            return false;
        }

        let start_time = |job: &Job| -> Option<Time> { job.status.as_ref()?.start_time.clone() };

        let mut update_status = false;

        jobs.sort_by(|a, b| match (start_time(a), start_time(b)) {
            (Some(atime), Some(btime)) => match atime.cmp(&btime) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => a.name_any().cmp(&b.name_any()),
                Ordering::Greater => Ordering::Greater,
            },
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => a.name_any().cmp(&b.name_any()),
        });

        for job in jobs.iter().take(num_to_delete) {
            update_status = self.delete_job(cronjob, job).await || update_status;
        }

        update_status
    }

    async fn precheck(&self, namespace: &str, name: &str) -> Result<Option<CronJob>, Error> {
        let cronjob = match self.get_cronjob(&namespace, &name).await {
            Ok(cronjob) => cronjob,
            Err(Error::NotFound) => {
                info!(name, namespace, "Cronjob not found, may be it is deleted");
                return Ok(None);
            }
            Err(e) => {
                error!(name, namespace, error = ?e, "Error getting cronjob");
                return Err(e);
            }
        };
        if cronjob.is_deleted() {
            info!(name, namespace, "Cronjob is deleted");
            return Ok(None);
        }

        if !cronjob.can_run() {
            info!(name, namespace, "Job cannot run in current state");
            return Ok(None);
        }

        cronjob.validate_cronjob()?;

        Ok(Some(cronjob))
    }

    async fn get_cronjob(&self, namespace: &str, name: &str) -> Result<CronJob, Error> {
        let cronjob = match self.ctx.get::<CronJob>(&namespace, &name).await {
            Ok(cronjob) => cronjob,
            Err(crate::Error::NotFound) => {
                info!(name, namespace, "Cronjob not found, may be it is deleted");
                return Err(Error::NotFound);
            }
            Err(e) => {
                error!(name, namespace, error = ?e, "Error getting cronjob");
                return Err(e);
            }
        };

        Ok(cronjob)
    }

    async fn reconcile(&self, job: Arc<CronJob>) -> Result<(Action, CronJob, bool), Error> {
        let name = job.name_any();
        let namespace = job.namespace().unwrap_or_default();

        info!(name, namespace, "Starting cronjob reconciliation");

        // Check current status if it is ok to run. The following cases are considered:
        // * If **CronJob** is not found.
        // * If **CronJob** is deleted.
        // * If **CronJob** has invalid spec.
        // * If **CronJob** with unrunnable status.
        let cronjob = match self.precheck(&namespace, &name).await? {
            Some(cronjob) => cronjob,
            None => {
                info!(name, namespace, "Cronjob not found, may be it is deleted");
                return Ok((Action::await_change(), CronJob::clone(&job), false));
            }
        };

        // List all jobs that need to be reconciled.
        let jobs = match job.uid() {
            Some(uid) => self.jobs_to_be_reconciled(&namespace, &uid).await?,
            None => {
                error!(name, namespace, "Cronjob has no uid");
                return Ok((Action::await_change(), CronJob::clone(&job), false));
            }
        };

        let mut update_status = false;

        debug!("before cleanup_finished_jobs");

        // Clean up tasks, including three parts:
        // 1. Delete completed jobs
        // 2. Remove completed job UIDs from active_jobs
        // 3. Record timestamp if job is not in active_jobs
        let cronjob = self
            .cleanup_finished_jobs(cronjob, &jobs, &mut update_status)
            .await?;

        debug!("after cleanup_finished_jobs");

        if cronjob.is_deleted() {
            info!(name, namespace, "Cronjob is deleted");
            return Ok((Action::await_change(), cronjob, update_status));
        }

        let now = Local::now();

        info!(name, namespace, now=?now, "before sync_cronjob");

        let (next_time, mut cronjob) = self.sync_cronjob(cronjob, &mut update_status, now).await?;

        info!(name, namespace, next_time=?next_time, "after sync_cronjob");

        let action = match next_time {
            Some(next_time) => {
                info!(name, namespace, now = ?now, next_time = ?next_time, future = next_time > now, "Next time is in the future");
                Action::requeue(next_time.signed_duration_since(now).to_std().unwrap())
            }
            None => {
                // 统一的兜底校验：检查是否有正在运行的jobs
                let has_active_jobs = cronjob.active_jobs_count() > 0;
                let status = cronjob.status.get_or_insert_default();

                if has_active_jobs {
                    info!(
                        name,
                        namespace,
                        "No next schedule time but still have active jobs, keeping as Running"
                    );
                    status.phase = CronJobPhase::Running;
                    status.message = "Waiting for active jobs to complete".to_string();
                } else {
                    info!(
                        name,
                        namespace, "No next schedule time and no active jobs, marking as Completed"
                    );
                    status.phase = CronJobPhase::Completed;
                    status.message = "Cronjob has completed".to_string();
                }
                update_status = true;
                Action::await_change()
            }
        };

        info!(name, namespace, action=?action, "update_status: {:?}", update_status);

        info!(name, namespace, action = ?action, update_status = update_status, "Cronjob reconciliation completed");
        Ok((action, cronjob, update_status))
    }

    async fn handle_error(&self, mut job: CronJob, e: Error) -> Result<Action, Error> {
        let name = job.name_any();
        let namespace = job.namespace().unwrap_or_default();
        info!(name, namespace, "Starting reconciliation");

        let status = job.status.get_or_insert_default();

        let action = match e {
            Error::NotFound => {
                warn!(name, namespace, "Cronjob not found");
                Ok(Action::await_change())
            }
            Error::AlreadyExists(name, namespace, time) => {
                warn!(name, namespace, time = ?time, "Job already exists");
                Ok(Action::await_change())
            }
            Error::UnretriableK8SError(e) => {
                error!(name, namespace, error = ?e, "Unretriable Kubernetes API error");
                status.phase = CronJobPhase::Failed;
                status.message = format!("Unretriable Kubernetes API error: {}", e);
                Ok(Action::await_change())
            }
            Error::InvalidStartTime => {
                warn!(name, namespace, "Invalid start time specified");
                status.phase = CronJobPhase::InvalidStartTime;
                status.message = "Invalid start time specified".to_string();
                Ok(Action::await_change())
            }
            Error::InvalidEndTime => {
                warn!(name, namespace, "Invalid end time specified");
                status.phase = CronJobPhase::InvalidEndTime;
                status.message = "Invalid end time specified".to_string();
                Ok(Action::await_change())
            }
            Error::EndBeforeStart => {
                warn!(name, namespace, "End time is before start time");
                status.phase = CronJobPhase::EndBeforeStart;
                status.message = "End time is before start time".to_string();
                Ok(Action::await_change())
            }
            Error::WaitFor(duration) => {
                status.phase = CronJobPhase::Pending;
                status.message = "Waiting for scheduled time".to_string();

                let duration = duration.to_std().unwrap();

                tracing::info!(name = name, namespace = namespace, duration = ?duration, "Waiting for scheduled time");

                Ok(Action::requeue(duration))
            }
            Error::Expired(_) => {
                info!(name, namespace, "Schedule has completed");

                status.phase = CronJobPhase::Completed;
                status.message = "Schedule has completed".to_string();

                Ok(Action::await_change())
            }
            Error::Kube(e) => {
                error!(name, namespace, error = ?e, "Kubernetes API error");

                status.phase = CronJobPhase::Failed;
                status.message = e.to_string();

                Ok(Action::requeue(Duration::from_secs(5)))
            }
            Error::Serialization(e) => {
                error!(name, namespace, error = ?e, "Serialization error");
                status.phase = CronJobPhase::Failed;
                status.message = e.to_string();

                Ok(Action::requeue(Duration::from_secs(5)))
            }
            Error::InvalidConcurrencyPolicy => {
                warn!(name, namespace, "Invalid concurrency policy");
                status.phase = CronJobPhase::Failed;
                status.message = "Invalid concurrency policy".to_string();

                Ok(Action::await_change())
            }
            Error::InvalidFailedJobsHistoryLimit => {
                warn!(name, namespace, "Invalid failed jobs history limit");
                status.phase = CronJobPhase::Failed;
                status.message = "Invalid failed jobs history limit".to_string();

                Ok(Action::await_change())
            }
            Error::CronjobSpecNotFound => {
                warn!(name, namespace, "Cronjob spec not found");
                status.phase = CronJobPhase::Failed;
                status.message = "Cronjob spec not found".to_string();

                Ok(Action::await_change())
            }
            Error::InvalidBackoffLimit => {
                warn!(name, namespace, "Invalid backoff limit");
                status.phase = CronJobPhase::Failed;
                status.message = "Invalid backoff limit".to_string();

                Ok(Action::await_change())
            }
            Error::DurationTooShort(start, end) => {
                warn!(name, namespace, start = ?start, end = ?end, "Duration too short");
                status.phase = CronJobPhase::Failed;
                status.message = e.to_string();

                Ok(Action::await_change())
            }
        };

        self.ctx.update_status(&job).await?;

        action
    }

    /// 同步 cronjob 的状态，返回下一次任务执行的时间，如果 cronjob 已经结束，则返回 None
    /// 上层根据返回值时间决定等待时间
    async fn sync_cronjob<'s>(
        &self,
        mut cronjob: CronJob,
        update_status: &mut bool,
        now: DateTime<Local>,
    ) -> Result<(Option<DateTime<Local>>, CronJob), Error> {
        let name = cronjob.name_any();
        let namespace = cronjob.namespace().unwrap_or_default();

        let end_time = cronjob.end_time();

        // 如果 cronjob 已经结束，则删除所有任务
        if let Some(end_time) = end_time {
            if end_time <= now {
                info!(name, namespace, "Cronjob has ended");
                cronjob.status.get_or_insert_default().phase = CronJobPhase::Completed;
                return Ok((None, cronjob));
            }
        }

        if let Some(next_schedule_time) = cronjob
            .status
            .as_ref()
            .and_then(|status| status.next_schedule_time.as_ref())
        {
            // 上次计算的调度时间
            let next_schedule_time = next_schedule_time.0.with_timezone(&Local);
            // 最大等待时间
            let not_arrived = now < next_schedule_time;
            tracing::info!(name, namespace, now=?now, next_schedule_time=?next_schedule_time, not_arrived = not_arrived, "next_schedule_time");
            if not_arrived {
                return Ok((Some(next_schedule_time), cronjob));
            }
            let starting_deadline_seconds = cronjob
                .spec
                .schedule
                .as_ref()
                .and_then(|schedule| schedule.starting_deadline_seconds)
                .unwrap_or(120); // 默认等待 120 秒
            // 最晚运行时间 = 上次计算的调度时间 + 最大等待时间
            let starting_deadline =
                next_schedule_time + Duration::from_secs(starting_deadline_seconds as u64);
            let missed = now > starting_deadline;
            tracing::info!(name, namespace, starting_deadline=?starting_deadline, now=?now, missed = missed, "check missed");
            // 当前时间晚于计算的最大调度截止时间，说明不在运行上次的任务
            if missed {
                // 计算下一次的调度时间
                match cronjob.next_schedule_time(next_schedule_time) {
                    Some(next_time) => {
                        let status = cronjob.status.get_or_insert_default();
                        status.next_schedule_time = Some(Time(next_time.to_utc()));
                        status.last_successful_time = Some(Time(now.to_utc()));
                        *update_status = true;
                        return Ok((Some(next_time), cronjob));
                    }
                    None => {
                        info!(name, namespace, "No next schedule time");
                        cronjob.status.get_or_insert_default().phase = CronJobPhase::Completed;
                        return Ok((None, cronjob));
                    }
                }
            }
            // 判断任务是否在运行

            // 这个任务已经存在了，则返回下一次要执行的时间
            if self.check_job_exists(&mut cronjob, &next_schedule_time) {
                tracing::info!(name, namespace, schedule_time=?next_schedule_time, "job exists");
                return Ok((
                    cronjob.next_schedule_time_after(now, Some(next_schedule_time)),
                    cronjob,
                ));
            }

            // 根据 concurrency_policy 处理已经存在的任务
            let next_time = match self
                .process_concurrency_policy(&mut cronjob, now, next_schedule_time, update_status)
                .await?
            {
                Some(next_time) => next_time,
                None => {
                    return Ok((None, cronjob));
                }
            };

            tracing::info!(name, namespace, current_schedule_time=?next_schedule_time, adviced_schedule_time=?next_time, "check process_concurrency_policy");
            if next_time == next_schedule_time {
                // 说明当前时间可以运行
                info!(name, namespace, "Ready to create job");
                self.create_job(&mut cronjob, &next_time, update_status)
                    .await?;

                match cronjob.next_schedule_time(now) {
                    Some(next_time) => {
                        let status = cronjob.status.get_or_insert_default();
                        status.phase = CronJobPhase::Running;
                        status.last_successful_time = Some(Time(now.to_utc()));
                        status.last_schedule_time = Some(Time(now.to_utc()));
                        status.execution_count += 1;
                        status.next_schedule_time = Some(Time(next_time.to_utc()));
                        *update_status = true;
                        return Ok((Some(next_time), cronjob));
                    }
                    None => {
                        info!(name, namespace, "No next schedule time, but job is running");
                        // 即使没有下一次调度时间，如果当前有active jobs，也不应该立即标记为completed
                        // 应该等待所有jobs完成后再标记为completed
                        let status = cronjob.status.get_or_insert_default();
                        status.phase = CronJobPhase::Running;
                        status.last_successful_time = Some(Time(now.to_utc()));
                        status.last_schedule_time = Some(Time(now.to_utc()));
                        status.execution_count += 1;
                        *update_status = true;
                        return Ok((None, cronjob));
                    }
                }
            } else {
                let status = cronjob.status.get_or_insert_default();
                status.phase = CronJobPhase::Running;
                status.last_successful_time = Some(Time(now.to_utc()));
                status.last_schedule_time = Some(Time(now.to_utc()));
                status.execution_count += 1;
                status.next_schedule_time = Some(Time(next_time.to_utc()));
                *update_status = true;
                return Ok((Some(next_time), cronjob));
            }
        }

        info!(name, namespace, "before next_schedule_time");

        // 获取下一次任务执行的时间，如果 cronjob 已经结束，则返回 None
        let next_time = match cronjob.next_schedule_time(now) {
            Some(next_time) => next_time,
            None => {
                info!(name, namespace, "No next schedule time");
                // 检查是否还有active jobs，如果有则不应该立即标记为completed
                if cronjob.active_jobs_count() > 0 {
                    info!(
                        name,
                        namespace, "Still have active jobs, keeping as Running"
                    );
                    let status = cronjob.status.get_or_insert_default();
                    status.phase = CronJobPhase::Running;
                    *update_status = true;
                    return Ok((None, cronjob));
                } else {
                    cronjob.status.get_or_insert_default().phase = CronJobPhase::Completed;
                    return Ok((None, cronjob));
                }
            }
        };

        info!(name, namespace, next_time=?next_time, "after next_schedule_time");

        // 下次执行时间超过当前时间，表明暂时没有到达执行时间，返回目标时间
        if next_time > now {
            info!(name, namespace, next_time = ?next_time, "Next time is in the future");
            let status = cronjob.status.get_or_insert_default();
            status.next_schedule_time = Some(Time(next_time.to_utc()));
            *update_status = true;
            return Ok((Some(next_time), cronjob));
        }

        // next_time <= now

        // 这个任务已经存在了，则返回下一次要执行的时间
        if self.check_job_exists(&mut cronjob, &next_time) {
            return Ok((
                cronjob.next_schedule_time_after(now, Some(next_time)),
                cronjob,
            ));
        }

        info!(name, namespace, "before process_concurrency_policy");

        // 根据 concurrency_policy 处理已经存在的任务
        let next_time = match self
            .process_concurrency_policy(&mut cronjob, now, next_time, update_status)
            .await?
        {
            Some(next_time) => next_time,
            None => {
                return Ok((None, cronjob));
            }
        };

        if next_time > now {
            return Ok((Some(next_time), cronjob));
        }

        info!(name, namespace, "Ready to create job");

        self.create_job(&mut cronjob, &next_time, update_status)
            .await?;

        let status = cronjob.status.get_or_insert_default();

        status.phase = CronJobPhase::Running;
        status.last_successful_time = Some(Time(next_time.to_utc()));
        status.execution_count += 1;

        // 更新一些状态

        Ok((
            cronjob.next_schedule_time_after(now, Some(next_time)),
            cronjob,
        ))
    }

    async fn create_job(
        &self,
        cronjob: &mut CronJob,
        time: &DateTime<Local>,
        update_status: &mut bool,
    ) -> Result<Option<DateTime<Local>>, Error> {
        let job = cronjob.batch_job(time);
        let namespace = cronjob.meta().namespace.as_deref().unwrap_or_default();
        let job_ref = match self.ctx.create::<Job>(&namespace, &job).await {
            Ok(_) => {
                let job = self.ctx.get::<Job>(&namespace, &job.name_any()).await?;
                if !job.is_controlled_by(cronjob) {
                    return Err(Error::AlreadyExists(
                        job.name_any(),
                        namespace.to_string(),
                        time.clone(),
                    ));
                }

                ObjectReference {
                    uid: job.meta().uid.clone(),
                    namespace: Some(namespace.to_string()),
                    name: Some(job.name_any()),
                    ..Default::default()
                }
            }
            Err(Error::Kube(kube::Error::Api(resp))) if resp.code == 409 => {
                let job = self.ctx.get::<Job>(&namespace, &job.name_any()).await?;
                if !job.is_controlled_by(cronjob) {
                    return Err(Error::AlreadyExists(
                        job.name_any(),
                        namespace.to_string(),
                        time.clone(),
                    ));
                }

                let _contains =
                    cronjob.contains_active_job(job.uid().as_deref().unwrap_or_default());

                // if contains {
                return Err(Error::AlreadyExists(
                    job.name_any(),
                    namespace.to_string(),
                    time.clone(),
                ));
                // }
            }
            Err(e) => return Err(e),
        };
        *update_status = true;

        let name = cronjob.name_any();
        info!(name, namespace, time=?time, "Created job");

        let status = cronjob.status.get_or_insert_default();
        status.active_jobs.push(job_ref);

        Ok(None)
    }

    fn check_job_exists(&self, cronjob: &mut CronJob, time: &DateTime<Local>) -> bool {
        let job_name = cronjob.job_name(time);

        if let Some(active_jobs) = cronjob.active_jobs() {
            for job in active_jobs {
                let namespace = job.namespace.as_deref().unwrap_or_default();
                let name = job.name.as_deref().unwrap_or_default();
                if namespace == cronjob.namespace().unwrap_or_default() && name == job_name {
                    return true;
                }
            }
        }

        let last_successful_time = cronjob
            .status
            .as_ref()
            .and_then(|status| status.last_successful_time.clone());

        if let Some(last_successful_time) = last_successful_time {
            if last_successful_time.0 == *time {
                return true;
            }
        }

        false
    }

    /// 根据 concurrency_policy 处理已经存在的任务
    /// * `Forbid` 如果当前有正在运行的任务，则返回下一次要执行的时间
    /// * `Replace` 删除当前正在运行的任务，并返回下一次要执行的时间
    /// * `Allow` 如果当前有正在运行的任务，则返回下一次要执行的时间
    async fn process_concurrency_policy(
        &self,
        cronjob: &mut CronJob,
        now: DateTime<Local>,
        next_time: DateTime<Local>,
        update_status: &mut bool,
    ) -> Result<Option<DateTime<Local>>, Error> {
        if cronjob.spec.schedule.is_none() {
            return Ok(Some(next_time));
        }
        let schedule = cronjob.spec.schedule.as_ref().unwrap();
        match schedule.concurrency_policy {
            ConcurrencyPolicy::Forbid if cronjob.active_jobs_count() > 0 => {
                tracing::info!(
                    "Forbid concurrency policy, but there are active jobs, next_time: {}",
                    next_time
                );
                // 在Forbid策略中，如果有active jobs，需要清理已完成的jobs
                // 但保持正在运行的jobs
                self.cleanup_active_jobs_if_needed(cronjob, update_status)
                    .await?;
                Ok(cronjob.next_schedule_time_after(now, Some(next_time)))
            }
            ConcurrencyPolicy::Replace => {
                // 先收集需要删除的job UIDs，避免借用冲突
                let job_uids: Vec<String> = cronjob
                    .active_jobs()
                    .unwrap_or_default()
                    .iter()
                    .map(|j| j.uid.as_deref().unwrap_or_default().to_string())
                    .collect();

                for j in cronjob.active_jobs().unwrap_or_default() {
                    let namespace = j.namespace.as_deref().unwrap_or_default();
                    let name = j.name.as_deref().unwrap_or_default();
                    match self.ctx.delete::<Job>(namespace, name).await {
                        Ok(_) => {}
                        Err(Error::NotFound) => {
                            continue;
                        }
                        Err(e) => {
                            error!(name, namespace, error = ?e, "Error deleting job");
                            return Err(e);
                        }
                    }
                    *update_status = true;
                }

                // 从active_jobs列表中移除被删除的job
                for uid in job_uids {
                    self.delete_from_active_list(cronjob, &uid);
                }

                Ok(Some(next_time))
            }
            _ => Ok(Some(next_time)),
        }
    }

    async fn cleanup_active_jobs_if_needed(
        &self,
        cronjob: &mut CronJob,
        update_status: &mut bool,
    ) -> Result<(), Error> {
        let mut active_jobs_to_delete = vec![];

        if let Some(active_jobs) = cronjob.active_jobs() {
            for job_ref in active_jobs {
                let namespace = job_ref.namespace.as_deref().unwrap_or_default();
                let name = job_ref.name.as_deref().unwrap_or_default();
                match self.ctx.get::<Job>(namespace, name).await {
                    Ok(job) => {
                        if conditions::is_job_finished(&job) {
                            active_jobs_to_delete
                                .push(job_ref.uid.as_deref().unwrap_or_default().to_string());
                        }
                    }
                    Err(Error::NotFound) => {
                        active_jobs_to_delete
                            .push(job_ref.uid.as_deref().unwrap_or_default().to_string());
                    }
                    Err(e) => {
                        error!(name, namespace, error = ?e, "Error getting job for cleanup");
                        return Err(e);
                    }
                }
            }
        }

        for uid in active_jobs_to_delete {
            self.delete_from_active_list(cronjob, &uid);
        }
        *update_status = true;
        Ok(())
    }
}
