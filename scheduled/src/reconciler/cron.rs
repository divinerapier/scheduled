use std::{sync::Arc, time::Duration};

use k8s_openapi::api::batch::v1::CronJob as K8sCronJob;
use kube::{ResourceExt as _, core::object::HasStatus, runtime::controller::Action};
use tracing::{debug, error, info, warn};

use crate::{Context, CronJob, Error, crd::CronJobPhase};

pub async fn reconcile(job: Arc<CronJob>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = job.name_any();
    let namespace = job.namespace().unwrap_or_default();
    info!(name, namespace, "Starting reconciliation");

    match reconcile_cronjob_impl(&job, ctx.clone()).await {
        Ok(action) => {
            debug!(
                name,
                namespace,
                ?action,
                "Reconciliation completed successfully"
            );
            Ok(action)
        }
        Err(Error::NotFound) => {
            unreachable!()
        }
        Err(Error::UnretriableK8SError(e)) => {
            error!(name, namespace, error = ?e, "Unretriable Kubernetes API error");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                &format!("Unretriable Kubernetes API error: {}", e),
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidStartTime) => {
            warn!(name, namespace, "Invalid start time specified");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::InvalidStartTime,
                "Warning",
                "Invalid start time specified",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidEndTime) => {
            warn!(name, namespace, "Invalid end time specified");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::InvalidEndTime,
                "Warning",
                "Invalid end time specified",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::EndBeforeStart) => {
            warn!(name, namespace, "End time is before start time");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::EndBeforeStart,
                "Warning",
                "End time is before start time",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::WaitFor(duration)) => {
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Pending,
                "Normal",
                "Waiting for scheduled time",
            )
            .await?;

            let duration = duration.to_std().unwrap();

            tracing::info!(name = name, namespace = namespace, duration = ?duration, "Waiting for scheduled time");

            Ok(Action::requeue(duration))
        }
        Err(Error::Expired(_)) => {
            info!(name, namespace, "Schedule has completed");
            ctx.delete::<K8sCronJob>(&namespace, &name).await?;

            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Completed,
                "Normal",
                "Schedule has completed",
            )
            .await?;

            Ok(Action::await_change())
        }
        Err(Error::Kube(e)) => {
            error!(name, namespace, error = ?e, "Kubernetes API error");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                e.to_string().as_str(),
            )
            .await?;

            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Err(Error::Serialization(e)) => {
            error!(name, namespace, error = ?e, "Serialization error");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                e.to_string().as_str(),
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Err(Error::InvalidConcurrencyPolicy) => {
            warn!(name, namespace, "Invalid concurrency policy");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                "Invalid concurrency policy",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidFailedJobsHistoryLimit) => {
            warn!(name, namespace, "Invalid failed jobs history limit");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                "Invalid failed jobs history limit",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::CronjobSpecNotFound) => {
            warn!(name, namespace, "Cronjob spec not found");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                "Cronjob spec not found",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidBackoffLimit) => {
            warn!(name, namespace, "Invalid backoff limit");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                "Invalid backoff limit",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(e @ Error::DurationTooShort(start, end)) => {
            warn!(name, namespace, start = ?start, end = ?end, "Duration too short");
            ctx.update_scheduled_cronjob(
                &job,
                CronJobPhase::Failed,
                "Warning",
                e.to_string().as_str(),
            )
            .await?;
            Ok(Action::await_change())
        }
    }
}

async fn reconcile_cronjob_impl(job: &CronJob, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = job.name_any();
    let namespace = job.namespace().unwrap_or_default();
    info!(name, namespace, "Starting cronjob reconciliation");

    // 检查当前状态，如果当前状态就是无法运行的，直接返回
    if !job.can_run() {
        info!(name, namespace, "Job cannot run in current state");
        return Ok(Action::await_change());
    }

    job.validate_cronjob()?;

    // 验证时间范围，如果时间有问题，或者已经超时了，也返回，由上层创建事件，修改状态
    info!(name, namespace, "Validating effective time");
    job.validate_effective_time()?;
    info!(name, namespace, "Time validation passed");

    // 获取或创建 CronJob
    info!(name, namespace, "Getting or creating cronjob");
    let _cronjob = get_cronjob(ctx.clone(), &namespace, &name, job).await?;
    info!(name, namespace, "Cronjob operation completed");

    // 此时，cronjob 应该已经创建出来了，根据当前 scheduled cronjob 的状态，更新
    match job.status() {
        Some(status) => {
            info!(name, namespace, phase = ?status.phase, "Current job status");
            match status.phase {
                CronJobPhase::Pending | CronJobPhase::Unknown => {
                    info!(name, namespace, "Updating status to Running");
                    ctx.update_scheduled_cronjob(
                        job,
                        CronJobPhase::Running,
                        "Normal",
                        "Job is running",
                    )
                    .await?;
                }
                CronJobPhase::Running => {
                    debug!(name, namespace, "Job is already running");
                }
                _ => {
                    unreachable!()
                }
            }
        }
        None => {
            info!(name, namespace, "No status found, setting to Running");
            ctx.update_scheduled_cronjob(job, CronJobPhase::Running, "Normal", "Job is running")
                .await?;
        }
    }

    // 设置重新检查间隔
    info!(name, namespace, "Setting requeue interval to 120 seconds");
    Ok(Action::requeue(Duration::from_secs(120)))
}

async fn get_cronjob(
    ctx: Arc<Context>,
    namespace: &str,
    name: &str,
    job: &CronJob,
) -> Result<K8sCronJob, Error> {
    debug!(name, namespace, "Attempting to get cronjob");
    match ctx.get::<K8sCronJob>(namespace, name).await {
        Ok(cronjob) => {
            debug!(name, namespace, "Cronjob found");
            Ok(cronjob)
        }
        Err(Error::NotFound) => {
            info!(name, namespace, "Cronjob not found, creating new one");
            Ok(ctx.create_cronjob(namespace, &job.cronjob()).await?)
        }
        Err(e) => {
            error!(name, namespace, error = ?e, "Error getting cronjob");
            Err(e)
        }
    }
}
