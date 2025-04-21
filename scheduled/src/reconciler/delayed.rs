use std::{sync::Arc, time::Duration};

use chrono::Utc;
use k8s_openapi::api::batch::v1::Job;
use kube::{
    ResourceExt as _,
    runtime::{conditions, controller::Action, wait::Condition},
};
use tracing::{debug, error, info, warn};

use crate::{
    Context, Error,
    crd::{DelayedJob, DelayedJobPhase},
};

pub async fn reconcile(job: Arc<DelayedJob>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = job.name_any();
    let namespace = job.namespace().unwrap_or_default();

    info!(name, namespace, "Starting delayed job reconciliation");

    match implement(&job, ctx.clone()).await {
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
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                &format!("Unretriable Kubernetes API error: {}", e),
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidStartTime) => {
            warn!(name, namespace, "Invalid start time specified");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::InvalidStartTime,
                "Warning",
                "Invalid start time specified",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidEndTime) => {
            unreachable!()
        }
        Err(Error::EndBeforeStart) => {
            unreachable!()
        }
        Err(Error::WaitFor(duration)) => {
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Pending,
                "Normal",
                "Waiting for scheduled time",
            )
            .await?;

            let duration = duration.to_std().unwrap();

            tracing::info!(name = name, namespace = namespace, duration = ?duration, "Waiting for scheduled time");

            Ok(Action::requeue(duration))
        }
        Err(Error::Expired(_)) => {
            unreachable!()
        }
        Err(Error::Kube(e)) => {
            error!(name, namespace, error = ?e, "Kubernetes API error");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                e.to_string().as_str(),
            )
            .await?;

            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Err(Error::Serialization(e)) => {
            error!(name, namespace, error = ?e, "Serialization error");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                e.to_string().as_str(),
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Err(Error::InvalidConcurrencyPolicy) => {
            warn!(name, namespace, "Invalid concurrency policy");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                "Invalid concurrency policy",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidFailedJobsHistoryLimit) => {
            warn!(name, namespace, "Invalid failed jobs history limit");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                "Invalid failed jobs history limit",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::CronjobSpecNotFound) => {
            warn!(name, namespace, "Cronjob spec not found");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                "Cronjob spec not found",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(Error::InvalidBackoffLimit) => {
            warn!(name, namespace, "Invalid backoff limit");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                "Invalid backoff limit",
            )
            .await?;
            Ok(Action::await_change())
        }
        Err(e @ Error::DurationTooShort(start, end)) => {
            warn!(name, namespace, start = ?start, end = ?end, "Duration too short");
            ctx.update_delayed_job(
                &job,
                DelayedJobPhase::Failed,
                "Warning",
                e.to_string().as_str(),
            )
            .await?;
            Ok(Action::await_change())
        }
    }
}

// 判断 Job 是否正在运行
fn is_job_running(obj: &Job) -> bool {
    if let Some(status) = &obj.status {
        if let Some(active) = status.active {
            return active > 0;
        }
    }
    false
}

// 判断 Job 是否失败
fn is_job_failed(obj: &Job) -> bool {
    if let Some(status) = &obj.status {
        if let Some(failed) = status.failed {
            return failed > 0;
        }
    }
    false
}

async fn implement(delayed_job: &DelayedJob, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = delayed_job.name_any();
    let namespace = delayed_job.namespace().unwrap_or_default();

    info!(name, namespace, "Starting delayed job reconciliation");

    if !delayed_job.can_run() {
        info!(name, namespace, "Job cannot run in current state");
        return Ok(Action::await_change());
    }

    if let Some(ref start_time) = delayed_job.spec.start_time {
        let now = Utc::now();
        if start_time.0 > now {
            let wait_for = (start_time.0 - now).to_std().unwrap();
            return Ok(Action::requeue(wait_for));
        }
    }

    let job = match ctx.get::<Job>(&namespace, &name).await {
        Ok(job) => job,
        Err(Error::NotFound) => ctx.create::<Job>(&namespace, &delayed_job.job()).await?,
        Err(e) => return Err(e),
    };

    if conditions::is_job_completed().matches_object(Some(&job)) {
        // Only update status if phase changed
        if delayed_job.status.as_ref().map(|s| s.phase) == Some(DelayedJobPhase::Completed) {
            return Ok(Action::await_change());
        }

        ctx.update_delayed_job(
            delayed_job,
            DelayedJobPhase::Completed,
            "Job completed",
            "Job completed",
        )
        .await?;
        ctx.delete::<Job>(&namespace, &name).await?;
        return Ok(Action::await_change());
    }

    if is_job_running(&job) {
        info!(name, namespace, "Job is running");
        // Only update status if phase changed
        if delayed_job.status.as_ref().map(|s| s.phase) != Some(DelayedJobPhase::Running) {
            ctx.update_delayed_job(
                delayed_job,
                DelayedJobPhase::Running,
                "Normal",
                "Job is running",
            )
            .await?;
        }
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    if is_job_failed(&job) {
        // Only update status if phase changed
        if delayed_job.status.as_ref().map(|s| s.phase) == Some(DelayedJobPhase::Failed) {
            return Ok(Action::await_change());
        }

        let failed_count = job.status.as_ref().and_then(|s| s.failed).unwrap_or(0);
        let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(6);

        if failed_count >= backoff_limit {
            warn!(
                name,
                namespace, failed_count, backoff_limit, "Job failed after maximum retries"
            );
            ctx.update_delayed_job(
                delayed_job,
                DelayedJobPhase::Failed,
                "Warning",
                &format!("Job failed after {} retries", failed_count),
            )
            .await?;
            ctx.delete::<Job>(&namespace, &name).await?;
            return Ok(Action::await_change());
        }

        info!(
            name,
            namespace, failed_count, backoff_limit, "Job failed, waiting for retry"
        );
        ctx.update_delayed_job(
            delayed_job,
            DelayedJobPhase::Failed,
            "Warning",
            &format!("Job failed, retry {}/{}", failed_count, backoff_limit),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    Ok(Action::await_change())
}
