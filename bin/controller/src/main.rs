use std::sync::Arc;

use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use kube::{Api, Client, runtime::controller::Controller};
use scheduled_cronjob::{
    Context,
    crd::{DelayedJob, ScheduledCronJob},
    reconciler::{reconcile_delayed_job, reconcile_scheduled_cronjob},
};
use tracing_subscriber::filter::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), kube::Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .with_line_number(true)
        .init();

    let client = Client::try_default().await?;

    // 创建 API 客户端
    let scheduled_cronjobs = Api::<ScheduledCronJob>::all(client.clone());
    let delayed_jobs = Api::<DelayedJob>::all(client.clone());
    let cronjobs = Api::<CronJob>::all(client.clone());
    let jobs = Api::<k8s_openapi::api::batch::v1::Job>::all(client.clone());

    let ctx = Arc::new(Context::new(client));

    tokio::select! {
        _ = run_scheduled_cronjob_controller(scheduled_cronjobs, cronjobs, ctx.clone()) => {},
        _ = run_delayed_job_controller(delayed_jobs, jobs, ctx.clone()) => {},
    }

    Ok(())
}

async fn run_scheduled_cronjob_controller(
    scheduled_cronjobs: Api<ScheduledCronJob>,
    cronjobs: Api<CronJob>,
    ctx: Arc<Context>,
) {
    Controller::new(scheduled_cronjobs.clone(), Default::default())
        .shutdown_on_signal()
        .owns(cronjobs, Default::default())
        .run(
            reconcile_scheduled_cronjob,
            scheduled_cronjob::error_policy,
            ctx.clone(),
        )
        .for_each(|_| futures::future::ready(()))
        .await;
}

async fn run_delayed_job_controller(
    delayed_jobs: Api<DelayedJob>,
    jobs: Api<Job>,
    ctx: Arc<Context>,
) {
    Controller::new(delayed_jobs.clone(), Default::default())
        .shutdown_on_signal()
        .owns(jobs, Default::default())
        .run(
            reconcile_delayed_job,
            scheduled_cronjob::error_policy,
            ctx.clone(),
        )
        .for_each(|_| futures::future::ready(()))
        .await;
}
