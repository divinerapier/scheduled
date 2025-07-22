use std::sync::Arc;

use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::Job;
use kube::{Api, Client, runtime::controller::Controller};
use scheduled::{
    Context,
    crd::{CronJob, DelayedJob},
    reconciler::{reconcile_delayed_job, reconcile_scheduled_cronjob},
};
use tracing::info;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), kube::Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .with_line_number(true)
        .init();

    let client = Client::try_default().await?;

    // 创建 API 客户端
    let scheduled_cronjobs = Api::<CronJob>::all(client.clone());
    let delayed_jobs = Api::<DelayedJob>::all(client.clone());
    let jobs = Api::<Job>::all(client.clone());

    let ctx = Arc::new(Context::new(client));

    tokio::select! {
        _ = run_scheduled_cronjob_controller(scheduled_cronjobs, jobs.clone(), ctx.clone()) => {},
        _ = run_delayed_job_controller(delayed_jobs, jobs, ctx.clone()) => {},
        // 增加一个响应 ctrl-c 的信号，当收到信号时，强制退出程序
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received, force shutting down");
            std::process::exit(0);
        }
    }

    Ok(())
}

async fn run_scheduled_cronjob_controller(
    scheduled_cronjobs: Api<CronJob>,
    jobs: Api<Job>,
    ctx: Arc<Context>,
) {
    info!("run_scheduled_cronjob_controller");
    Controller::new(scheduled_cronjobs.clone(), Default::default())
        .owns(jobs, Default::default())
        .run(
            reconcile_scheduled_cronjob,
            scheduled::error_policy,
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
    info!("run_delayed_job_controller");
    Controller::new(delayed_jobs.clone(), Default::default())
        .owns(jobs, Default::default())
        .run(reconcile_delayed_job, scheduled::error_policy, ctx.clone())
        .for_each(|_| futures::future::ready(()))
        .await;
}
