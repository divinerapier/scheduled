use std::sync::Arc;

use futures::StreamExt as _;
use k8s_openapi::api::batch::v1::CronJob;
use kube::{Api, Client, runtime::Controller};

use scheduled_cronjob::Context;
use scheduled_cronjob::ScheduledCronJob;
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), kube::Error> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .with_line_number(true)
        .init();

    let client = Client::try_default().await?;

    let scheduled_cronjobs = Api::<ScheduledCronJob>::all(client.clone());
    let cronjobs = Api::<CronJob>::all(client.clone());

    let ctx = Arc::new(Context::new(client));

    Controller::new(scheduled_cronjobs.clone(), Default::default())
        .shutdown_on_signal()
        .owns(cronjobs, Default::default())
        .run(
            scheduled_cronjob::reconcile_scheduled_cronjob,
            scheduled_cronjob::error_policy_for_scheduled_cronjob,
            ctx,
        )
        .for_each(|_| futures::future::ready(()))
        .await;

    Ok(())
}
