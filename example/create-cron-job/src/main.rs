use std::time::Duration;

use chrono::DateTime;
use k8s_openapi::{
    api::{
        batch::v1::JobSpec,
        core::v1::{Container, PodSpec, PodTemplateSpec},
    },
    apimachinery::pkg::apis::meta::v1::Time,
};
use kube::{
    Api, ResourceExt as _,
    api::{DeleteParams, ObjectMeta},
};
use scheduled::{CronJob, CronJobSpec, Interval, ScheduleRule, ScheduleType};

#[tokio::main]
async fn main() {
    let client = kube::Client::try_default().await.unwrap();

    let spec = CronJobSpec::builder()
        .schedule(
            ScheduleRule::builder()
                .start_time(Some(Time(
                    DateTime::parse_from_rfc3339("2025-04-16T17:44:00+08:00")
                        .unwrap()
                        .to_utc(),
                )))
                .schedule(
                    vec![ScheduleType::Interval(Interval::from(Duration::from_secs(
                        10,
                    )))]
                    .into(),
                )
                .build()
                .unwrap()
                .into(),
        )
        .spec(JobSpec {
            template: PodTemplateSpec {
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "test-container".to_string(),
                        image: Some("ubuntu:24.04".to_string()),
                        command: Some(["/bin/bash", "-c", "sleep 10"].map(String::from).to_vec()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        })
        .build()
        .unwrap();

    let cronjob = CronJob {
        metadata: ObjectMeta {
            name: Some("test-cronjob".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec,
        status: None,
    };

    let api = Api::<CronJob>::namespaced(client.clone(), "default");
    if let Ok(_) | Err(scheduled::Error::NotFound) = api
        .delete(&cronjob.name_any(), &DeleteParams::foreground())
        .await
        .map_err(scheduled::Error::from)
    {
    } else {
    }
    api.create(&Default::default(), &cronjob).await.unwrap();
}
