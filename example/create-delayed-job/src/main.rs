use k8s_openapi::api::{batch::v1::JobSpec, core::v1::PodTemplateSpec};
use kube::{
    Api, ResourceExt as _,
    api::{DeleteParams, ObjectMeta},
};
use scheduled::{DelayedJob, DelayedJobSpec};

#[tokio::main]
async fn main() {
    let client = kube::Client::try_default().await.unwrap();

    let cronjob = DelayedJob {
        metadata: ObjectMeta {
            name: Some("test-cronjob".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: DelayedJobSpec::new(
            "2025-04-16T17:44:00+08:00",
            JobSpec {
                template: PodTemplateSpec {
                    ..Default::default()
                },
                // schedule: "* * * * *".to_string(),
                // concurrency_policy: Some("Forbid".to_string()),
                // starting_deadline_seconds: Some(10),
                // job_template: JobTemplateSpec {
                //     metadata: Some(ObjectMeta {
                //         ..Default::default()
                //     }),
                //     spec: Some(JobSpec {
                //         backoff_limit: Some(3),
                //         template: PodTemplateSpec {
                //             spec: Some(PodSpec {
                //                 restart_policy: Some("OnFailure".to_string()),
                //                 containers: vec![Container {
                //                     name: "test".to_string(),
                //                     image: Some("ubuntu:22.04".to_string()),
                //                     command: Some(vec![
                //                         "/bin/bash".to_string(),
                //                         "-c".to_string(),
                //                         "echo 'Hello, world!' && sleep 10".to_string(),
                //                     ]),
                //                     ..Default::default()
                //                 }],
                //                 ..Default::default()
                //             }),
                //             ..Default::default()
                //         },
                //         ..Default::default()
                //     }),
                //     ..Default::default()
                // },
                ..Default::default()
            },
        )
        .unwrap(),
        status: None,
    };

    let api = Api::<DelayedJob>::namespaced(client.clone(), "default");
    let res = api
        .delete(&cronjob.name_any(), &DeleteParams::foreground())
        .await
        .map_err(scheduled::Error::from);
    if let Ok(_) | Err(scheduled::Error::NotFound) = res {
    } else {
        println!("delete: {:?}", res);
    }
    api.create(&Default::default(), &cronjob).await.unwrap();
}
