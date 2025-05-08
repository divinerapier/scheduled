use std::ops::Deref;
use std::sync::Arc;

use crate::crd::{CronJob, DelayedJob, DelayedJobPhase, DelayedJobStatus};
use chrono::Utc;
use k8s_openapi::NamespaceResourceScope;
use k8s_openapi::api::batch::v1::CronJob as K8sCronJob;
use k8s_openapi::api::core::v1::{Event, EventSeries};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta, Time};
use kube::ResourceExt;
use kube::api::{DeleteParams, ListParams, PostParams};
use kube::core::Resource as KubeResource;
use kube::core::object::HasStatus;
use kube::{Api, Client, Error as KubeError};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;

pub struct Context {
    client: Client,
    _namespaces: Arc<dashmap::DashSet<String>>,
}

impl Context {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            _namespaces: Arc::new(dashmap::DashSet::new()),
        }
    }

    pub async fn get<K>(&self, namespace: &str, name: &str) -> Result<K, crate::Error>
    where
        K: KubeResource<Scope = NamespaceResourceScope>,
        K: KubeResource,
        K: Clone + DeserializeOwned + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let api = Api::<K>::namespaced(self.client.clone(), namespace);
        match api.get(name).await {
            Ok(object) => Ok(object),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list<K>(&self, namespace: &str) -> Result<Vec<K>, crate::Error>
    where
        K: KubeResource<Scope = NamespaceResourceScope>,
        K: KubeResource,
        K: Clone + DeserializeOwned + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let api = Api::<K>::namespaced(self.client.clone(), namespace);
        match api.list(&ListParams::default()).await {
            Ok(object) => Ok(object.items),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create<K>(&self, namespace: &str, object: &K) -> Result<K, crate::Error>
    where
        K: KubeResource<Scope = NamespaceResourceScope>,
        K: KubeResource,
        K: Clone + DeserializeOwned + Serialize + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let api = Api::<K>::namespaced(self.client.clone(), namespace);
        match api.create(&PostParams::default(), object).await {
            Ok(object) => Ok(object),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn delete<K>(&self, namespace: &str, name: &str) -> Result<(), crate::Error>
    where
        K: KubeResource<Scope = NamespaceResourceScope>,
        K: KubeResource,
        K: Clone + DeserializeOwned + Serialize + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let api = Api::<K>::namespaced(self.client.clone(), namespace);
        if let Err(e) = api.delete(name, &DeleteParams::foreground()).await {
            match e {
                KubeError::Api(e) if e.code == 404 => return Ok(()),
                _ => return Err(crate::Error::Kube(e)),
            }
        }
        Ok(())
    }

    pub async fn delete_background<K>(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(), crate::Error>
    where
        K: KubeResource<Scope = NamespaceResourceScope>,
        K: KubeResource,
        K: Clone + DeserializeOwned + Serialize + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let api = Api::<K>::namespaced(self.client.clone(), namespace);
        if let Err(e) = api.delete(name, &DeleteParams::background()).await {
            match e {
                KubeError::Api(e) if e.code == 404 => return Ok(()),
                _ => return Err(crate::Error::Kube(e)),
            }
        }
        Ok(())
    }

    pub async fn create_cronjob(
        &self,
        namespace: &str,
        object: &K8sCronJob,
    ) -> Result<K8sCronJob, crate::Error> {
        self.create(namespace, object).await
    }

    // pub async fn update_scheduled_cronjob(
    //     &self,
    //     resource: &CronJob,
    //     status: CronJobPhase,
    //     event_type: &str,
    //     message: &str,
    // ) -> Result<(), crate::Error> {
    //     tracing::info!(
    //         name = resource.name_any(),
    //         namespace = resource.namespace().unwrap_or_default(),
    //         status = status.as_str(),
    //         message = message,
    //         "Updating status for scheduled cronjob",
    //     );
    //     self.create_scheduled_cronjob_event(resource, event_type, status.as_str(), message)
    //         .await?;
    //     self.update_scheduled_cronjob_status(resource, status, message)
    //         .await?;
    //     Ok(())
    // }

    pub async fn update_status<K>(&self, resource: &K) -> Result<(), crate::Error>
    where
        K: KubeResource<Scope = NamespaceResourceScope>,
        K: KubeResource,
        K: Clone + DeserializeOwned + Serialize + std::fmt::Debug,
        K::DynamicType: Default,
    {
        let namespace = resource.meta().namespace.as_deref().unwrap();
        let name = resource.meta().name.as_deref().unwrap_or_default();
        let api = Api::<CronJob>::namespaced(self.client.clone(), &namespace);
        let data = serde_json::to_vec(resource)?;
        api.replace_status(name, &PostParams::default(), data)
            .await?;
        Ok(())
    }

    // pub async fn update_scheduled_cronjob_status(
    //     &self,
    //     resource: &CronJob,
    //     phase: CronJobPhase,
    //     message: &str,
    // ) -> Result<(), crate::Error> {
    //     let namespace = resource.namespace().unwrap_or_default();
    //     let name = resource.name_any();
    //     let api = Api::<CronJob>::namespaced(self.client.clone(), &namespace);

    //     let mut resource = match api.get(&name).await {
    //         Ok(resource) => resource,
    //         Err(KubeError::Api(e)) if e.code == 404 => return Ok(()),
    //         Err(e) => return Err(crate::Error::Kube(e)),
    //     };

    //     let status = resource
    //         .status
    //         .get_or_insert_default()
    //         .update_last_schedule_time(Utc::now());

    //     status.phase = phase;
    //     status.message = message.to_string();

    //     assert_eq!(resource.status().unwrap().phase, phase);
    //     assert_eq!(resource.status().unwrap().message, message.to_string());

    //     let bytes = serde_json::to_vec(&resource)?;
    //     api.replace_status(&name, &PostParams::default(), bytes)
    //         .await?;
    //     Ok(())
    // }

    pub async fn create_scheduled_cronjob_event(
        &self,
        resource: &CronJob,
        event_type: &str,
        reason: &str,
        message: &str,
    ) -> Result<(), crate::Error> {
        let namespace = resource.namespace().unwrap_or_default();
        let name = resource.name_any();
        let api = Api::<Event>::namespaced(self.client.clone(), &namespace);
        let now = Utc::now();

        let api_version = CronJob::api_version(&());

        assert_eq!(api_version, "scheduled.divinerapier.io/v1alpha1");

        tracing::debug!(name, namespace, "before create event");

        let event = Event {
            metadata: ObjectMeta {
                name: Some(format!("{}-{}", name, now.timestamp())),
                namespace: Some(namespace.clone()),
                ..Default::default()
            },
            action: Some("Reconciling".to_string()),
            count: Some(1),
            event_time: Some(MicroTime(now)),
            first_timestamp: Some(Time(now)),
            involved_object: k8s_openapi::api::core::v1::ObjectReference {
                kind: Some("CronJob".to_string()),
                namespace: Some(namespace.clone()),
                name: Some(name.clone()),
                api_version: Some(api_version.to_string()),
                uid: resource.metadata.uid.clone(),
                ..Default::default()
            },
            last_timestamp: Some(Time(now)),
            message: Some(message.to_string()),
            reason: Some(reason.to_string()),
            reporting_component: Some("scheduled-cronjob".to_string()),
            reporting_instance: Some("scheduled-controller".to_string()),
            type_: Some(event_type.to_string()),
            series: Some(EventSeries {
                count: Some(1),
                last_observed_time: Some(MicroTime(now)),
                ..Default::default()
            }),
            source: Some(k8s_openapi::api::core::v1::EventSource {
                component: Some("scheduled-cronjob".to_string()),
                ..Default::default()
            }),
            related: None,
        };

        tracing::debug!(name, namespace, "before create event");

        match api.create(&PostParams::default(), &event).await {
            Ok(_) => Ok(()),
            Err(KubeError::Api(e)) if e.code == 409 => Ok(()),
            Err(e) => Err(crate::Error::Kube(e)),
        }
    }

    pub async fn update_delayed_job(
        &self,
        resource: &DelayedJob,
        status: DelayedJobPhase,
        event_type: &str,
        message: &str,
    ) -> Result<(), crate::Error> {
        tracing::info!(
            name = resource.name_any(),
            namespace = resource.namespace().unwrap_or_default(),
            status = status.as_str(),
            message = message,
            "Updating status for scheduled cronjob",
        );
        let e = self
            .create_delayed_job_event(resource, event_type, status.as_str(), message)
            .await;
        println!("create_delayed_job_event: {:?}", e);
        e?;
        let e = self
            .update_delayed_job_status(resource, status, message)
            .await;
        println!("update_delayed_job_status: {:?}", e);
        e?;
        Ok(())
    }

    pub async fn update_delayed_job_status(
        &self,
        resource: &DelayedJob,
        phase: DelayedJobPhase,
        message: &str,
    ) -> Result<(), crate::Error> {
        let namespace = resource.namespace().unwrap_or_default();
        let name = resource.name_any();
        let api = Api::<DelayedJob>::namespaced(self.client.clone(), &namespace);

        let mut resource = match api.get(&name).await {
            Ok(resource) => resource,
            Err(KubeError::Api(e)) if e.code == 404 => return Ok(()),
            Err(e) => return Err(crate::Error::Kube(e)),
        };
        resource.status = Some(DelayedJobStatus {
            phase,
            message: Some(message.to_string()),
            last_update_time: Some(Time(Utc::now())),
        });

        assert_eq!(resource.status().unwrap().phase, phase);
        assert_eq!(
            resource.status().unwrap().message,
            Some(message.to_string())
        );

        let bytes = serde_json::to_vec(&resource)?;
        let e = api
            .replace_status(&name, &PostParams::default(), bytes)
            .await;
        println!("replace_status: {:?}", e);
        e?;
        Ok(())
    }

    pub async fn create_delayed_job_event(
        &self,
        resource: &DelayedJob,
        event_type: &str,
        reason: &str,
        message: &str,
    ) -> Result<(), crate::Error> {
        let namespace = resource.namespace().unwrap_or_default();
        let name = resource.name_any();
        let api = Api::<Event>::namespaced(self.client.clone(), &namespace);
        let now = Utc::now();

        let api_version = DelayedJob::api_version(&());

        assert_eq!(api_version, "batch.divinerapier.io/v1alpha1");

        let event = Event {
            metadata: ObjectMeta {
                name: Some(format!("{}-{}", name, now.timestamp())),
                namespace: Some(namespace.clone()),
                ..Default::default()
            },
            action: Some("Reconciling".to_string()),
            count: Some(1),
            event_time: Some(MicroTime(now)),
            first_timestamp: Some(Time(now)),
            involved_object: k8s_openapi::api::core::v1::ObjectReference {
                kind: Some("DelayedJob".to_string()),
                namespace: Some(namespace),
                name: Some(name),
                api_version: Some(api_version.to_string()),
                uid: resource.metadata.uid.clone(),
                ..Default::default()
            },
            last_timestamp: Some(Time(now)),
            message: Some(message.to_string()),
            reason: Some(reason.to_string()),
            reporting_component: Some("delayed-job".to_string()),
            reporting_instance: Some("scheduled-controller".to_string()),
            type_: Some(event_type.to_string()),
            series: Some(EventSeries {
                count: Some(1),
                last_observed_time: Some(MicroTime(now)),
                ..Default::default()
            }),
            source: Some(k8s_openapi::api::core::v1::EventSource {
                component: Some("delayed-job".to_string()),
                ..Default::default()
            }),
            related: None,
        };

        match api.create(&PostParams::default(), &event).await {
            Ok(_) => Ok(()),
            Err(KubeError::Api(e)) if e.code == 409 => Ok(()),
            Err(e) => Err(crate::Error::Kube(e)),
        }
    }
}

impl Deref for Context {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
