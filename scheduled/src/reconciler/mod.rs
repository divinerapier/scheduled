mod context;
mod delayed_job;
mod scheduled_cronjob;

pub use context::Context;
pub use delayed_job::reconcile as reconcile_delayed_job;
use kube::{ResourceExt as _, core::Resource, runtime::controller::Action};
pub use scheduled_cronjob::reconcile as reconcile_scheduled_cronjob;
use serde::de::DeserializeOwned;
use std::{fmt::Debug, hash::Hash, sync::Arc, time::Duration};

use crate::Error;

pub fn error_policy<K>(job: Arc<K>, err: &Error, _ctx: Arc<Context>) -> Action
where
    K: Clone + Resource + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone,
    K::DynamicType: Debug + Unpin,
{
    let name = job.name_any();
    let namespace = job.namespace().unwrap_or_default();
    tracing:: error!(name = name, namespace = namespace, error = ?err, "Error in reconciliation, will retry in 5 seconds");
    Action::requeue(Duration::from_secs(5))
}
