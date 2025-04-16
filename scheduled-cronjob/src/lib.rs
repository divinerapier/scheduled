#![feature(let_chains)]

mod context;
pub mod crd;
pub mod error;
pub mod rbac;
pub mod reconciler;

pub use context::Context;
pub use crd::{CronJobBuilder, ScheduledCronJob, ScheduledCronJobSpec, ScheduledCronJobStatus};
pub use error::Error;
pub use rbac::{RbacRule, get_rbac_rules};
pub use reconciler::{error_policy_for_scheduled_cronjob, reconcile_scheduled_cronjob};
