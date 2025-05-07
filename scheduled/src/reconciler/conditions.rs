use std::fmt::Display;

use k8s_openapi::api::batch::v1::Job;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobFinishedType {
    Complete,
    Failed,
}

impl Display for JobFinishedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

pub fn get_job_finished_type(job: &Job) -> Option<JobFinishedType> {
    let status = job.status.as_ref()?;
    let conditions = status.conditions.as_ref()?;

    for condition in conditions.iter() {
        let type_ = &condition.type_;
        if type_ == "Complete" && condition.status == "True" {
            return Some(JobFinishedType::Complete);
        }
        if type_ == "Failed" && condition.status == "True" {
            return Some(JobFinishedType::Failed);
        }
    }

    None
}

pub fn is_job_finished(job: &Job) -> bool {
    get_job_finished_type(job).is_some()
}
