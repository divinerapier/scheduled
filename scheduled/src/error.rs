use chrono::{DateTime, Local};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("not found")]
    NotFound,

    #[error("invalid start time")]
    InvalidStartTime,

    #[error("invalid end time")]
    InvalidEndTime,

    #[error("end time is before start time")]
    EndBeforeStart,

    #[error("duration between {0} and {1} must be at least 5 minutes")]
    DurationTooShort(DateTime<Local>, DateTime<Local>),

    #[error("wait for {0}")]
    WaitFor(chrono::Duration),

    #[error("expired at {0}")]
    Expired(chrono::DateTime<chrono::Local>),

    #[error("k8s error: {0}")]
    Kube(#[from] kube::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("invalid concurrency policy")]
    InvalidConcurrencyPolicy,

    #[error("invalid failed jobs history limit")]
    InvalidFailedJobsHistoryLimit,

    #[error("cronjob spec not found")]
    CronjobSpecNotFound,

    #[error("invalid backoff limit")]
    InvalidBackoffLimit,
}
