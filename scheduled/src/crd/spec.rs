use chrono::{DateTime, Datelike, Days, Local, Months};
use itertools::Itertools;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::CELSchema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};

/// The rule of the schedule.
#[derive(Debug, Serialize, Deserialize, Clone, CELSchema, derive_builder::Builder)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRule {
    /// Specifies how to treat concurrent executions of a Job. Valid values are:
    ///
    /// - "Allow" (default): allows CronJobs to run concurrently;
    /// - "Forbid": forbids concurrent runs, skipping next run if previous run hasn't finished yet;
    /// - "Replace": cancels currently running job and replaces it with a new one
    #[builder(default = ConcurrencyPolicy::Forbid)]
    pub concurrency_policy: ConcurrencyPolicy,

    /// Optional end time for the schedule
    #[builder(default = None)]
    pub end_time: Option<Time>,

    /// Maximum number of executions allowed
    #[builder(default = None)]
    pub max_executions: Option<u32>,

    /// Maximum number of retries allowed for each job
    #[builder(default = None)]
    pub max_retries: Option<u32>,

    /// List of schedule types
    pub schedule: Schedule,

    /// Optional start time for the schedule
    #[builder(default = None)]
    pub start_time: Option<Time>,

    /// Optional deadline in seconds for starting the job
    #[builder(default = None)]
    pub starting_deadline_seconds: Option<i64>,
}

impl ScheduleRule {
    pub fn builder() -> ScheduleRuleBuilder {
        ScheduleRuleBuilder::default()
    }

    pub(crate) fn is_after_end_time(&self, t: DateTime<Local>) -> bool {
        if let Some(ref end_time) = self.end_time {
            return t >= end_time.0.with_timezone(&Local);
        }
        false
    }

    pub fn next(
        &self,
        executions_count: u32,
        now: DateTime<Local>,                        // now
        last_schedule_time: Option<DateTime<Local>>, // now -20min
    ) -> Option<DateTime<Local>> {
        // 检查执行次数是否达到最大
        if let Some(max_executions) = self.max_executions {
            if executions_count >= max_executions {
                return None;
            }
        }

        // 检查开始时间
        let last_time = match (last_schedule_time, &self.start_time) {
            // now - 20min, now - 1h
            (Some(last_schedule_time), Some(start_time)) => {
                assert!(last_schedule_time >= start_time.0.with_timezone(&Local));
                last_schedule_time
            }
            (Some(last_schedule_time), None) => last_schedule_time,
            (None, Some(start_time)) => {
                // 首次调度，从 start_time 开始计算下一个调度时间
                let start_time_local = start_time.0.with_timezone(&Local);
                // 如果 start_time 晚于当前时间，直接返回 start_time
                if start_time_local > now {
                    return Some(start_time_local);
                }
                // 否则从 start_time 开始计算下一个调度时间
                start_time_local
            }
            (_, _) => now,
        };

        // 如果有结束时间且当前时间大于结束时间，则返回 None 表示调度结束
        if let Some(ref end_time) = self.end_time {
            if now >= end_time.0.with_timezone(&Local) {
                return None;
            }
        }

        // 基于上次调度时间(包括上次成功调度时间，配置的开始时间，当前时间等多种情况)
        let next_time = self.schedule.next(Some(last_time))?;

        tracing::info!(
            last_time = ?last_time,
            next_time = ?next_time,
            "next time"
        );

        if let Some(ref end_time) = self.end_time {
            if next_time >= end_time.0.with_timezone(&Local) {
                return None;
            }
        }

        Some(next_time)
    }
}

/// The interval schedule.
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Interval {
    /// The interval in seconds.
    pub seconds: u32,
}

impl From<std::time::Duration> for Interval {
    fn from(duration: std::time::Duration) -> Self {
        Interval {
            seconds: duration.as_secs() as u32,
        }
    }
}

impl Interval {
    pub fn next(&self, previous_run_time: Option<DateTime<Local>>) -> Option<DateTime<Local>> {
        let now = Local::now();
        let previous = previous_run_time.unwrap_or(now);

        // 如果 previous 早于当前时间，从当前时间开始计算
        let base_time = if previous < now { now } else { previous };

        Some(base_time + Duration::from_secs(self.seconds as u64))
    }
}

impl From<Interval> for ScheduleType {
    fn from(interval: Interval) -> Self {
        ScheduleType::Interval(interval)
    }
}

/// The time point of the schedule.
#[derive(Debug, Serialize, Deserialize, Clone, CELSchema)]
#[serde(rename_all = "camelCase")]
#[cel_validate(rule = Rule::new("self.hour >= 0 && self.hour <= 23").message("Invalid hour").reason(Reason::FieldValueInvalid))]
#[cel_validate(rule = Rule::new("self.minute >= 0 && self.minute <= 59").message("Invalid minute").reason(Reason::FieldValueInvalid))]
pub struct TimePoint {
    /// The hour of the time point. The value is between 0 and 23.
    pub hour: u8,

    /// The minute of the time point. The value is between 0 and 59.
    pub minute: u8,
}

/// The daily schedule.
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DailySchedule {
    /// The time points of the schedule.
    pub time_points: Vec<TimePoint>,
}

impl DailySchedule {
    pub fn next(&self, previous_run_time: Option<DateTime<Local>>) -> Option<DateTime<Local>> {
        let now = Local::now();
        println!("now: {:?}", now);
        let previous = previous_run_time.unwrap_or(now);
        println!("previous: {:?}", previous);

        // 如果 previous 早于当前时间，从当前时间开始计算
        let base_time = if previous < now { now } else { previous };
        println!("base_time: {:?}", base_time);

        let time_points = self.time_points.iter().map(|tp| {
            base_time
                .clone()
                .date_naive()
                .and_hms_opt(tp.hour as u32, tp.minute as u32, 0)
                .unwrap()
                .and_local_timezone(Local)
                .unwrap()
        });

        time_points
            .clone()
            .into_iter()
            .map(|dt| dt + Duration::from_secs(24 * 60 * 60))
            .chain(time_points)
            .filter(|dt| dt > &base_time) // 改为严格大于，避免返回相同时间
            .min()
    }
}

impl From<DailySchedule> for ScheduleType {
    fn from(daily: DailySchedule) -> Self {
        ScheduleType::Daily(daily)
    }
}

/// The weekly schedule.
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WeeklySchedule {
    /// The day of the week.
    pub day: DayOfWeek,

    /// The time points of the schedule.
    pub time_points: Vec<TimePoint>,
}

impl WeeklySchedule {
    pub fn next(&self, previous_run_time: Option<DateTime<Local>>) -> Option<DateTime<Local>> {
        let now = Local::now();
        let previous = previous_run_time.unwrap_or(now);

        // 如果 previous 早于当前时间，从当前时间开始计算
        let base_time = if previous < now { now } else { previous };
        let base_weekday = base_time.weekday();
        let target_weekday = chrono::Weekday::from(self.day);

        let days_until_next = (target_weekday.num_days_from_monday() as i64
            - base_weekday.num_days_from_monday() as i64
            + 7)
            % 7;

        let first_date = base_time.date_naive() + Days::new(days_until_next as u64);
        let next_date = first_date + Days::new(7);

        [first_date, next_date]
            .iter()
            .cartesian_product(self.time_points.clone().into_iter())
            .map(|(datetime, timepoint)| {
                datetime
                    .and_hms_opt(timepoint.hour as u32, timepoint.minute as u32, 0)
                    .unwrap()
                    .and_local_timezone(Local)
                    .unwrap()
            })
            .filter(|dt| dt > &base_time)
            .min()
    }
}

impl From<WeeklySchedule> for ScheduleType {
    fn from(weekly: WeeklySchedule) -> Self {
        ScheduleType::Weekly(weekly)
    }
}

/// The monthly schedule.   
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonthlySchedule {
    /// The day of the month. If the day is negative, it means the last day of the month.
    pub day: u32,

    /// The time points of the schedule.
    pub time_points: Vec<TimePoint>,
}

impl MonthlySchedule {
    pub fn next(&self, previous_run_time: Option<DateTime<Local>>) -> Option<DateTime<Local>> {
        let now = Local::now();
        let previous = previous_run_time.unwrap_or(now);

        // 如果 previous 早于当前时间，从当前时间开始计算
        let base_time = if previous < now { now } else { previous };
        let target_day = self.day;

        let first_date = base_time.date_naive().with_day(target_day).unwrap();
        let next_date = first_date + Months::new(1);

        [first_date, next_date]
            .iter()
            .cartesian_product(self.time_points.clone().into_iter())
            .map(|(datetime, timepoint)| {
                datetime
                    .and_hms_opt(timepoint.hour as u32, timepoint.minute as u32, 0)
                    .unwrap()
                    .and_local_timezone(Local)
                    .unwrap()
            })
            .filter(|dt| dt > &base_time)
            .min()
    }
}

impl From<MonthlySchedule> for ScheduleType {
    fn from(monthly: MonthlySchedule) -> Self {
        ScheduleType::Monthly(monthly)
    }
}

/// The type of the schedule.
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ScheduleType {
    /// The standard Cron expression.
    Cron(String),

    /// The interval schedule.
    Interval(Interval),

    /// The daily schedule.
    Daily(DailySchedule),

    /// The weekly schedule.
    Weekly(WeeklySchedule),

    /// The monthly schedule.
    Monthly(MonthlySchedule),
}

impl ScheduleType {
    pub fn next(&self, previous_run_time: Option<DateTime<Local>>) -> Option<DateTime<Local>> {
        match self {
            ScheduleType::Cron(cron_expr) => {
                let schedule = cron::Schedule::from_str(&cron_expr);
                match schedule {
                    Ok(schedule) => schedule.upcoming(Local).next(),
                    Err(_) => None,
                }
            }
            ScheduleType::Interval(interval) => interval.next(previous_run_time),
            ScheduleType::Daily(daily) => daily.next(previous_run_time),
            ScheduleType::Weekly(weekly) => weekly.next(previous_run_time),
            ScheduleType::Monthly(monthly) => monthly.next(previous_run_time),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema)]
#[serde(transparent)]
pub struct Schedule(Vec<ScheduleType>);

impl From<Vec<ScheduleType>> for Schedule {
    fn from(schedule: Vec<ScheduleType>) -> Self {
        Self(schedule)
    }
}

impl FromIterator<ScheduleType> for Schedule {
    fn from_iter<T: IntoIterator<Item = ScheduleType>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl Schedule {
    pub fn next(&self, last_schedule_time: Option<DateTime<Local>>) -> Option<DateTime<Local>> {
        self.0
            .iter()
            .map(|schedule| schedule.next(last_schedule_time))
            .min()
            .flatten()
    }

    pub fn first_schedule_type(&self) -> Option<&ScheduleType> {
        self.0.first()
    }
}

/// The day of the week.
#[derive(Copy, Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DayOfWeek {
    #[default]
    #[serde(rename = "Monday")]
    Monday,
    #[serde(rename = "Tuesday")]
    Tuesday,
    #[serde(rename = "Wednesday")]
    Wednesday,
    #[serde(rename = "Thursday")]
    Thursday,
    #[serde(rename = "Friday")]
    Friday,
    #[serde(rename = "Saturday")]
    Saturday,
    #[serde(rename = "Sunday")]
    Sunday,
}

impl From<DayOfWeek> for chrono::Weekday {
    fn from(day: DayOfWeek) -> Self {
        match day {
            DayOfWeek::Monday => chrono::Weekday::Mon,
            DayOfWeek::Tuesday => chrono::Weekday::Tue,
            DayOfWeek::Wednesday => chrono::Weekday::Wed,
            DayOfWeek::Thursday => chrono::Weekday::Thu,
            DayOfWeek::Friday => chrono::Weekday::Fri,
            DayOfWeek::Saturday => chrono::Weekday::Sat,
            DayOfWeek::Sunday => chrono::Weekday::Sun,
        }
    }
}

/// The concurrency policy of the schedule.
#[derive(Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConcurrencyPolicy {
    #[default]
    #[serde(rename = "Forbid")]
    /// If the previous job is not finished, skip the current scheduling.
    Forbid,

    /// Allow concurrent execution of multiple tasks.
    #[serde(rename = "Allow")]
    Allow,

    /// Cancel the currently running task and execute the new task.
    #[serde(rename = "Replace")]
    Replace,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Local, TimeZone};

    struct TestCase {
        index: u32,
        input: DateTime<Local>,
        expected: DateTime<Local>,
    }

    impl TestCase {
        fn new(index: u32, input: DateTime<Local>, expected: DateTime<Local>) -> Self {
            Self {
                index,
                input,
                expected,
            }
        }
    }

    #[test]
    fn test_interval_next() {
        let interval = Interval { seconds: 120 }; // 1小时

        let test_cases = vec![
            TestCase::new(
                1,
                Local.with_ymd_and_hms(2024, 3, 10, 10, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 10, 2, 0).unwrap(),
            ),
            TestCase::new(
                2,
                Local.with_ymd_and_hms(2024, 3, 10, 10, 1, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 10, 3, 59).unwrap(),
            ),
            TestCase::new(
                3,
                Local.with_ymd_and_hms(2024, 3, 10, 23, 59, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 11, 0, 1, 1).unwrap(),
            ),
        ];

        for test_case in test_cases {
            let next = interval.next(Some(test_case.input)).unwrap();
            assert_eq!(next, test_case.expected, "case #{}", test_case.index);
        }
    }

    #[test]
    fn test_daily_next() {
        let daily = DailySchedule {
            time_points: vec![
                TimePoint {
                    hour: 10,
                    minute: 0,
                },
                TimePoint {
                    hour: 14,
                    minute: 0,
                },
                TimePoint {
                    hour: 18,
                    minute: 0,
                },
            ],
        };

        let test_cases = vec![
            TestCase::new(
                1,
                Local.with_ymd_and_hms(2024, 3, 10, 9, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                2,
                Local.with_ymd_and_hms(2024, 3, 10, 10, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                3,
                Local.with_ymd_and_hms(2024, 3, 10, 10, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                4,
                Local.with_ymd_and_hms(2024, 3, 10, 13, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                5,
                Local.with_ymd_and_hms(2024, 3, 10, 14, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                6,
                Local.with_ymd_and_hms(2024, 3, 10, 14, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                7,
                Local.with_ymd_and_hms(2024, 3, 10, 17, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                8,
                Local.with_ymd_and_hms(2024, 3, 10, 18, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 10, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                9,
                Local.with_ymd_and_hms(2024, 3, 10, 18, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 11, 10, 0, 0).unwrap(),
            ),
        ];

        for test_case in test_cases {
            let next = daily.next(Some(test_case.input)).unwrap();
            assert_eq!(next, test_case.expected, "case #{}", test_case.index);
        }
    }

    #[test]
    fn test_weekly_next() {
        let weekly = WeeklySchedule {
            day: DayOfWeek::Monday,
            time_points: vec![
                TimePoint {
                    hour: 10,
                    minute: 0,
                },
                TimePoint {
                    hour: 14,
                    minute: 0,
                },
                TimePoint {
                    hour: 18,
                    minute: 0,
                },
            ],
        };

        let test_cases = vec![
            TestCase::new(
                1,
                Local.with_ymd_and_hms(2025, 4, 7, 9, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                2,
                Local.with_ymd_and_hms(2025, 4, 7, 10, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                3,
                Local.with_ymd_and_hms(2025, 4, 7, 10, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                4,
                Local.with_ymd_and_hms(2025, 4, 7, 13, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                5,
                Local.with_ymd_and_hms(2025, 4, 7, 14, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                6,
                Local.with_ymd_and_hms(2025, 4, 7, 14, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                7,
                Local.with_ymd_and_hms(2025, 4, 7, 17, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                8,
                Local.with_ymd_and_hms(2025, 4, 7, 18, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 7, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                9,
                Local.with_ymd_and_hms(2025, 4, 7, 18, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2025, 4, 14, 10, 0, 0).unwrap(),
            ),
        ];

        for test_case in test_cases {
            let next = weekly.next(Some(test_case.input)).unwrap();
            assert_eq!(next, test_case.expected, "case #{}", test_case.index);
        }
    }

    #[test]
    fn test_monthly_next() {
        let monthly = MonthlySchedule {
            day: 15,
            time_points: vec![
                TimePoint {
                    hour: 10,
                    minute: 0,
                },
                TimePoint {
                    hour: 14,
                    minute: 0,
                },
                TimePoint {
                    hour: 18,
                    minute: 0,
                },
            ],
        };

        let test_cases = vec![
            TestCase::new(
                1,
                Local.with_ymd_and_hms(2024, 3, 15, 9, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                2,
                Local.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 15, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                3,
                Local.with_ymd_and_hms(2024, 3, 15, 10, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 15, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                4,
                Local.with_ymd_and_hms(2024, 3, 15, 13, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 15, 14, 0, 0).unwrap(),
            ),
            TestCase::new(
                5,
                Local.with_ymd_and_hms(2024, 3, 15, 14, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 15, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                6,
                Local.with_ymd_and_hms(2024, 3, 15, 17, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 3, 15, 18, 0, 0).unwrap(),
            ),
            TestCase::new(
                7,
                Local.with_ymd_and_hms(2024, 3, 15, 18, 0, 1).unwrap(),
                Local.with_ymd_and_hms(2024, 4, 15, 10, 0, 0).unwrap(),
            ),
            TestCase::new(
                8,
                Local.with_ymd_and_hms(2024, 3, 15, 23, 59, 59).unwrap(),
                Local.with_ymd_and_hms(2024, 4, 15, 10, 0, 0).unwrap(),
            ),
        ];

        for test_case in test_cases {
            let next = monthly.next(Some(test_case.input)).unwrap();
            assert_eq!(next, test_case.expected, "case #{}", test_case.index);
        }
    }

    #[test]
    #[should_panic]
    fn test_schedule_rule_time_constraints_should_panic() {
        let now = Local.with_ymd_and_hms(3026, 4, 16, 17, 44, 0).unwrap();

        let start_time = Time(
            Local
                .with_ymd_and_hms(3026, 4, 16, 17, 44, 0)
                .unwrap()
                .to_utc(),
        );
        let end_time = Time(
            Local
                .with_ymd_and_hms(3026, 4, 16, 18, 44, 0)
                .unwrap()
                .to_utc(),
        );

        let rule = ScheduleRule::builder()
            .start_time(Some(start_time.clone()))
            .end_time(Some(end_time.clone()))
            .schedule(Schedule(vec![ScheduleType::Interval(Interval {
                seconds: 60,
            })]))
            .build()
            .unwrap();

        // Test case 1: Current time before start time
        let current_time = Local.with_ymd_and_hms(3026, 4, 16, 17, 43, 0).unwrap();
        let next_time = rule.next(0, now, Some(current_time));
        assert_eq!(next_time, Some(start_time.0.with_timezone(&Local)));
    }

    #[test]
    fn test_schedule_rule_time_constraints() {
        let now = Local.with_ymd_and_hms(3026, 4, 16, 17, 44, 0).unwrap();

        let start_time = Time(
            Local
                .with_ymd_and_hms(3026, 4, 16, 17, 44, 0)
                .unwrap()
                .to_utc(),
        );
        let end_time = Time(
            Local
                .with_ymd_and_hms(3026, 4, 16, 18, 44, 0)
                .unwrap()
                .to_utc(),
        );

        let rule = ScheduleRule::builder()
            .start_time(Some(start_time.clone()))
            .end_time(Some(end_time.clone()))
            .schedule(Schedule(vec![ScheduleType::Interval(Interval {
                seconds: 60,
            })]))
            .build()
            .unwrap();

        // Test case 2: Current time after end time
        let current_time = Local.with_ymd_and_hms(3026, 4, 16, 18, 45, 0).unwrap();
        let next_time = rule.next(0, now, Some(current_time));
        assert_eq!(next_time, None);

        // Test case 3: Current time between start and end time
        let current_time = Local.with_ymd_and_hms(3026, 4, 16, 17, 45, 0).unwrap();
        let next_time = rule.next(0, now, Some(current_time));
        assert_eq!(
            next_time,
            Some(Local.with_ymd_and_hms(3026, 4, 16, 17, 46, 0).unwrap())
        );
    }
}
