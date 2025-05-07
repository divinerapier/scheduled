use chrono::DateTime;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

pub trait IntoTime {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError>;
}

impl IntoTime for Option<&str> {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        match self {
            None => Ok(None),     // 空值返回当前时间
            Some("") => Ok(None), // 空字符串返回当前时间
            Some(s) => {
                // 尝试解析 RFC3339 格式
                match DateTime::parse_from_rfc3339(s) {
                    Ok(dt) => Ok(Some(Time(dt.with_timezone(&chrono::Utc)))),
                    Err(_) => {
                        // 如果 RFC3339 解析失败，尝试其他格式
                        match DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                            Ok(dt) => Ok(Some(Time(dt.with_timezone(&chrono::Utc)))),
                            Err(e) => Err(e),
                        }
                    }
                }
            }
        }
    }
}

impl IntoTime for &str {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        Some(self).into_time()
    }
}

impl IntoTime for String {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        Some(self.as_str()).into_time()
    }
}

impl IntoTime for Option<String> {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        self.as_deref().into_time()
    }
}

impl IntoTime for Time {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        Ok(Some(self))
    }
}

impl IntoTime for Option<Time> {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        match self {
            Some(t) => Ok(Some(t)),
            None => Ok(None),
        }
    }
}

impl IntoTime for i64 {
    fn into_time(self) -> Result<Option<Time>, chrono::ParseError> {
        Ok(Some(Time(
            DateTime::from_timestamp_millis(self)
                .unwrap()
                .with_timezone(&chrono::Utc),
        )))
    }
}
