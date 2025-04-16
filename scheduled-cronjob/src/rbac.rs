use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RbacRule {
    pub name: String,
    pub api_groups: Option<Vec<String>>,
    pub resources: Option<Vec<String>>,
    pub verbs: Vec<String>,
}

pub fn get_rbac_rules() -> HashMap<String, RbacRule> {
    let mut rules = HashMap::new();

    // ScheduledCronJob rules
    rules.insert(
        "ScheduledCronJob".to_string(),
        RbacRule {
            name: "ScheduledCronJob".to_string(),
            api_groups: Some(vec!["batch.divinerapier.io".to_string()]),
            resources: Some(vec!["scheduledcronjobs".to_string()]),
            verbs: vec![
                "get".to_string(),
                "list".to_string(),
                "watch".to_string(),
                "create".to_string(),
                "update".to_string(),
                "patch".to_string(),
                "delete".to_string(),
            ],
        },
    );

    // CronJob rules
    rules.insert(
        "CronJob".to_string(),
        RbacRule {
            name: "CronJob".to_string(),
            api_groups: Some(vec!["batch".to_string()]),
            resources: Some(vec!["cronjobs".to_string()]),
            verbs: vec![
                "get".to_string(),
                "list".to_string(),
                "watch".to_string(),
                "create".to_string(),
                "update".to_string(),
                "patch".to_string(),
                "delete".to_string(),
            ],
        },
    );

    // Event rules
    rules.insert(
        "Event".to_string(),
        RbacRule {
            name: "Event".to_string(),
            api_groups: Some(vec!["".to_string()]),
            resources: Some(vec!["events".to_string()]),
            verbs: vec!["create".to_string(), "patch".to_string()],
        },
    );

    rules
}
