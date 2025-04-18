use std::fs::File;
use std::io::Write;
use std::path::Path;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use scheduled::rbac::get_rbac_rules;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rules = get_rbac_rules();

    // Generate ClusterRole
    let cluster_role = serde_yaml::to_string(&k8s_openapi::api::rbac::v1::ClusterRole {
        metadata: ObjectMeta {
            name: Some("scheduled-controller".to_string()),
            ..Default::default()
        },
        rules: Some(
            rules
                .values()
                .map(|rule| k8s_openapi::api::rbac::v1::PolicyRule {
                    api_groups: rule.api_groups.clone(),
                    resources: rule.resources.clone(),
                    verbs: rule.verbs.clone(),
                    ..Default::default()
                })
                .collect(),
        ),
        ..Default::default()
    })?;

    // Generate ClusterRoleBinding
    let cluster_role_binding =
        serde_yaml::to_string(&k8s_openapi::api::rbac::v1::ClusterRoleBinding {
            metadata: ObjectMeta {
                name: Some("scheduled-controller".to_string()),
                ..Default::default()
            },
            subjects: Some(vec![k8s_openapi::api::rbac::v1::Subject {
                kind: "ServiceAccount".to_string(),
                name: "scheduled-controller".to_string(),
                namespace: Some("default".to_string()),
                ..Default::default()
            }]),
            role_ref: k8s_openapi::api::rbac::v1::RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "ClusterRole".to_string(),
                name: "scheduled-controller".to_string(),
            },
        })?;

    // Write to file
    let path = Path::new("config/rbac");
    std::fs::create_dir_all(path)?;

    let mut file = File::create(path.join("role.yaml"))?;
    file.write_all(cluster_role.as_bytes())?;

    let mut file = File::create(path.join("role_binding.yaml"))?;
    file.write_all(cluster_role_binding.as_bytes())?;

    Ok(())
}
