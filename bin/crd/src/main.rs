use kube::core::crd::v1::CustomResourceExt as _;

fn main() {
    let crd = scheduled_cronjob::ScheduledCronJob::crd();
    println!("{}", serde_yaml::to_string(&crd).unwrap());
}
