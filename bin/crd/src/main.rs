use kube::core::crd::v1::CustomResourceExt as _;

fn main() {
    // let crds = vec![
    //     scheduled_cronjob::ScheduledCronJob::crd(),
    //     scheduled_cronjob::DelayedJob::crd(),
    // ];
    let crd = scheduled_cronjob::ScheduledCronJob::crd();
    println!("{}", serde_yaml::to_string(&crd).unwrap());
    // for crd in crds {
    //     println!("{}", serde_yaml::to_string(&crd).unwrap());
    // }
}
