use kube::core::crd::v1::CustomResourceExt as _;

fn main() {
    let crds = vec![
        // scheduled::ScheduledCronJob::crd(),
        scheduled::DelayedJob::crd(),
    ];
    for crd in crds {
        println!("{}", serde_yaml::to_string(&crd).unwrap());
        println!("---");
    }
}
