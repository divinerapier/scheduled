# scheduled

`scheduled` is a Rust-based Kubernetes CronJob/scheduled task controller that supports custom CRDs for flexible scheduling and management of jobs.

## Features

- **Custom CRD**: Supports multiple schedule types (Cron, Interval, Daily, Weekly, Monthly).
- **Kubernetes Native Integration**: Built on [kube-rs](https://github.com/kube-rs/kube), supports full Job lifecycle management.
- **Concurrency Policy**: Supports Forbid, Allow, and other concurrency controls.
- **High Extensibility**: Easily extendable scheduling strategies and job templates.
- **Rich Logging and Error Handling**: Integrated with tracing, thiserror, and more.

## Directory Structure

```
scheduled/
├── src/
│   ├── crd/           # CRD definitions (spec, cron, delayed, time)
│   ├── reconciler/    # Controller logic (cronjob, delayed job, context, error handling, etc.)
│   ├── error.rs       # Common error types
│   ├── rbac.rs        # RBAC related
│   └── lib.rs         # Library entry
├── Cargo.toml         # Rust package description and dependencies
```

## Main Modules

- **crd/**  
  - `spec.rs`: CRD specification, supports multiple schedule types.
  - `cron.rs`, `delayed.rs`, `time.rs`: Scheduling implementations and time handling.

- **reconciler/**  
  - `cron.rs`: Core logic for CronJob controller.
  - `delayed.rs`: Delayed job controller.
  - `context.rs`: Controller context and event handling.
  - `conditions.rs`: Job status checks.
  - `error_policy.rs`: Error handling policy.

## Dependencies

- [kube](https://crates.io/crates/kube)
- [k8s-openapi](https://crates.io/crates/k8s-openapi)
- [tokio](https://crates.io/crates/tokio)
- [serde](https://crates.io/crates/serde)
- [tracing](https://crates.io/crates/tracing)
- [cron](https://crates.io/crates/cron)
- And other common Rust libraries

## Usage

1. **Build the project**

   ```sh
   cargo build
   ```

2. **Deploy CRD to Kubernetes**

   * First time deployment

     ```sh
     cargo run --package crd | kubectl create -f -
     ```

   * Update

     ```sh
     cargo run --package crd | kubectl replace -f -
     ```

3. **Run the controller**

   ```sh
   cargo run --package controller
   ```

4. **Write CronJob CR instances**

   - Refer to the `spec.schedule` field for supported schedule types.

## Contributing

Issues and PRs are welcome!

## License

MIT 