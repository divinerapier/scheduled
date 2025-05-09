# scheduled

[EN](./README_EN.md)

`scheduled` 是一个基于 Rust 的 Kubernetes CronJob/定时任务调度控制器，支持自定义 CRD，实现灵活的定时任务编排和管理。


## 特性

- **自定义 CRD**：支持多种调度类型（Cron、Interval、Daily、Weekly、Monthly）。
- **Kubernetes 原生集成**：基于 [kube-rs](https://github.com/kube-rs/kube) 实现，支持 Job 生命周期管理。
- **并发策略**：支持 Forbid、Allow 等并发控制。
- **高可扩展性**：可扩展的调度策略和任务模板。
- **丰富的日志与错误处理**：集成 tracing、thiserror 等库。

## 目录结构

```
scheduled/
├── src/
│   ├── crd/           # CRD 相关定义（spec、cron、delayed、time）
│   ├── reconciler/    # 控制器逻辑（cronjob、delayed job、上下文、错误处理等）
│   ├── error.rs       # 通用错误类型
│   ├── rbac.rs        # RBAC 相关
│   └── lib.rs         # 库入口
├── Cargo.toml         # Rust 包描述与依赖
```

## 主要模块

- **crd/**  

  - `spec.rs`：CRD 规范定义，支持多种调度类型。
  - `cron.rs`、`delayed.rs`、`time.rs`：具体调度实现与时间处理。

- **reconciler/**  

  - `cron.rs`：CronJob 控制器核心逻辑。
  - `delayed.rs`：延迟任务控制器。
  - `context.rs`：控制器上下文与事件处理。
  - `conditions.rs`：Job 状态判断。
  - `error_policy.rs`：错误处理策略。

## 依赖

- [kube](https://crates.io/crates/kube)
- [k8s-openapi](https://crates.io/crates/k8s-openapi)
- [tokio](https://crates.io/crates/tokio)
- [serde](https://crates.io/crates/serde)
- [tracing](https://crates.io/crates/tracing)
- [cron](https://crates.io/crates/cron)
- 及其他常用 Rust 库

## 用法

1. **构建项目**

   ```sh
   cargo build
   ```

2. **部署 CRD 到 Kubernetes**

* 首次部署

    ``` sh
    cargo run --package crd | kubectl create -f -
    ```

* 更新

    ``` sh
    cargo run --package crd | kubectl replace -f -
    ```

3. **运行控制器**

   ```sh
   cargo run --package controller
   ```

4. **编写 CronJob CR 实例**

   - 参考 `spec.schedule` 字段支持的多种调度类型。

## 贡献

欢迎 issue 和 PR！

## License

MIT
