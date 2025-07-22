# Rust 代码 LLDB 调试指南

## 1. 启动调试

```bash
# 编译调试版本
cargo build

# 启动 LLDB 调试
lldb target/debug/scheduled

# 或者直接调试测试
lldb -- cargo test test_next -- --nocapture
```

## 2. 设置断点

```bash
# 在特定函数设置断点
(lldb) b scheduled::crd::cron::CronJob::next_schedule_time

# 在特定文件行号设置断点
(lldb) b scheduled/src/crd/cron.rs:296

# 在测试函数设置断点
(lldb) b scheduled::crd::cron::tests::test_next

# 条件断点（正确的语法）
(lldb) b scheduled::crd::cron::CronJob::next_schedule_time -c "self.name_any().contains(\"name\")"
```

## 3. 运行和单步调试

```bash
# 运行程序
(lldb) run

# 继续执行
(lldb) continue

# 单步执行（步入函数）
(lldb) step

# 单步执行（不步入函数）
(lldb) next

# 单步执行（步出当前函数）
(lldb) finish
```

## 4. 查看变量

```bash
# 查看所有局部变量
(lldb) frame variable

# 查看特定变量
(lldb) p now
(lldb) p next_time
(lldb) p self.name_any()

# 查看变量类型
(lldb) ptype now
(lldb) ptype self
```

## 5. 查看调用栈

```bash
# 查看当前调用栈
(lldb) bt

# 查看调用栈详细信息
(lldb) bt all
```

## 6. 调试特定测试用例

```bash
# 设置断点在测试函数
(lldb) b scheduled::crd::cron::tests::test_next

# 运行特定测试
(lldb) run -- test_next

# 或者使用条件断点只调试特定测试用例
(lldb) b scheduled::crd::cron::tests::test_next -c "test_case.name.as_str() == \"real_world_cronjob_case\""
```

## 7. 常用调试命令

```bash
# 列出所有断点
(lldb) breakpoint list

# 删除断点
(lldb) breakpoint delete 1

# 禁用断点
(lldb) breakpoint disable 1

# 启用断点
(lldb) breakpoint enable 1

# 查看源代码
(lldb) list

# 查看特定行号的源代码
(lldb) list 296
```

## 8. 调试死循环问题

```bash
# 在循环开始处设置断点
(lldb) b scheduled/src/crd/cron.rs:331

# 在循环内部设置断点
(lldb) b scheduled/src/crd/cron.rs:335

# 查看循环变量
(lldb) p next_time
(lldb) p next_time0
(lldb) p next_time1
(lldb) p missing_count
```

## 9. 条件断点示例

```bash
# 只在特定条件下触发断点
(lldb) b scheduled::crd::cron::CronJob::next_schedule_time_after -c "anchor.is_some()"

# 只在时间大于某个值时触发
(lldb) b scheduled::crd::cron::CronJob::next_schedule_time_after -c "now > chrono::Local::now() - chrono::Duration::hours(1)"
```

## 10. 退出调试

```bash
# 退出 LLDB
(lldb) quit
``` 