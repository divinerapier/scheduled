ARG BASE_IMAGE=rust:1.86.0-bookworm

# 第一阶段：规划器
FROM ${BASE_IMAGE} AS planner

RUN cargo install cargo-chef

WORKDIR /app

# 复制 Cargo 文件
COPY Cargo.toml Cargo.lock ./
COPY bin/controller/Cargo.toml /app/bin/controller/
COPY scheduled/Cargo.toml /app/scheduled/
COPY example/create-delayed-job/Cargo.toml /app/example/create-delayed-job/
COPY example/create-cron-job/Cargo.toml /app/example/create-cron-job/

RUN cargo chef prepare --recipe-path recipe.json

# 第二阶段：缓存
FROM ${BASE_IMAGE} AS cacher

RUN cargo install cargo-chef

WORKDIR /app

COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json

# 第三阶段：构建器
FROM ${BASE_IMAGE} AS builder

WORKDIR /app

COPY --from=cacher /app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo

COPY . .

RUN cargo build --release

# 第四阶段：最终镜像
FROM ${BASE_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/controller /usr/local/bin/controller

# 暴露端口
EXPOSE 3000

# Set timezone to Asia/Shanghai
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 启动命令
CMD ["/usr/local/bin/controller"]
