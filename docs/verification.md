# 本地验证记录

日期：2026-10-19（Asia/Shanghai）。环境：Windows x64、Rust 1.99.1、MSVC 14.51.36231、Windows SDK 10.0.26100.0。

| 检查 | 实际结果 |
|---|---|
| `cargo check --offline --all-targets` | 通过 |
| `cargo fmt -- --check` | 通过 |
| `cargo test --offline` | 11 个单元测试、6 个集成测试通过，0 失败 |
| `cargo build --offline` | 通过，debug 可执行文件可启动 |
| OpenAPI JSON / 内部模型引用 / 路由覆盖 | 26 路径、33 operations，无缺失引用 |
| 独立进程 HTTP 冒烟 | 两流程上传、查看、参数定制、部署、执行、状态、验证、报告、结果下载通过 |
| 真实 Slurm / Apptainer / 生信分析 | 本机未执行，待 Linux 集群验收 |
| 100 QPS | 已交付 k6 方案与脚本，未执行目标机压测 |

完整 Cargo 输出见 [cargo-test.txt](../acceptance-output/cargo-test.txt)，HTTP 记录见 [smoke.json](../acceptance-output/smoke.json)。本机网络沙箱禁止回环连接，涉及本机 HTTP 的完整测试在允许回环连接的环境中运行。

HTTP 冒烟的两个模拟运行：

| 流程 | Run ID | 状态 | 退出码 | JSON 报告 |
|---|---|---|---|---|
| Germline | ff9ad64f-02d7-4638-85e9-65ea42d47f34 | COMPLETED | 0:0 | passed=true，simulated=true |
| Somatic | 65023715-b1c4-419b-ad10-eafa309a2b66 | COMPLETED | 0:0 | passed=true，simulated=true |

测试覆盖：鉴权与 owner 隔离、管理员权限、应用集群绑定、上传预览/下载、两流程闭环、重复执行竞争、不可覆盖模板版本与重启恢复、hold/release/cancel、代理路径/query 与凭据剥离、不跟随重定向、参数注入/资源限制、缺失镜像/参考/输入、损坏或删除产物后的失败报告。

所有结果均为明确标记的模拟 fixture，没有执行 BWA/GATK，也不构成生物学准确性验证。真实验收以架构文档中的验收矩阵为准。
