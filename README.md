# HPC REST Gateway + Workflow Template Service

Rust / Axum / Tokio / SQLite 的独立后端。流程为 YAML 模板，每个流程生成一个顺序执行的 Slurm 作业；没有额外工作流引擎。

## 快速运行（mock，无需 Slurm）

在项目目录执行，需安装 Rust 稳定工具链和系统 C/C++ 链接工具：

```powershell
$env:HPC_API_TOKEN = 'local-development-token-change-me'
cargo run --release
```

当前工作区另备有本地 Rust/MSVC 辅助脚本；本机没有全局 Cargo 时，可从项目目录运行：

```powershell
$env:HPC_API_TOKEN = 'local-development-token-change-me'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/check_env.ps1 run --offline
```

```bash
export HPC_API_TOKEN='local-development-token-change-me'
cargo run --release
```

- Swagger UI：<http://127.0.0.1:8080/swagger-ui/>
- OpenAPI：<http://127.0.0.1:8080/api-docs/openapi.json>
- 在 Swagger 的 Authorize 中输入上述 token。
- `HPC_CONFIG` 指定配置文件；默认 `config.example.json`。
- 数据、输入、脚本、日志和报告保存在 `data/`。
- `mock_slurm=true` 模拟作业生命周期与结果，报告明确记录 `simulated=true`。

```powershell
cargo test
python scripts/smoke.py --token $env:HPC_API_TOKEN
```

## 真实 Slurm

在可访问 `sbatch/squeue/sacct/scancel/scontrol` 的 Linux Slurm 提交节点部署，设置 `mock_slurm=false`。配置 cluster ID 必须是 Slurm 集群名，应用与分区使用允许列表。`data_dir` 中的运行目录、上传目录放在提交节点和计算节点可见的同一共享文件系统；`database_path` 单独设为提交节点本地磁盘路径，避免 SQLite WAL 使用 NFS。镜像、参考基因组和索引按模板部署。

镜像内需包含 `bwa`、`samtools`、`gatk`，节点需能执行 `apptainer`。两个模板为双端 DNA FASTQ 到 germline / somatic VCF 的参考实现；真实数据集、镜像版本与参考资源需要项目方确认并实测。

API 使用 SHA256 存储的高熵 API key 与 owner 权限。可以通过配置 `api_keys` 为多个主体提供独立凭据；环境变量 token 创建管理员主体 `local-admin`。仅管理员可提交任意 Slurm 脚本、编辑模板或修改应用集群绑定。作业共用网关服务的 Unix 身份，HTTP owner 隔离不等同于计算节点上的 OS 用户隔离。

## 设计与验收资料

- [完整架构、数据模型、SQL、实施计划与逐项验收](docs/architecture.md)
- [全部接口及请求、返回示例](docs/api-examples.md)
- [OpenAPI 规范](openapi.json)
- [工作流 JSON Schema](workflows/schema.json)
- [k6 压测脚本](scripts/load.js)
- [本地实际验证记录](docs/verification.md)

压测示例（Linux / 安装 k6 的主机）：

```bash
API_TOKEN="$HPC_API_TOKEN" RATE=110 DURATION=10m k6 run scripts/load.js
```

100 QPS 为实测验收项，需保留服务器规格、版本、配置、k6 原始报告和服务日志。当前文档中的门槛不代表已在目标服务器达标。JSON 验证报告检查执行与产物结构；需要 precision / recall / F1 时应另用约定的 truth set 与比较工具。
