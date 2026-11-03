# REST API 请求与返回示例

基础地址 http://127.0.0.1:8080。除公开接口，其余请求需 Authorization 头。时间为 UTC；UUID、Slurm ID、哈希、大小和路径仅说明字段形态。Run 示例压缩展示关键字段，完整结构见 ../openapi.json。模板响应示例只演示字段；实际完整流程以 workflows 下的 YAML 为准。全部 JSON 代码块均为有效 JSON。

文件上传返回 ID 用于 deploy.inputs，不接受用户任意服务器路径。应用 genomics，集群 local；模板 germline / somatic。示例 simulated=true，真实集群验收须为 false。列表为数组。无 body 请求不必发送空 JSON。

## 1. 进程存活



```http
GET /healthz HTTP/1.1
Host: 127.0.0.1:8080
```

返回：200。

```json
{
  "status": "ok"
}
```

## 2. 数据库就绪及运行模式



```http
GET /readyz HTTP/1.1
Host: 127.0.0.1:8080
```

返回：200。

```json
{
  "status": "ready",
  "mode": "mock"
}
```

## 3. OpenAPI 3.0.3 JSON

Returns this complete document; example shows its beginning.

```http
GET /api-docs/openapi.json HTTP/1.1
Host: 127.0.0.1:8080
```

返回：200。

此处仅展示文档开头；实际返回完整静态规范。

```json
{
  "openapi": "3.0.3",
  "info": {
    "title": "HPC REST Gateway + Workflow Template Service",
    "version": "0.1.0"
  }
}
```

## 4. Swagger UI



```http
GET /swagger-ui/ HTTP/1.1
Host: 127.0.0.1:8080
```

返回：200。

```html
<html>Swagger UI</html>
```

## 5. 查询已登记集群



```http
GET /api/v1/clusters HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
[
  {
    "id": "local",
    "name": "Local Slurm",
    "partitions": [
      "cpu"
    ]
  }
]
```

## 6. 查询应用及当前默认集群



```http
GET /api/v1/applications HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
[
  {
    "id": "genomics",
    "name": "Genomics pipelines",
    "cluster_id": "local",
    "partitions": [
      "cpu"
    ]
  }
]
```

## 7. 查询应用可用队列

Configured application/cluster allowlist intersection, not live sinfo metrics.

```http
GET /api/v1/applications/genomics/queues HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "application_id": "genomics",
  "cluster_id": "local",
  "queues": [
    "cpu"
  ],
  "source": "configured_allowlist"
}
```

## 8. 绑定应用运行集群

Administrator only.

```http
PUT /api/v1/applications/genomics/cluster HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "cluster_id": "local"
}
```

返回：200。

```json
{
  "application_id": "genomics",
  "cluster_id": "local"
}
```

## 9. 查询已登记作业列表

Cached SQLite state; default refresh period 2 seconds. Caller-owned rows unless administrator.

```http
GET /api/v1/jobs HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
[
  {
    "id": "33333333-3333-4333-8333-333333333333",
    "owner": "local-admin",
    "application_id": "genomics",
    "cluster_id": "local",
    "name": "example",
    "run_id": null,
    "slurm_id": "12345",
    "state": "PENDING",
    "script_path": "/srv/hpc/data/jobs/33333333-3333-4333-8333-333333333333/job.sbatch",
    "created_at": "2026-10-19T02:00:00Z",
    "updated_at": "2026-10-19T02:00:00Z",
    "exit_code": null,
    "elapsed_seconds": null,
    "simulated": true
  }
]
```

## 10. 提交可信 Slurm 脚本

Administrator only. Submission timeout persists SUBMISSION_UNKNOWN. Reconcile Slurm before retry. HTTP 201 means local record created.

```http
POST /api/v1/jobs HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "application_id": "genomics",
  "cluster_id": "local",
  "name": "example",
  "script": "#!/bin/bash\necho hi\n"
}
```

返回：201。

```json
{
  "id": "33333333-3333-4333-8333-333333333333",
  "owner": "local-admin",
  "application_id": "genomics",
  "cluster_id": "local",
  "name": "example",
  "run_id": null,
  "slurm_id": "12345",
  "state": "PENDING",
  "script_path": "/srv/hpc/data/jobs/33333333-3333-4333-8333-333333333333/job.sbatch",
  "created_at": "2026-10-19T02:00:00Z",
  "updated_at": "2026-10-19T02:00:00Z",
  "exit_code": null,
  "elapsed_seconds": null,
  "simulated": true
}
```

## 11. 查询作业详情



```http
GET /api/v1/jobs/33333333-3333-4333-8333-333333333333 HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "id": "33333333-3333-4333-8333-333333333333",
  "owner": "local-admin",
  "application_id": "genomics",
  "cluster_id": "local",
  "name": "example",
  "run_id": null,
  "slurm_id": "12345",
  "state": "PENDING",
  "script_path": "/srv/hpc/data/jobs/33333333-3333-4333-8333-333333333333/job.sbatch",
  "created_at": "2026-10-19T02:00:00Z",
  "updated_at": "2026-10-19T02:00:00Z",
  "exit_code": null,
  "elapsed_seconds": null,
  "simulated": true
}
```

## 12. 取消/挂起/释放作业

hold for pending, release for held, cancel for active jobs. Terminal or submission-unknown states conflict.

```http
PATCH /api/v1/jobs/33333333-3333-4333-8333-333333333333/state HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "action": "hold"
}
```

返回：200。

```json
{
  "id": "33333333-3333-4333-8333-333333333333",
  "owner": "local-admin",
  "application_id": "genomics",
  "cluster_id": "local",
  "name": "example",
  "run_id": null,
  "slurm_id": "12345",
  "state": "HELD",
  "script_path": "/srv/hpc/data/jobs/33333333-3333-4333-8333-333333333333/job.sbatch",
  "created_at": "2026-10-19T02:00:00Z",
  "updated_at": "2026-10-19T02:00:00Z",
  "exit_code": null,
  "elapsed_seconds": null,
  "simulated": true
}
```

## 13. 查询文件列表



```http
GET /api/v1/files HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
[
  {
    "id": "11111111-1111-4111-8111-111111111111",
    "owner": "local-admin",
    "filename": "sample_R1.fastq",
    "path": "/srv/hpc/data/uploads/11111111-1111-4111-8111-111111111111",
    "size": 15,
    "sha256": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "created_at": "2026-10-19T02:00:00Z"
  }
]
```

## 14. 上传单个文件

Exactly one multipart field named file. Default file limit 64 MiB; server-generated ID.

```http
POST /api/v1/files HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: multipart/form-data; boundary=example

--example
Content-Disposition: form-data; name="file"; filename="sample_R1.fastq"
Content-Type: application/octet-stream

@r
ACGT
+
FFFF
--example--
```

返回：201。

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "owner": "local-admin",
  "filename": "sample_R1.fastq",
  "path": "/srv/hpc/data/uploads/11111111-1111-4111-8111-111111111111",
  "size": 15,
  "sha256": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
  "created_at": "2026-10-19T02:00:00Z"
}
```

## 15. 查询文件元数据



```http
GET /api/v1/files/11111111-1111-4111-8111-111111111111 HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "owner": "local-admin",
  "filename": "sample_R1.fastq",
  "path": "/srv/hpc/data/uploads/11111111-1111-4111-8111-111111111111",
  "size": 15,
  "sha256": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
  "created_at": "2026-10-19T02:00:00Z"
}
```

## 16. 查看最多 64 KiB 文件内容



```http
GET /api/v1/files/11111111-1111-4111-8111-111111111111/content HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "content": "@r\nACGT\n+\nFFFF\n",
  "truncated": false,
  "bytes": 15,
  "encoding": "utf-8-lossy"
}
```

## 17. 流式下载文件



```http
GET /api/v1/files/11111111-1111-4111-8111-111111111111/download HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```http
HTTP/1.1 200 OK
Content-Type: application/octet-stream
Content-Disposition: attachment; filename="11111111-1111-4111-8111-111111111111"

@r
ACGT
+
FFFF
```

## 18. 查询当前流程模板

Example is representative; repository YAML files are the complete real workflows.

```http
GET /api/v1/workflows HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
[
  {
    "id": "germline",
    "name": "Germline variant calling",
    "version": "1.0.0",
    "application": "genomics",
    "description": "Paired-end FASTQ to germline variants.",
    "image": "/opt/hpc/images/genomics.sif",
    "references": {
      "fasta": "/opt/hpc/references/GRCh38.fa"
    },
    "inputs": {
      "reads_r1": {
        "kind": "fastq",
        "required": true,
        "description": "Read 1 FASTQ"
      },
      "reads_r2": {
        "kind": "fastq",
        "required": true,
        "description": "Read 2 FASTQ"
      }
    },
    "parameters": {
      "sample_id": {
        "type": "string",
        "default": "sample01",
        "format": "sample_id"
      },
      "threads": {
        "type": "integer",
        "default": 4,
        "minimum": 1,
        "maximum": 128
      },
      "memory_gb": {
        "type": "integer",
        "default": 8,
        "minimum": 1,
        "maximum": 1024
      },
      "time_minutes": {
        "type": "integer",
        "default": 120,
        "minimum": 1,
        "maximum": 10080
      }
    },
    "resources": {
      "cpus": 4,
      "memory_gb": 8,
      "time_minutes": 120
    },
    "steps": [
      {
        "id": "align",
        "command": "bwa mem -t {{param:threads}} {{reference:fasta}} {{input:reads_r1}} {{input:reads_r2}} | samtools sort -o aligned.bam"
      },
      {
        "id": "index",
        "command": "samtools index aligned.bam"
      },
      {
        "id": "call",
        "command": "gatk HaplotypeCaller -R {{reference:fasta}} -I aligned.bam -O {{output:variants}}"
      }
    ],
    "outputs": {
      "variants": {
        "path": "results/germline.filtered.vcf",
        "kind": "vcf",
        "required": true
      }
    }
  }
]
```

## 19. 查询当前流程模板详情

Example is representative; repository YAML includes all reference/index checks and real pipeline steps.

```http
GET /api/v1/workflows/germline HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "id": "germline",
  "name": "Germline variant calling",
  "version": "1.0.0",
  "application": "genomics",
  "description": "Paired-end FASTQ to germline variants.",
  "image": "/opt/hpc/images/genomics.sif",
  "references": {
    "fasta": "/opt/hpc/references/GRCh38.fa"
  },
  "inputs": {
    "reads_r1": {
      "kind": "fastq",
      "required": true,
      "description": "Read 1 FASTQ"
    },
    "reads_r2": {
      "kind": "fastq",
      "required": true,
      "description": "Read 2 FASTQ"
    }
  },
  "parameters": {
    "sample_id": {
      "type": "string",
      "default": "sample01",
      "format": "sample_id"
    },
    "threads": {
      "type": "integer",
      "default": 4,
      "minimum": 1,
      "maximum": 128
    },
    "memory_gb": {
      "type": "integer",
      "default": 8,
      "minimum": 1,
      "maximum": 1024
    },
    "time_minutes": {
      "type": "integer",
      "default": 120,
      "minimum": 1,
      "maximum": 10080
    }
  },
  "resources": {
    "cpus": 4,
    "memory_gb": 8,
    "time_minutes": 120
  },
  "steps": [
    {
      "id": "align",
      "command": "bwa mem -t {{param:threads}} {{reference:fasta}} {{input:reads_r1}} {{input:reads_r2}} | samtools sort -o aligned.bam"
    },
    {
      "id": "index",
      "command": "samtools index aligned.bam"
    },
    {
      "id": "call",
      "command": "gatk HaplotypeCaller -R {{reference:fasta}} -I aligned.bam -O {{output:variants}}"
    }
  ],
  "outputs": {
    "variants": {
      "path": "results/germline.filtered.vcf",
      "kind": "vcf",
      "required": true
    }
  }
}
```

## 20. 保存新的不可变模板版本

Administrator only. Increment version. Existing (ID,version) cannot be overwritten. Template commands are administrator code.

```http
PUT /api/v1/workflows/germline HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "yaml": "id: germline\nname: Germline variant calling\nversion: 1.0.1\napplication: genomics\ndescription: Paired FASTQ through alignment, duplicate marking, BQSR, haplotype calling and basic quality filtering. GRCh38 references and a pinned tool image are provisioned by the administrator.\nimage: /opt/hpc/images/genomics-1.0.0.sif\nreferences:\n  fasta: /opt/hpc/references/GRCh38/genome.fa\n  fasta_index: /opt/hpc/references/GRCh38/genome.fa.fai\n  fasta_dict: /opt/hpc/references/GRCh38/genome.dict\n  bwa_amb: /opt/hpc/references/GRCh38/genome.fa.amb\n  bwa_ann: /opt/hpc/references/GRCh38/genome.fa.ann\n  bwa_bwt: /opt/hpc/references/GRCh38/genome.fa.bwt\n  bwa_pac: /opt/hpc/references/GRCh38/genome.fa.pac\n  bwa_sa: /opt/hpc/references/GRCh38/genome.fa.sa\n  known_sites: /opt/hpc/references/GRCh38/dbsnp.vcf.gz\n  known_sites_index: /opt/hpc/references/GRCh38/dbsnp.vcf.gz.tbi\ninputs:\n  reads_r1: {kind: fastq, required: true, description: Paired-end sample read 1, FASTQ or FASTQ.gz}\n  reads_r2: {kind: fastq, required: true, description: Paired-end sample read 2, FASTQ or FASTQ.gz}\nparameters:\n  sample_id:\n    type: string\n    default: SAMPLE001\n    format: sample_id\n    description: Read group and VCF sample identifier\n  threads: {type: integer, default: 4, minimum: 1, maximum: 64}\n  memory_gb: {type: integer, default: 8, minimum: 4, maximum: 512}\n  time_minutes: {type: integer, default: 120, minimum: 1, maximum: 10080}\nresources: {cpus: 4, memory_gb: 8, time_minutes: 120}\nsteps:\n  - id: align_and_sort\n    command: >-\n      bwa mem -t {{param:threads}}\n      -R '@RG\\tID:'{{param:sample_id}}'\\tSM:'{{param:sample_id}}'\\tPL:ILLUMINA'\n      {{reference:fasta}} {{input:reads_r1}} {{input:reads_r2}}\n      | samtools sort -@ {{param:threads}} -m 256M -o {{run_dir}}/aligned.bam -\n  - id: mark_duplicates\n    command: >-\n      gatk --java-options '-Xmx2g' MarkDuplicates\n      -I {{run_dir}}/aligned.bam -O {{run_dir}}/dedup.bam\n      -M {{run_dir}}/duplicate_metrics.txt --CREATE_INDEX true\n  - id: base_recalibration\n    command: >-\n      gatk --java-options '-Xmx2g' BaseRecalibrator\n      -R {{reference:fasta}} -I {{run_dir}}/dedup.bam\n      --known-sites {{reference:known_sites}} -O {{run_dir}}/recalibration.table\n  - id: apply_recalibration\n    command: >-\n      gatk --java-options '-Xmx2g' ApplyBQSR -R {{reference:fasta}}\n      -I {{run_dir}}/dedup.bam --bqsr-recal-file {{run_dir}}/recalibration.table\n      -O {{run_dir}}/recalibrated.bam\n  - id: haplotype_caller\n    command: >-\n      gatk --java-options '-Xmx2g' HaplotypeCaller -R {{reference:fasta}}\n      -I {{run_dir}}/recalibrated.bam --native-pair-hmm-threads {{param:threads}}\n      -O {{run_dir}}/raw.vcf\n  - id: variant_quality_filter\n    command: >-\n      gatk --java-options '-Xmx2g' VariantFiltration -R {{reference:fasta}}\n      -V {{run_dir}}/raw.vcf --filter-name LowQual --filter-expression 'QUAL < 30.0'\n      -O {{output:variants}}\noutputs:\n  variants: {path: results/germline.filtered.vcf, kind: vcf, required: true}\n"
}
```

返回：200。

```json
{
  "id": "germline",
  "name": "Germline variant calling",
  "version": "1.0.1",
  "application": "genomics",
  "description": "Paired-end FASTQ to germline variants.",
  "image": "/opt/hpc/images/genomics.sif",
  "references": {
    "fasta": "/opt/hpc/references/GRCh38.fa"
  },
  "inputs": {
    "reads_r1": {
      "kind": "fastq",
      "required": true,
      "description": "Read 1 FASTQ"
    },
    "reads_r2": {
      "kind": "fastq",
      "required": true,
      "description": "Read 2 FASTQ"
    }
  },
  "parameters": {
    "sample_id": {
      "type": "string",
      "default": "sample01",
      "format": "sample_id"
    },
    "threads": {
      "type": "integer",
      "default": 4,
      "minimum": 1,
      "maximum": 128
    },
    "memory_gb": {
      "type": "integer",
      "default": 8,
      "minimum": 1,
      "maximum": 1024
    },
    "time_minutes": {
      "type": "integer",
      "default": 120,
      "minimum": 1,
      "maximum": 10080
    }
  },
  "resources": {
    "cpus": 4,
    "memory_gb": 8,
    "time_minutes": 120
  },
  "steps": [
    {
      "id": "align",
      "command": "bwa mem -t {{param:threads}} {{reference:fasta}} {{input:reads_r1}} {{input:reads_r2}} | samtools sort -o aligned.bam"
    },
    {
      "id": "index",
      "command": "samtools index aligned.bam"
    },
    {
      "id": "call",
      "command": "gatk HaplotypeCaller -R {{reference:fasta}} -I aligned.bam -O {{output:variants}}"
    }
  ],
  "outputs": {
    "variants": {
      "path": "results/germline.filtered.vcf",
      "kind": "vcf",
      "required": true
    }
  }
}
```

## 21. 检查并生成运行目录和脚本

No submission. Inputs are uploaded file IDs. Real mode checks image/references; mock marks skipped. Resolved defaults in deployment.snapshot.parameters.

```http
POST /api/v1/workflows/germline/deploy HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "inputs": {
    "reads_r1": "11111111-1111-4111-8111-111111111111",
    "reads_r2": "22222222-2222-4222-8222-222222222222"
  },
  "parameters": {
    "sample_id": "sample01",
    "threads": 4
  },
  "cluster_id": "local"
}
```

返回：201。

```json
{
  "id": "44444444-4444-4444-8444-444444444444",
  "owner": "local-admin",
  "workflow_id": "germline",
  "workflow_version": "1.0.0",
  "inputs": {
    "reads_r1": "11111111-1111-4111-8111-111111111111",
    "reads_r2": "22222222-2222-4222-8222-222222222222"
  },
  "parameters": {
    "sample_id": "sample01",
    "threads": 4
  },
  "cluster_id": "local",
  "state": "DEPLOYED",
  "job_id": null,
  "created_at": "2026-10-19T02:00:00Z",
  "updated_at": "2026-10-19T02:00:00Z",
  "started_at": null,
  "finished_at": null,
  "elapsed_seconds": null,
  "exit_code": null,
  "simulated": true
}
```

## 22. 查询流程运行列表



```http
GET /api/v1/runs HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
[
  {
    "id": "44444444-4444-4444-8444-444444444444",
    "owner": "local-admin",
    "workflow_id": "germline",
    "workflow_version": "1.0.0",
    "inputs": {
      "reads_r1": "11111111-1111-4111-8111-111111111111",
      "reads_r2": "22222222-2222-4222-8222-222222222222"
    },
    "parameters": {
      "sample_id": "sample01",
      "threads": 4
    },
    "cluster_id": "local",
    "state": "DEPLOYED",
    "job_id": null,
    "created_at": "2026-10-19T02:00:00Z",
    "updated_at": "2026-10-19T02:00:00Z",
    "started_at": null,
    "finished_at": null,
    "elapsed_seconds": null,
    "exit_code": null,
    "simulated": true
  }
]
```

## 23. 查询流程运行详情



```http
GET /api/v1/runs/44444444-4444-4444-8444-444444444444 HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "id": "44444444-4444-4444-8444-444444444444",
  "owner": "local-admin",
  "workflow_id": "germline",
  "workflow_version": "1.0.0",
  "inputs": {
    "reads_r1": "11111111-1111-4111-8111-111111111111",
    "reads_r2": "22222222-2222-4222-8222-222222222222"
  },
  "parameters": {
    "sample_id": "sample01",
    "threads": 4
  },
  "cluster_id": "local",
  "state": "DEPLOYED",
  "job_id": null,
  "created_at": "2026-10-19T02:00:00Z",
  "updated_at": "2026-10-19T02:00:00Z",
  "started_at": null,
  "finished_at": null,
  "elapsed_seconds": null,
  "exit_code": null,
  "simulated": true
}
```

## 24. 提交已部署流程到 Slurm

No request body. Only DEPLOYED can execute. Create another deployment to repeat; never blindly retry unknown submission.

```http
POST /api/v1/runs/44444444-4444-4444-8444-444444444444/execute HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：202。

```json
{
  "id": "44444444-4444-4444-8444-444444444444",
  "owner": "local-admin",
  "workflow_id": "germline",
  "workflow_version": "1.0.0",
  "inputs": {
    "reads_r1": "11111111-1111-4111-8111-111111111111",
    "reads_r2": "22222222-2222-4222-8222-222222222222"
  },
  "parameters": {
    "sample_id": "sample01",
    "threads": 4
  },
  "cluster_id": "local",
  "state": "PENDING",
  "job_id": "33333333-3333-4333-8333-333333333333",
  "created_at": "2026-10-19T02:00:00Z",
  "updated_at": "2026-10-19T02:00:00Z",
  "started_at": "2026-10-19T02:00:00Z",
  "finished_at": null,
  "elapsed_seconds": null,
  "exit_code": null,
  "simulated": true
}
```

## 25. 分段读取运行日志

Use next_offset for the next poll; missing log returns empty content.

```http
GET /api/v1/runs/44444444-4444-4444-8444-444444444444/logs?offset=0&limit=65536 HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "run_id": "44444444-4444-4444-8444-444444444444",
  "content": "step=align\n",
  "offset": 0,
  "next_offset": 11,
  "truncated": false,
  "simulated": true
}
```

## 26. 查询并登记运行输出供下载

Terminal state required. Missing outputs have exists=false. Mock fixtures are generated on completion.

```http
GET /api/v1/runs/44444444-4444-4444-8444-444444444444/results HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "run_id": "44444444-4444-4444-8444-444444444444",
  "state": "COMPLETED",
  "simulated": true,
  "outputs": [
    {
      "name": "variants",
      "exists": true,
      "file_id": "44444444-4444-4444-8444-444444444444-variants",
      "size": 165,
      "download_url": "/api/v1/files/44444444-4444-4444-8444-444444444444-variants/download"
    }
  ]
}
```

## 27. 验证输出并保存 JSON 报告

No body. Terminal state required. Pass requires COMPLETED and exit code 0:0. Checks files, SHA-256 and VCF structure, not biological accuracy. Retains failed reports.

```http
POST /api/v1/runs/44444444-4444-4444-8444-444444444444/validate HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "run_id": "44444444-4444-4444-8444-444444444444",
  "workflow_id": "germline",
  "workflow_version": "1.0.0",
  "generated_at": "2026-10-19T02:00:00Z",
  "simulated": true,
  "validation_scope": "output existence, SHA-256 integrity and basic VCF structure; biological accuracy is not evaluated",
  "passed": true,
  "results": [
    {
      "name": "variants",
      "path": "results/germline.filtered.vcf",
      "size_bytes": 165,
      "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "passed": true,
      "details": {
        "format": "VCF",
        "variant_records": 1,
        "structural_validation": "passed"
      }
    }
  ],
  "biological_validation": {
    "performed": false,
    "reason": "requires independently curated truth sets, comparison tools and acceptance thresholds"
  },
  "job_state": "COMPLETED",
  "exit_code": "0:0",
  "inputs": {
    "reads_r1": "11111111-1111-4111-8111-111111111111",
    "reads_r2": "22222222-2222-4222-8222-222222222222"
  },
  "parameters": {
    "sample_id": "sample01",
    "threads": 4
  },
  "resolved_parameters": {
    "sample_id": "sample01",
    "threads": 4,
    "memory_gb": 8,
    "time_minutes": 120
  },
  "elapsed_seconds": 3,
  "started_at": "2026-10-19T02:00:00Z",
  "finished_at": "2026-10-19T02:00:03Z"
}
```

## 28. 读取已保存验证报告

404 until a report has been generated.

```http
GET /api/v1/runs/44444444-4444-4444-8444-444444444444/report HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "run_id": "44444444-4444-4444-8444-444444444444",
  "workflow_id": "germline",
  "workflow_version": "1.0.0",
  "generated_at": "2026-10-19T02:00:00Z",
  "simulated": true,
  "validation_scope": "output existence, SHA-256 integrity and basic VCF structure; biological accuracy is not evaluated",
  "passed": true,
  "results": [
    {
      "name": "variants",
      "path": "results/germline.filtered.vcf",
      "size_bytes": 165,
      "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "passed": true,
      "details": {
        "format": "VCF",
        "variant_records": 1,
        "structural_validation": "passed"
      }
    }
  ],
  "biological_validation": {
    "performed": false,
    "reason": "requires independently curated truth sets, comparison tools and acceptance thresholds"
  },
  "job_state": "COMPLETED",
  "exit_code": "0:0",
  "inputs": {
    "reads_r1": "11111111-1111-4111-8111-111111111111",
    "reads_r2": "22222222-2222-4222-8222-222222222222"
  },
  "parameters": {
    "sample_id": "sample01",
    "threads": 4
  },
  "resolved_parameters": {
    "sample_id": "sample01",
    "threads": 4,
    "memory_gb": 8,
    "time_minutes": 120
  },
  "elapsed_seconds": 3,
  "started_at": "2026-10-19T02:00:00Z",
  "finished_at": "2026-10-19T02:00:03Z"
}
```

## 29. 转发 GET 到白名单上游

service maps to fixed base URL; path may contain slashes. Query forwarded. No redirects or credential forwarding. Request max 1 MiB. Status and streamed content come from upstream.

```http
GET /api/v1/gateway/echo/echo?sample=demo HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "method": "GET",
  "path": "/echo",
  "query": "sample=demo"
}
```

## 30. 转发 POST 到白名单上游

service maps to fixed base URL; path may contain slashes. Query forwarded. No redirects or credential forwarding. Request max 1 MiB. Status and streamed content come from upstream.

```http
POST /api/v1/gateway/echo/echo?sample=demo HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "sample": "demo"
}
```

返回：200。

```json
{
  "method": "POST",
  "path": "/echo",
  "query": "sample=demo"
}
```

## 31. 转发 PUT 到白名单上游

service maps to fixed base URL; path may contain slashes. Query forwarded. No redirects or credential forwarding. Request max 1 MiB. Status and streamed content come from upstream.

```http
PUT /api/v1/gateway/echo/echo?sample=demo HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "sample": "demo"
}
```

返回：200。

```json
{
  "method": "PUT",
  "path": "/echo",
  "query": "sample=demo"
}
```

## 32. 转发 PATCH 到白名单上游

service maps to fixed base URL; path may contain slashes. Query forwarded. No redirects or credential forwarding. Request max 1 MiB. Status and streamed content come from upstream.

```http
PATCH /api/v1/gateway/echo/echo?sample=demo HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "sample": "demo"
}
```

返回：200。

```json
{
  "method": "PATCH",
  "path": "/echo",
  "query": "sample=demo"
}
```

## 33. 转发 DELETE 到白名单上游

service maps to fixed base URL; path may contain slashes. Query forwarded. No redirects or credential forwarding. Request max 1 MiB. Status and streamed content come from upstream.

```http
DELETE /api/v1/gateway/echo/echo?sample=demo HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
```

返回：200。

```json
{
  "method": "DELETE",
  "path": "/echo",
  "query": "sample=demo"
}
```

## 两流程分别定制、部署和编辑

Somatic 上传四个 FASTQ 后发送：

```http
POST /api/v1/workflows/somatic/deploy HTTP/1.1
Host: 127.0.0.1:8080
Authorization: Bearer <API_TOKEN>
Content-Type: application/json

{
  "inputs": {
    "tumor_r1": "11111111-1111-4111-8111-111111111111",
    "tumor_r2": "22222222-2222-4222-8222-222222222222",
    "normal_r1": "55555555-5555-4555-8555-555555555555",
    "normal_r2": "66666666-6666-4666-8666-666666666666"
  },
  "parameters": {
    "tumor_sample": "tumor01",
    "normal_sample": "normal01",
    "threads": 8,
    "memory_gb": 16,
    "time_minutes": 240
  },
  "cluster_id": "local"
}
```

返回 201，结构同 germline，workflow_id 为 somatic，state 为 DEPLOYED。随后 execute、等待终态、validate、report、results。两流程均可通过 parameters 定制。管理员基于仓库完整 YAML 编辑并提高 version 后 PUT，可分别保存两个模板的新版本；既有运行保存原快照。

## 统一错误示例与原生拒绝

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json

{"error":{"code":"unauthorized","message":"Valid Bearer API key required"}}
```

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json

{"error":{"code":"rate_limited","message":"Request rate exceeded"}}
```

当前用户访问不到的对象通常 404，管理员入口普通用户调用为 403；参数校验 400、重复版本或执行 409、超限 413、上游调用失败 502。Axum JSON/Query/Body extractor 的语法错误和大小拒绝可能返回原生 text/plain，客户端需先看状态与 Content-Type。提交结果不明确时记录 SUBMISSION_UNKNOWN，先核对真实 Slurm 再人工处理，不自动重提。
