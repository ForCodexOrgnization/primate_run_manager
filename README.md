# Primate run manager

外围管理层，不修改现有 `launch_pipeline_streaming_per_sample.sh` 和 Nextflow 流程。

## 状态流

`PENDING -> SUBMITTED -> READY_TO_TRANSFER -> TRANSFERRING -> TRANSFERRED_FULL -> LOCAL_FINAL_RETAINED`

失败的 Globus 任务标记为 `TRANSFER_FAILED`。分析流程未完成时，样本保留原状态并在 notes 中记录缺失文件。

## Workspace 与本地保留策略

- Workspace：完整传输整个样本输出目录。
- HPC：Globus task 成功后，保留以下文件到 `ANALYSIS_ROOT`：
  - `*.round2.original_coords.clean.final.split.vcf.gz` 和可选 `.tbi`
  - `*.round2.original_coords.per_base_coverage.tsv`
  - `*.numt_decoy.clean.realigned.per_base_coverage.tsv`
  - `*.round2.mtcn.tsv`
- 其他本地样本目录内容在启用清理后删除。

## 初次部署

```bash
cd /home/lt692/ycga_work
git clone <your-repository-url> primate_run_manager
cd primate_run_manager
chmod +x bin/*.sh manager_daemon.slurm
cp config/hpc2.template.sh config/<second_hpc>.sh
```

编辑每台 HPC 对应的配置文件。特别区分：

- `LOCAL_RESULTS`：作业看到的 POSIX 路径。
- `SOURCE_ROOT`：Globus Collection 看到的路径。

## 分配两台 HPC

输入必须为无表头、严格 TAB 分隔的两列：

```text
DRS139837<TAB>Macaca_fascicularis
```

```bash
bin/split_samples_for_two_hpcs.sh samples/all_samples.txt \
  samples/bouchet_samples.txt samples/hpc2_samples.txt
```

## 初始化

```bash
bin/initialize_samples.sh config/bouchet.sh
```

## 安全上线顺序

配置初始值：

```bash
ENABLE_TRANSFER=1
ENABLE_LOCAL_CLEANUP=0
```

先运行若干批次并人工确认 Workspace 内容。确认稳定后再改为：

```bash
ENABLE_LOCAL_CLEANUP=1
```

## 单次管理循环

```bash
bin/manager_cycle.sh config/bouchet.sh
```

## 持续运行

```bash
RUN_MANAGER_CONFIG=/absolute/path/primate_run_manager/config/bouchet.sh \
  sbatch manager_daemon.slurm
```

## 常用检查

```bash
column -t -s $'\t' state/sample_status.tsv | less -S
column -t -s $'\t' state/transfer_tasks.tsv | less -S
awk -F '\t' 'NR>1{n[$4]++} END{for(k in n) print k,n[k]}' state/sample_status.tsv | sort
```

## Globus 前提

每台 HPC 的运行账号需要已完成：

```bash
globus login --no-local-server
```

并确保以下命令都成功：

```bash
globus ls "${SOURCE_COLLECTION}:${SOURCE_ROOT}"
globus ls "${DEST_COLLECTION}:${DEST_ROOT}"
```
