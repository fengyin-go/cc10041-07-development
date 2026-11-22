# 流批一体数据处理中心

统一管理流式加工和批量计算的数据处理平台，覆盖数据清洗、转换、关联和结果落地

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8016/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5196/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8016`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式数据源 | `stream_source` | 流式源 | 源名称、消息队列主题、序列化格式 |
| 转换规则 | `transform_rule` | 转换规则 | 规则名称、输入Schema、输出Schema |
| 批量计算管道 | `batch_pipeline` | 计算管道 | 管道名称、输入源、输出目标 |
| 数据关联合并 | `join_config` | 关联配置 | 关联名称、左表源、右表源 |
| 检查点管理 | `checkpoint_mgr` | 检查点 | 管道名称、检查点序列、创建时间 |
| 异常处理策略 | `error_handler` | 异常策略 | 策略名称、适用管道、异常类型 |
| 结果写入器 | `result_writer` | 写入配置 | 配置名称、写入目标类型、输出格式 |
| 迟到数据处理 | `late_data` | 迟到策略 | 策略名称、最大延迟时间、迟到处理方式 |
| 窗口配置 | `window_config` | 窗口配置 | 窗口名称、窗口类型、窗口长度 |
| 聚合计算规则 | `aggregation_rule` | 聚合规则 | 规则名称、聚合函数、分组键 |
| 数据清洗链 | `data_cleanse` | 清洗步骤 | 步骤序号、清洗类型、输入字段 |
| 管道监控 | `pipeline_monitor` | 监控指标 | 指标名称、监控目标、告警阈值 |
| 历史回填 | `backfill_handler` | 回填任务 | 任务名称、目标管道、回填时间范围 |
| 输出审计 | `output_audit` | 审计记录 | 管道名称、输出目标、输出时间 |
| Schema注册 | `schema_registry` | Schema定义 | Schema名称、字段列表、版本号 |
| 数据分区 | `data_partition` | 分区配置 | 分区名称、分区键、分区类型 |
| 管道编排DAG | `pipeline_dag` | 编排图 | 图名称、节点列表、依赖关系 |
| Iceberg表管理 | `iceberg_table` | Iceberg表 | 表名、所属业务域、存储格式 |
| 数据采样 | `data_sample` | 采样配置 | 配置名称、采样比例、采样方式 |
| 计算血缘追踪 | `lineage_tracker` | 血缘条目 | 输入表、转换管道、输出表 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 采样归属

Iceberg 表登记时按「表名 + 所属业务域」写入采样归属清单
（`backend/app/sampling.py`）。同名表在两个业务域各占一条登记，互不覆盖。

- **可信身份**：业务域与维护人只从网关头读取
  （`X-Trusted-Domain`、`X-Trusted-Principal`，可选 `X-Trusted-Grant`）。
  请求体里的表名/业务域只描述目标，不能当读取凭据。
- **旧配置**：`data_sample` 只存表名的配置仍可用；范围按可信域解析，
  两个域分别只能看到自己的分区。
- **排队与交付**：排队只记录身份与清单快照，不读分区；交付前重新读取清单，
  只取「归属已确认 + 分区已核准」的行，排队后被撤回的分区不会读取。
- **归属不明**：缺少可信上下文、域未确认或分区未核准时，返回核对依据
  （`basis`），不返回任何样本行。
- **可追溯**：每行交付都带 `domain / table / partition / source`
  （`iceberg://<域>/<表>/<分区>/<行>`），可按请求查交付了哪几行。

采样工作台接口（`/api/sampling/**`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/sampling/manifest` | 按可信域读归属清单（他域只给状态） |
| GET | `/api/sampling/scope?table=` | 同名表在各业务域的可见范围 |
| GET | `/api/sampling/legacy-scope?config_id=` | 旧采样配置的两域范围 |
| POST | `/api/sampling/claims` | 登记归属与分区（新登记从待核对起步） |
| POST | `/api/sampling/claims/{id}/confirm|reject` | 确认/驳回归属 |
| POST | `/api/sampling/partitions/{id}/approve|revoke` | 核准/撤回分区 |
| POST | `/api/sampling/requests` | 排队采样（旧配置动作也走这里） |
| POST | `/api/sampling/requests/{id}/execute` | 执行并重新核对清单 |
| GET | `/api/sampling/requests/{id}/delivery` | 查是否交付、交付了哪几行 |

前端在「Iceberg表管理」和「数据采样」页底部提供采样工作台
（`frontend/src/views/SamplingWorkbench.vue`）。

领域：数据工程
子方向：数仓与数据湖
