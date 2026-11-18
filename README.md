# 多源数据采集与RPA自动化平台

面向网页抓取、API接入、增量采集、RPA流程编排、数据校验与采集监控的数据工程平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
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

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源配置 | `datasource` | 数据源 | 源端编号、连接类型、接入地址 |
| 网页采集规则 | `crawler` | 采集规则 | 规则编号、目标网址、页面选择器 |
| API接入管理 | `apiclient` | API客户端 | 客户端编号、接口名称、请求方法 |
| 增量采集 | `incremental` | 增量游标 | 游标编号、来源标识、增量字段 |
| RPA任务编排 | `robot_task` | RPA任务 | 任务编号、任务名称、页面定位器 |
| 数据接入通道 | `ingestion` | 接入通道 | 通道编号、通道名称、输入格式 |
| 格式解析器 | `parser` | 解析配置 | 解析编号、输入格式、输出格式 |
| 采集结果校验 | `validator` | 校验规则 | 规则编号、校验字段、校验类型 |
| 采集去重 | `dedup` | 去重策略 | 策略编号、输入来源、去重键 |
| 采集调度 | `schedule` | 调度计划 | 计划编号、关联任务、调度周期 |
| 采集监控 | `monitor` | 监控面板 | 面板编号、采集源总数、今日采集量 |
| 采集日志 | `log_viewer` | 日志条目 | 日志编号、关联任务、日志级别 |
| 凭证管理 | `auth` | 认证凭证 | 凭证编号、凭证类型、关联数据源 |
| 采集转换管道 | `transform` | 转换节点 | 节点编号、输入来源、输出目标 |
| 数据流转编排 | `flow` | 流转管道 | 管道编号、管道名称、起始节点 |
| 数据血缘追踪 | `lineage` | 血缘关系 | 关系编号、上游节点、下游节点 |
| 采集报告 | `report` | 采集报告 | 报告编号、采集源、统计周期 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 监控分析分批执行

采集监控不再在一次请求里展开全部样本，改为可持久化、可恢复的分析批次：

- `POST /api/monitor/batches`：读取监控面板记录，冻结样本集合并生成监控指标快照，
  按 `单批容量` 拆成分批（分组），返回监控分析批次清单；重复提交通过幂等键/活动批次
  复用规则只生效一次。
- `POST /api/monitor/batches/{批次编号}/execute|stop`：执行/停止/继续执行，已完成分组
  不会重复计算，停止后续跑只处理未完成分组；`GET /api/monitor/batches/{批次编号}` 按
  同一编号回查，监控页与报告页看到相同结果。
- `POST /api/monitor/events`：新增采集事件。冲突策略 `freeze`（冻结当前样本集合，新
  事件进入下一批次）或 `new_batch`（原批次保留已完成分组并停止，未处理分组转到使用
  新样本集合的后继批次）；同一分析批次保证不混入两个样本版本。
- 采集报告侧入口：`GET /api/report/batch-reviews` 读取监控分析批次清单，
  `POST /api/report/batch-reviews` 按批次加权汇总成功率与平均延迟并生成“待复核”结果，
  同批次重复提交复用同一份复核记录。
- 所有写操作在 `Store.transaction` 快照事务内进行，落库失败（可用 `_fail_flag` 注入
  模拟）整体回滚到原批次状态。

后端验收脚本（无需额外依赖）：

```bash
cd backend
.venv/bin/python -I tests/verify_batches.py   # 服务层 52 项分批/幂等/回滚/冲突/复核验收
.venv/bin/python -I tests/verify_http.py      # 真实 uvicorn + HTTP 接口链路验收
```

领域：数据工程
子方向：数据采集、爬虫与RPA
