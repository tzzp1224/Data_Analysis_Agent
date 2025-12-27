# Agentic Finance | Enterprise Data Analyst

**Agentic Finance** 是一个面向企业财务与数据分析场景的智能 Agent 系统。它利用 LLM 的语义理解能力与 Pandas 的数据处理能力，通过 **LangGraph** 编排实现了从数据清洗、实体对齐到复杂财务对账的全自动化流程。

系统采用 **Supervisor-Worker** 架构，具备代码生成、自愈修正（Self-Healing）及全链路审计（Audit Logging）能力。

## 🏗 System Architecture (系统架构)

本项目采用前后端分离架构，核心逻辑由 LangGraph 状态机驱动：

[![](https://mermaid.ink/img/pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaTgMGgka7ukbDT3f6_Y9fzgYjyd-4AfBdYfuMDzojgeTSRCMJp7vD0bB6NCh703VfjcYD_ut5R3-AVb_Hok?type=png)](https://mermaid.live/edit#pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaTgMGgka7ukbDT3f6_Y9fzgYjyd-4AfBdYfuMDzojgeTSRCMJp7vD0bB6NCh703VfjcYD_ut5R3-AVb_Hok)

### Core Components

- **Orchestrator (`workflow.py`)**: 基于 LangGraph 的状态机，管理用户指令、上下文流转及错误重试。
- **Ingestion (`ingestion.py`)**: 智能数据摄取模块，利用 LLM 自动识别 Excel Header 与 Sheet，无需人工配置。
- **Tools (`tools.py`)**: 封装了企业级数据处理算子：
  - `AuditLogger`: 记录所有数据变更与剔除操作。
  - `VectorMatcher`: 基于 `sentence-transformers` 的语义实体对齐（如 "ByteDance" <-> "字节跳动"）。
  - `SmartReconcile`: 支持金额容差（Tolerance）与多对一聚合的财务对账工具。

## 🚀 Key Features (核心功能)

系统将数据分析能力划分为四个层级（L1-L4）：

### L1: Intelligent Hygiene (智能清洗)

- 自动化识别并剔除完全重复行、空值。
- **异常检测**：自动扫描负数金额、极端异常值（Outliers）。
- **审计追踪**：所有清洗操作均记录在 `AuditLogger` 中，提供“被剔除数据”的独立快照，确保合规性。

### L2: Semantic Entity Alignment (语义对齐)

- 解决多表关联中的 Key 不一致问题（如中文名 vs 英文名、全称 vs 简称）。
- 结合 **Fuzzy Matching (RapidFuzz)** 与 **Vector Embedding (MiniLM)** 实现高召回率匹配。
- 引入 **LLM Judge** 机制，利用大模型世界知识进行最终裁决。

### L3: Financial Reconciliation (智能对账)

- **容差匹配**：支持设定金额误差范围（如忽略 0.01 元或 5 元以内的差异）。
- **多对一处理**：支持聚合系统流水（多笔订单）与银行流水（单笔汇总）进行核对。
- **状态分类**：自动生成 `完全匹配`、`容差匹配`、`金额不符`、`单边账` 四种对账状态。

### L4: Interactive Visualization (交互分析)

- 基于 Plotly 生成交互式图表。
- 支持自然语言指令生成趋势图、分布图，并自动提取分析洞察（Insights）。

## 📂 Project Structure (项目结构)

Plaintext

```
.
├── app/
│   ├── core/
│   │   └── config.py          # 环境变量配置
│   ├── services/
│   │   ├── ingestion.py       # 智能文件加载 (Schema Inference)
│   │   ├── llm_factory.py     # LLM 实例工厂 (Google Gemini)
│   │   └── workflow.py        # LangGraph 核心编排逻辑
│   ├── utils/
│   │   ├── tools.py           # 核心算子 (Audit, VectorMatch, Reconcile)
│   │   ├── generator.py       # 测试数据生成器 (清洗/对齐)
│   │   └── finance_generator.py # 财务对账测试数据生成器
│   ├── server.py              # FastAPI 后端入口
│   └── ui.py                  # Streamlit 前端入口
├── data/                      # 数据存储目录
├── temp_uploads/              # 临时上传区
├── temp_outputs/              # 结果导出区
├── main.py                    # CLI 模式入口 (调试用)
└── requirements.txt           # 项目依赖
```

## 🛠 Installation & Usage (安装与使用)

### Prerequisites

- Python 3.9+
- Google Gemini API Key

### 1. Environment Setup

Bash

```
# Clone repository
git clone <repository_url>
cd agentic-finance

# Install dependencies
pip install -r requirements.txt

# Setup Environment Variables
# Create a .env file and add your API key
echo "GOOGLE_API_KEY=your_api_key_here" > .env
```

### 2. Start the Application

需分别启动后端 API 服务和前端 UI 服务。

**Terminal 1 (Backend):**

Bash

```
uvicorn app.server:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 (Frontend):**

Bash

```
streamlit run app/ui.py
```

### 3. Workflow

1. 在 Web UI 上传 Excel/CSV 数据表。
2. 等待 Agent 自动识别表头并加载数据。
3. 输入自然语言指令（例如：“清洗数据”、“核对系统账和银行账”、“分析销售趋势”）。
4. 查看 Agent 的执行过程、审计日志及可视化图表。
5. 下载包含清洗结果和审计报告的 Excel 文件。

## 🔮 Roadmap & Future Optimization (待优化方向)

当前版本为 **Proof of Concept (PoC)**，在生产环境中部署需重点关注以下方向：

### 🔴 P0: Security Sandbox (安全沙箱化) [High Priority]

- **现状**：当前代码执行采用 Python 原生 `exec()`，存在严重的安全隐患（如文件系统访问、无限循环等）。
- **计划**：引入 **Docker** 容器化沙箱。
  - 将 `app.services.workflow.execute_code` 改造为通过 Docker SDK 调用独立容器。
  - 利用 Volume 挂载实现主机与沙箱间的数据（Parquet/Pickle）交换。
  - 限制容器的网络访问与资源使用（CPU/RAM）。

### 🟠 P1: Persistence Layer (持久化层)

- **现状**：Session 状态与 DataFrame 均存储于内存中，重启服务会导致状态丢失。
- **计划**：引入 **Redis** 存储 LangGraph Checkpoints，引入 **PostgreSQL/MinIO** 存储上传的数据文件与处理结果。

### 🟡 P2: Vector Store Optimization (向量库优化)

- **现状**：使用内存级 `SentenceTransformer`，在数据量 >10k 时存在性能瓶颈。
- **计划**：集成 **FAISS** 或 **ChromaDB**，支持大规模实体对齐检索。

### 🔵 P3: Advanced Planning (高级规划)

- 支持 **Human-in-the-loop**：当 Agent 遇到不确定的模糊匹配时，主动暂停并请求人类确认。

## 📄 License

[MIT License](https://www.google.com/search?q=LICENSE)