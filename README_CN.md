# Agentic Finance | 智能财务数据分析助手

[![Language](https://img.shields.io/badge/Lang-English-blue.svg)](README.md)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Agentic Finance** 是一个面向企业级场景的智能财务数据分析系统。项目基于 **LangGraph** 进行编排，利用 **Google Gemini** 的推理能力，实现了从数据摄取、Schema 推断、实体对齐到复杂财务对账的全流程自动化。

系统采用 **Supervisor-Worker（管理-执行）架构**，具备代码自动生成、错误自愈（Self-Healing）及全链路审计（Audit Logging）能力。

## 系统架构

核心逻辑由状态机驱动，协调 Supervisor（决策节点）与 Python Worker（执行节点）之间的交互。

[![](https://mermaid.ink/img/pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaThsFGi4p2809Hyv2_f84WA8nviBHwTXHbrD8KA7HkwmQTCaeL4_GAWjQ4e-N0X73WA87LeWd_gHOXceUQ?type=png)](https://mermaid.live/edit#pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaThsFGi4p2809Hyv2_f84WA8nviBHwTXHbrD8KA7HkwmQTCaeL4_GAWjQ4e-N0X73WA87LeWd_gHOXceUQ)

## 核心功能

系统能力划分为四个层级（L1-L4）：

### L1: 智能清洗 (Intelligent Hygiene)

- **Schema 推断**：利用 LLM 自动识别 Excel 表头行（Header）与有效工作表（Sheet）。
- **数据清洗**：自动扫描并处理重复行、空值及异常值（如负数金额、极端值）。
- **合规审计**：通过 `AuditLogger` 记录所有数据变更操作（删除、填充、剔除），确保数据处理过程可追溯。

### L2: 语义实体对齐 (Semantic Entity Alignment)

- **场景**：解决多表关联中主体名称不一致的问题（例如：“腾讯科技” vs “Tencent”）。
- **方案**：采用混合匹配策略，结合 **RapidFuzz**（模糊匹配）与 **Sentence-Transformers**（向量语义匹配），并引入 LLM 作为最终裁判。

### L3: 财务对账 (Financial Reconciliation)

- **容差匹配**：支持设定金额误差范围（Tolerance），允许忽略微小差异（如 < 0.01 元）。
- **多对一聚合**：自动处理“多笔系统流水对应单笔银行流水”的复杂聚合场景。
- **状态分类**：自动生成对账结果，包括“完全匹配”、“容差匹配”、“单边账”（仅系统/仅银行）等状态。

### L4: 交互式可视化 (Interactive Visualization)

- 基于自然语言指令生成 Plotly 交互式图表。
- 在图表输出的同时，自动提取数据趋势与业务洞察。

## 安装与部署

### 环境要求

- Python 3.9+
- Google Gemini API Key

### 安装步骤

1. **克隆代码仓库**

   Bash

   ```
   git clone [https://github.com/your-username/agentic-finance.git](https://github.com/your-username/agentic-finance.git)
   cd agentic-finance
   ```

2. **安装依赖**

   Bash

   ```
   pip install -r requirements.txt
   ```

3. **配置环境变量** 在项目根目录创建 `.env` 文件：

   Bash

   ```
   GOOGLE_API_KEY=your_api_key_here
   ```

## 启动服务

系统需要同时启动后端 API 和前端 UI 服务。

**1. 启动后端 (FastAPI)**

Bash

```
uvicorn app.server:app --reload --host 0.0.0.0 --port 8000
```

**2. 启动前端 (Streamlit)**

Bash

```
streamlit run app/ui.py
```

启动后，访问浏览器地址 `http://localhost:8501` 使用系统。

## 待优化方向 (Roadmap)

- **安全沙箱 (P0):** 引入 Docker 容器化方案，替代本地 `exec()`，实现代码执行环境的完全隔离。
- **持久化存储 (P1):** 集成 Redis 进行状态管理，使用 PostgreSQL/MinIO 存储业务数据。
- **大规模检索 (P2):** 集成向量数据库 (FAISS/Chroma) 以支持大规模实体对齐。

## 开源协议

本项目遵循 MIT 开源协议。