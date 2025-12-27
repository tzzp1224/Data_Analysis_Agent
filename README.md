# Agentic Finance: Enterprise Data Analyst Agent

[![Language](https://img.shields.io/badge/Lang-简体中文-red.svg)](README_CN.md)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-green.svg)](https://www.python.org/)

**Agentic Finance** is an autonomous data analysis system designed for enterprise financial scenarios. Leveraging **LangGraph** for orchestration and **Google Gemini** for reasoning, it automates the workflow of data ingestion, schema inference, entity alignment, and financial reconciliation.

The system implements a **Supervisor-Worker architecture**, featuring self-healing code execution and comprehensive audit logging.

## System Architecture

The core logic is driven by a state machine that orchestrates interactions between the Supervisor (decision maker) and the Python Worker (executor).

[![](https://mermaid.ink/img/pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaThsFGi4p2809Hyv2_f84WA8nviBHwTXHbrD8KA7HkwmQTCaeL4_GAWjQ4e-N0X73WA87LeWd_gHOXceUQ?type=png)](https://mermaid.live/edit#pako:eNp1Ul1v2jAU_SuWnymQlI-Qh0qIsq4Ta1FDV2mhD15ySaIlNvLHVkr477uOoc0k8IPtc3Xuuedee08TkQINaSbZNier2zUnuJ4VyNhupEciLYFVZaHJ8_0rubq6qb-uVsvet-jxoSbT5X38hSmNJ4lA_gH56hRswHIjUKoQvCYvQv7elOJvvGA8u2uqnULHFLcr88t5WdNpBlyTR5nkoLRkGnXW1LHsiswWKxZKyPjzSh6wH2fzSRgN0lXGdpY7nQt-RI52Tuu_1FnOdHwHHCQrG9BKOQpZOjLIDAVrMn-DxGh0ZOEHamWdQq4MKFPq3lxK8eHzAjUySYKjrFtOHRN4emF8KyHKgmdkwXYoS88LkyXjKVOxO8icZwWHC36JVVTxzCgtKgdazAY3tIUXT02KH2YhsgzkeY4f_4DECn9nGh_4Aus6jiomNXmCRPCkKE_WsGvawU9bpDTU0kCHViArZiHdW8qa6hwqWNMQrylsGA7azuCAaVvGfwpRnTKlMFlOww0rFSKzTZmG24LhGD8pWA_kTBiuaThsFGi4p2809Hyv2_f84WA8nviBHwTXHbrD8KA7HkwmQTCaeL4_GAWjQ4e-N0X73WA87LeWd_gHOXceUQ)

## Key Capabilities

The system categorizes capabilities into four levels (L1-L4):

### L1: Intelligent Hygiene

- **Schema Inference:** Automatically detects header rows and sheet names using LLM-based inspection.
- **Data Cleaning:** Identifies and handles duplicates, null values, and outliers.
- **Audit Logging:** Tracks all data modifications (drops, fills, exclusions) in a dedicated audit log for compliance.

### L2: Semantic Entity Alignment

- **Problem:** Resolves inconsistencies in entity names across datasets (e.g., "ByteDance" vs. "字节跳动").
- **Solution:** Hybrid matching approach combining **RapidFuzz** (string similarity) and **Sentence-Transformers** (vector embeddings), validated by an LLM Judge.

### L3: Financial Reconciliation

- **Tolerance Matching:** Supports monetary reconciliation with configurable tolerance thresholds (e.g., ignoring differences < 0.01).
- **Many-to-One Aggregation:** Automatically handles scenarios where multiple system records correspond to a single bank transaction.
- **Status Classification:** Categorizes records into "Matched", "Tolerance Matched", or "Unilateral" (System/Bank only).

### L4: Interactive Visualization

- Generates interactive charts (Plotly) based on natural language queries.
- Provides automated insights and trend analysis alongside visual outputs.

## Installation

### Prerequisites

- Python 3.9+
- Google Gemini API Key

### Setup

1. **Clone the repository**

   Bash

   ```
   git clone [https://github.com/your-username/agentic-finance.git](https://github.com/your-username/agentic-finance.git)
   cd agentic-finance
   ```

2. **Install dependencies**

   Bash

   ```
   pip install -r requirements.txt
   ```

3. **Configure Environment** Create a `.env` file in the root directory:

   Bash

   ```
   GOOGLE_API_KEY=your_api_key_here
   ```

## Usage

The system requires both the backend API and frontend UI to be running.

**1. Start the Backend (FastAPI)**

Bash

```
uvicorn app.server:app --reload --host 0.0.0.0 --port 8000
```

**2. Start the Frontend (Streamlit)**

Bash

```
streamlit run app/ui.py
```

Access the web interface at `http://localhost:8501`.

## Roadmap

- **Security (P0):** Implement Docker Sandbox for isolated code execution (replacing local `exec()`).
- **Persistence (P1):** Integration with Redis for state management and PostgreSQL/MinIO for file storage.
- **Performance (P2):** Integration with vector databases (FAISS/Chroma) for large-scale entity alignment.

## License

This project is licensed under the MIT License.