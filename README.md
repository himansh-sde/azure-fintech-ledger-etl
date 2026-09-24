# 🚀 Enterprise Fintech Data Pipeline: Azure Medallion Architecture

![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white) ![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white) ![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white) ![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white) ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A9E0?style=for-the-badge&logo=databricks&logoColor=white)

> **Business Objective:** Engineered a secure, scalable, and fully automated data pipeline to ingest streaming Fintech subscription events, track historical dimension changes (SCD Type 2), and aggregate Monthly Recurring Revenue (MRR) for executive BI dashboards.

**[📸 PLACEHOLDER 1: Insert your Validation Report Screenshot here]**
*Above: Custom data observability script output verifying exact record counts, schema integrity, and MRR calculations across all Medallion layers.*

---

## 🏗️ Architecture & Orchestration

This pipeline transforms raw JSON events into business-ready insights using the **Medallion Architecture**, orchestrated entirely via **Databricks Workflows**.

**[📸 PLACEHOLDER 2: Insert screenshot of your Databricks Workflow DAG (the green success graph) here]**

| Layer | Component | Technical Implementation & Purpose |
| :--- | :--- | :--- |
| 🥉 **Bronze** | Databricks Auto Loader | Ingests streaming raw JSON incrementally. Executed as a transient micro-batch (`trigger(availableNow=True)`) to drastically minimize DBU compute costs. |
| 🥈 **Silver** | PySpark Delta Merge | Implements an **SCD Type 2** architecture using the efficient "Null Merge Key" pattern to track historical user upgrades/downgrades without full table scans. |
| 🥇 **Gold** | PySpark Aggregation | Applies business logic to calculate tier-based MRR. Physically sorts data on disk using `OPTIMIZE` and `ZORDER BY` to guarantee sub-second Power BI query performance. |

---

## 🧠 Core Engineering Achievements

This project bypasses basic notebook tutorials to implement the strict standards required in production, zero-trust enterprise environments.

<details>
<summary><b>🛡️ 1. Zero-Trust Security & Credential Management (Click to expand)</b></summary>
<br>
Zero hardcoded passwords. The PySpark cluster authenticates directly to Azure Data Lake Storage (ADLS Gen2) using a Service Principal dynamically fetched from an <b>Azure Key Vault Secret Scope</b>. 

**[📸 PLACEHOLDER 3: Insert screenshot of Azure Key Vault Secrets page here]**
</details>

<details>
<summary><b>⚙️ 2. Idempotency & Eager Evaluation (Click to expand)</b></summary>
<br>
The pipeline handles its own state and can be rerun infinitely without data duplication. Bypassing PySpark's default lazy evaluation, the scripts use eager limits (`.limit(1).count()`) to detect missing metadata. They autonomously decide whether to execute an initial table build or a complex Delta Merge.
</details>

<details>
<summary><b>⏱️ 3. Cost-Optimized Orchestration (Click to expand)</b></summary>
<br>
Scripts are decoupled from interactive notebooks into modular `.py` files. The orchestration relies on automated <b>Databricks Job Clusters</b> that spin up solely for execution and terminate immediately, reducing cloud compute costs by roughly 50% compared to all-purpose clusters.
</details>

<details>
<summary><b>🏗️ 4. Infrastructure as Code (IaC) (Click to expand)</b></summary>
<br>
All Azure resources—Resource Groups, Storage Accounts (with Hierarchical Namespace enabled), Key Vaults, and Databricks Workspaces—were provisioned immutably using <b>Terraform</b>.
</details>

---

## 📂 Repository Structure

```text
├── infrastructure/
│   ├── main.tf                 # Terraform definitions for Azure resources
│   └── variables.tf            # IaC variable configurations
├── src/
│   ├── config/
│   │   └── spark_config.py     # Centralized Key Vault authentication module
│   ├── ingestion/
│   │   └── bronze_loader.py    # Auto Loader micro-batching script
│   ├── processing/
│   │   └── silver_scd2.py      # Idempotent SCD Type 2 merge logic
│   ├── aggregation/
│   │   └── gold_revenue.py     # MRR business logic and Z-Ordering
│   └── validation/
│       └── pipeline_validator.py # Data observability and integrity checks
└── README.md