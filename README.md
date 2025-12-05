# Pipedrive CRM Sales Funnel Analytics

This project implements a **PostgreSQL and dbt-based analytics pipeline** designed to transform raw Pipedrive CRM event data into a robust, realiable, monthly aggregated sales funnel mart. The solution prioritizes data quality, maintainability, and query efficiency addressing the project requirements.

---

### 🛠️ Project Setup and Configuration Summary

This project was established with a production mindset:

* **Structure:** All core dbt files (`models/`, `macros/`, `dbt_project.yml`) are consolidated into a single **`dbt/`** directory.
* **Layering:** The `models/` directory is structured into **`staging/` (Views)**, **`intermediate/` (Views)**, and **`marts/` (Tables)** to enforce logical separation and cost-efficiency.
* **Security:** Essential files (`profiles.yml`, `.user.yml`) are correctly excluded in the `.gitignore` file to protect credentials and local overrides.
* **Environments:** The `profiles.yml` was updated to define separate **`dev`** and **`prod`** environments, enabling clear CI/CD deployment paths.



## 🧭 Core Architecture and Data Flow

The architecture follows the **Medallion Pattern (Staging → Intermediate → Marts)**.

### Separation of Concerns

* **Raw Data:** All raw data resides in the `postgres_public` schema in PostgreSQL.
* **Staging Layer (`staging/`):** Acts as the cleaning layer. It renames columns, casts IDs to **BIGINT** (for scale), and ensures basic type check validation, materialized as view for low-cost.
* **Intermediate Layer (`intermediate/`):** Solves the core business logic. This is where we convert the chronological event log into a continuous stage timeline using **PostgreSQL Window Functions LEAD()** and flatten complex JSON metadata.
* **Marts Layer (`marts/`):** The final aggregated output, ready for the BI tool.

---

## 💡 Key Assumptions & Design Decisions

* **1. Primary Data Source:** Assuming the raw data resides in the PostgreSQL schema named **`postgres_public`**.
* **2. Data Loading Column (`_loaded_at`):** This model critically relies on the presence of the **`_loaded_at`** timestamp column in all source tables for incremental logic and freshness monitoring in DBT.
* **3. ID Casting:** All primary keys (PKs) and foreign keys (FKs) are cast to **BIGINT** in the staging layer to ensure future scalability.
* **4. Materialization Strategy:**
    * `staging/` and `intermediate/`: Materialized as **Views** (cost efficiency).
    * `marts/`: Materialized as **Incremental Tables** (performance and scale).

---

### Data Model ERD

![Pipedrive Funnel Model ERD](assets/erd.png)

## 🛡️ Data Quality Strategy

Data quality is enforced using a multi-layered testing strategy that ensures correctness at low cost.

### Testing Strategy

| Layer | Test Focus | Key Test (Gatekeeper) | Rationale |
| :--- | :--- | :--- | :--- |
| **Source/Raw** | **Health & Integrity** | **Freshness Check** (`dbt source freshness`) and **Volume Anomaly Check** (`dbt-expectations`). | Catches external pipeline failures (e.g., zero-row loads). |
| **Staging/Intermediate** | **Logic Integrity** | **Window Function Validation:** Custom test ensuring `stage_exit_at` and `stage_enter_at` validity | Ensures the complex `LEAD()` logic is correct. |
| **Marts (Final Report)** | **Uniqueness & Domain** | **Incremental PK Check:** Runs a partial uniqueness check on `month`, `kpi_name` using a `WHERE` clause **to limit scanning to the current/last month.** | **Scalability:** Prevents scanning the entire multi-year mart every day(cost effectiveness) |

---

## 📈 Answering the Business Questions

The final reporting model, `sales_funnel_monthly`, is structured to directly answer the assessment's key performance indicators (KPIs) and required funnel steps by unifying complex metrics.

* **Funnel Steps (1.0, 2.0, 4.0, etc.):** Derived by counting distinct deals entering a stage from the `int_deal_stage_history` model.
* **Activity Steps (2.1, 3.1):** Derived by counting completed calls from the `int_activity_monthly` model.

| Business Question / KPI | Metric Source (Model.Column) |
| :--- | :--- |
| **Step 1: Lead Generation** | `mart_sales_funnel_monthly` |
| **Step 2.1: Sales Call 1** |`mart_sales_funnel_monthly` |
| **Total Deals Moving** | `mart_sales_funnel_monthly` |
| **Conversion Rate (Future)** | Calculated in the BI tool using `(Deals_Entering_Stage_N / Deals_Entering_Stage_{N-1})`. |

---

## 🛠️ Quick Start Guide

To initialize and test the project:

1.  **Clone the Repo** and navigate to the project directory.
2.  **Install:** `pip install dbt-core dbt-postgres`
3.  **Setup DB:** Ensure your local PostgreSQL instance is running and raw CSV data is loaded into the **`raw`** schema.
4.  **Initial Load & Build:**
    ```bash
    dbt deps
    dbt build 
    ```
5.  **View Lineage:** `dbt docs generate && dbt docs serve`