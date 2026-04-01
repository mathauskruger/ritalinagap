# RitalinaGap: Healthcare Data Pipeline & Pharmacoeconomic Audit 🧠💊

**RitalinaGap** is a data engineering project designed to identify discrepancies between the consumption of Methylphenidate (Ritalin) and officially recorded ADHD diagnoses (ICD-10 F90) within the Brazilian healthcare system.

Built as a decision-support tool for **Health Insurance Operators and Self-Management Plans**, it focuses on reducing "sinistralidade" (loss ratio) and ensuring rational resource allocation.

---

## 🚀 The Business Value

In the healthcare sector, a surge in high-cost medication dispensing without clinical backing leads to:
1. **Unmanaged Loss Ratio:** Excessive spending on off-label or recreational use.
2. **Audit Opportunities:** Identifying specific regions or providers where drug dispensing outpaces diagnoses.
3. **Clinical Risk:** Preventing health complications from unmonitored stimulant use, which leads to secondary ER costs.

---

## 🛠️ Tech Stack

* **Data Engineering:** Python (Pandas) for ETL from **ANVISA (SNGPC)** and **DataSUS (SIA)**.
* **Database:** **PostgreSQL** for structured storage and analytical querying.
* **Environment:** **Docker** orchestration for database and Apache Superset.
* **Visualization:** **Apache Superset** for heavy analytics and **Streamlit** for AI-driven narratives.
* **AI Integration:** **OpenRouter (GPT-4o)** for automated interpretation of pharmaceutical gaps.

---

## 📂 Project Structure

```text
├── dashboard/        # Streamlit app for AI-powered insights
├── data/             # Processed datasets (CSV/JSON)
├── pipeline/         # ETL Scripts (Ingest, Clean, Validate, Load)
├── sql/              # Database Schema and Analytical Queries (Ouro Puro)
├── docker-compose.yml # Infrastructure as Code
└── requirements.txt  # Project dependencies