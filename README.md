# 🚀 Vibe Code Examples — Prompt-Driven Data Pipelines

A collection of **Apache Airflow data pipeline** examples built using the **"Vibe Coding"** approach: write prompts first, let AI generate the code.

## 📂 Project Structure

```
Vibe-Code-Examples/
├── covid-19-stats-pipeline/      # COVID-19 global statistics ETL
│   ├── prompts/                  # AI prompts (.prompt.txt)
│   └── code/                    # Generated Python code (.py)
│
├── nyc-taxi-trips-pipeline/      # NYC Yellow Taxi trip analysis
│   ├── prompts/
│   └── code/
│
└── world-happiness-pipeline/     # World Happiness Report analysis
    ├── prompts/
    └── code/
```

## 🎯 How It Works

Each pipeline follows the same 4-step ETL pattern:

| Step | Function | Description |
|------|----------|-------------|
| 1 | `ingest_*` | Download raw data from public sources |
| 2 | `clean_*` | Validate and clean the data |
| 3 | `transform_*` | Compute derived features |
| 4 | `load_*_model` | Load into a MySQL star-schema |

## 🔄 Workflow: Edit Prompt → Regenerate Code

1. **Edit** a `.prompt.txt` file in `prompts/`
2. **Copy** the prompt to your AI assistant (e.g., ChatGPT, Gemini, Claude)
3. **Paste** the generated code into the corresponding `.py` file in `code/`
4. **Commit** both the prompt and code changes to Git

This way, Git tracks **why** (prompt) and **what** (code) changed together.

## 📊 Pipelines

### 🦠 COVID-19 Stats Pipeline
- **Source**: [Our World in Data](https://covid.ourworldindata.org/data/owid-covid-data.csv)
- **Features**: 7-day rolling averages, cases/deaths per million, case fatality rate
- **Schema**: Dimensional model with country/date dimensions

### 🚕 NYC Taxi Trips Pipeline
- **Source**: [NYC Open Data](https://data.cityofnewyork.us/api/views/4c9m-wp7t/rows.csv)
- **Features**: Trip duration, speed, fare per mile, temporal features
- **Schema**: Star schema with time/payment dimensions + fact_trips

### 😊 World Happiness Pipeline
- **Source**: [World Happiness Report](https://github.com/rashida048/Datasets) (2019-2020)
- **Features**: Normalized cross-year metrics, IQR outlier detection, per-year ranking
- **Schema**: Star schema with country/year dimensions + fact_happiness

## ⚙️ Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- MySQL database
- Required Python packages: `pandas`, `requests`, `sqlalchemy`, `pymysql`
