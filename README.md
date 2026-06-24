\# 🛒 E-Commerce Data Pipeline



!\[Dashboard] --> (dashboard/Ecommerce\_Dashboard.png)



\## 📌 Project Overview

An end-to-end data engineering pipeline that fetches real-time e-commerce data from a REST API, processes it using Apache Spark, orchestrates workflows with Apache Airflow, stores data on AWS S3 and PostgreSQL, and visualizes insights in Power BI.



\---



\## 🏗️ Architecture

```

Fake Store API → Apache Airflow → Python (Clean/KPIs) → PySpark (Transform) → AWS S3 (Data Lake) → PostgreSQL (Data Warehouse) → Power BI (Dashboard)

```



\---



\## 🛠️ Tech Stack



| Technology | Purpose |

|-----------|---------|

| Python | API ingestion, data cleaning, KPI calculation |

| Apache Spark (PySpark) | Large scale data transformation, Parquet files |

| Apache Airflow | Pipeline orchestration \& daily scheduling |

| AWS S3 | Raw + processed data lake storage |

| PostgreSQL | Data warehouse with star schema design |

| SQL | Aggregations, joins, analytical queries |

| Docker | Containerized full stack infrastructure |

| Power BI | Business intelligence dashboard |



\---



\## 📊 Pipeline Tasks (9 Steps)



| Step | Task | Description |

|------|------|-------------|

| 1 | fetch\_api\_data | Fetches products, carts, users from Fake Store API |

| 2 | upload\_raw\_to\_s3 | Uploads raw JSON files to AWS S3 raw layer |

| 3 | validate\_raw\_data | Validates data — checks nulls and empty records |

| 4 | clean\_and\_enrich\_data | Cleans titles, adds price buckets, discount suggestions |

| 5 | calculate\_kpis | Calculates revenue, top category, total units sold |

| 6 | run\_spark\_transform | PySpark transforms data, saves Parquet files to S3 |

| 7 | load\_to\_postgres | Loads dimension and fact tables into PostgreSQL |

| 8 | run\_aggregations | Runs SQL aggregations into summary table |

| 9 | generate\_daily\_report | Generates JSON report and uploads to S3 |



\---



\## 🗄️ Database Schema (Star Schema)



```

dim\_products ──┐

&#x20;              ├──▶ fact\_orders

dim\_users    ──┘



agg\_category\_sales  (daily summary table)

```



\### Tables

\- \*\*dim\_products\*\* — product\_id, title, category, price, price\_bucket, rating\_score, high\_rated

\- \*\*dim\_users\*\* — user\_id, username, email, city

\- \*\*fact\_orders\*\* — order\_id, cart\_id, user\_id, product\_id, quantity, unit\_price, total\_amount, order\_date

\- \*\*agg\_category\_sales\*\* — category, total\_orders, total\_revenue, avg\_order\_value, report\_date



\---



\## 📈 Power BI Dashboard Visuals



| Visual | Type | Insight |

|--------|------|---------|

| Total Revenue | Card | $4,691.27 total revenue |

| Total Orders | Card | 14 total orders |

| Revenue by Category | Bar Chart | Men's clothing leads at $2.6K |

| Products by Price Bucket | Pie Chart | 36% products are Luxury |

| High Rated Products | Donut Chart | 35% products are high rated |

| Top Products by Price | Bar Chart | Top 4 most expensive products |

| Orders per Customer | Bar Chart | johnd is top customer |

| Avg Order Value by Category | Treemap | Jewelery has highest avg order value |

| Category Summary | Table | Full breakdown of all 4 categories |



\---



\## ☁️ AWS S3 Structure



```

s3://ecommerce-pipeline-abhay/

├── raw/

│   ├── products.json

│   ├── carts.json

│   └── users.json

├── processed/

│   ├── products/

│   │   └── \*.parquet

│   └── kpis/

│       └── kpis.json

└── reports/

&#x20;   └── 2026/

&#x20;       └── 06/

&#x20;           └── 23/

&#x20;               └── daily\_report.json

```



\---



\## 🐳 Docker Services



| Container | Image | Port |

|-----------|-------|------|

| airflow | apache/airflow:2.7.0 | 8081 |

| spark-master | apache/spark:3.5.0 | 8080, 7077 |

| spark-worker | apache/spark:3.5.0 | — |

| postgres-db | postgres:15 | 5432 |



\---



\## 🚀 How to Run



\### Prerequisites

\- Docker Desktop installed and running

\- AWS CLI configured with valid credentials

\- Python 3.8+

\- Power BI Desktop



\### Steps



\*\*1. Clone the repo:\*\*

```bash

git clone https://github.com/yourusername/ecommerce-pipeline.git

cd ecommerce-pipeline

```



\*\*2. Create `.env` file from template:\*\*

```bash

cp .env.example .env

```

Then fill in your actual AWS credentials in `.env`



\*\*3. Create S3 bucket:\*\*

```bash

aws s3 mb s3://(Enter your s3 bucket name) --region (Enter your region)

```



\*\*4. Start all Docker containers:\*\*

```bash

docker-compose up -d

```



\*\*5. Install Python dependencies in Airflow:\*\*

```bash

docker exec -it airflow pip install pyspark boto3 psycopg2-binary

```



\*\*6. Create PostgreSQL tables:\*\*

```bash

docker exec -it postgres-db psql -U pipeline\_user -d ecommerce\_db -f /docker-entrypoint-initdb.d/init.sql

```



\*\*7. Open Airflow UI and trigger pipeline:\*\*

```

URL:      http://localhost:8081

Username: admin

Password: admin

```

\- Find `ecommerce\_pipeline` DAG

\- Toggle ON

\- Click ▶️ Trigger DAG

\- Watch all 9 tasks turn green ✅



\*\*8. Verify data in PostgreSQL:\*\*

```bash

docker exec -it postgres-db psql -U pipeline\_user -d ecommerce\_db -c "SELECT \* FROM agg\_category\_sales;"

```



\*\*9. Verify data in S3:\*\*

```bash

aws s3 ls s3://ecommerce-pipeline-yourname/ --recursive

```



\*\*10. Open Power BI Dashboard:\*\*

\- Open `dashboard/Ecommerce\_Dashboard.pbix`

\- Refresh data



\---



\## 📁 Project Structure



```

ecommerce-pipeline/

├── dags/

│   └── ecommerce\_dag.py          ← Airflow DAG (9 tasks)

├── scripts/

│   └── spark\_transform.py        ← PySpark transformation

├── sql/

│   └── init.sql                  ← PostgreSQL star schema

├── dashboard/

│   ├── Ecommerce\_Dashboard.pbix  ← Power BI file

│   └── Ecommerce\_Dashboard.png   ← Dashboard screenshot

├── docker-compose.yml            ← 4 container setup

├── .env                          ← AWS credentials (not pushed)

├── .gitignore

└── README.md

```



\---



\## 📊 Key Results



\- ✅ Ingested \*\*20 products\*\*, \*\*10 users\*\*, \*\*14 orders\*\* from live API

\- ✅ Processed and stored \*\*Parquet files\*\* on AWS S3

\- ✅ Built \*\*star schema\*\* with 3 dimension/fact tables in PostgreSQL

\- ✅ Automated \*\*daily pipeline\*\* scheduled at 6AM via Airflow

\- ✅ Generated \*\*9 Power BI visuals\*\* for business insights



\---



\## 👨‍💻 Author



\*\*Abhay\*\*

\- 🔗 LinkedIn: \[Add your LinkedIn URL]

\- 🐙 GitHub: \[Add your GitHub URL]



\---



\## 📄 License

MIT License

