# 🛍️ End-to-End Retail Data Pipeline (ETL / Data Lakehouse)

> **Proyek Portofolio** — Pipeline Data Engineering end-to-end berbasis dataset e-commerce Brazil Olist.
> Mengimplementasikan Medallion Architecture (Bronze → Silver → Gold), Kimball-style Star Schema, dan dashboard BI interaktif.

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-4.2-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Airflow-2.x-lightblue?logo=apacheairflow)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow?logo=amazons3)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5-black?logo=duckdb)
![Streamlit](https://img.shields.io/badge/Streamlit-1.64-red?logo=streamlit)

---

## 📌 Gambaran Umum

Proyek ini mendemonstrasikan alur kerja Data Engineering end-to-end — dari ingesti CSV mentah hingga dashboard BI interaktif — mengikuti standar industri:

- **Medallion Architecture**: Raw (Bronze) → Cleaned/Modeled (Silver) → Aggregated (Gold)
- **Dimensional Modeling**: Kimball-style Star Schema dengan SCD Type 1 dan SCD Type 2
- **Cloud Storage**: AWS S3 sebagai Data Lake persisten
- **Orkestrasi**: Apache Airflow DAG dengan eksekusi idempoten berbasis tanggal
- **Processing**: PySpark (local mode, terhubung ke S3 via `hadoop-aws`)
- **Serving**: DuckDB sebagai query engine atas Gold Parquet → Streamlit dashboard

**Dataset**: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k+ pesanan, 2016–2018.

---

## 🏗️ Arsitektur

<!-- TODO: Ganti dengan gambar arsitektur yang sudah dibuat -->

![Arsitektur Pipeline](docs/architecture.png)

---

## 🗄️ Data Model

<!-- TODO: Ganti dengan gambar Star Schema yang sudah dibuat -->

![Star Schema](docs/star_schema.png)

---

## 🧰 Tech Stack

| Komponen          | Tool                      | Fungsi                                    |
| ----------------- | ------------------------- | ----------------------------------------- |
| Package Manager   | `uv`                      | Manajemen dependency Python yang cepat    |
| Processing Engine | PySpark 4.2               | Transformasi data terdistribusi           |
| Cloud Storage     | AWS S3                    | Data Lake Medallion Architecture          |
| S3 Connector      | `hadoop-aws 3.5.0`        | Koneksi Spark ↔ S3 via protokol S3A       |
| Orkestrasi        | Apache Airflow 2.x        | Penjadwalan DAG, dependency, dan backfill |
| Airflow Runtime   | Docker Compose (official) | Environment Airflow lokal                 |
| Query Engine      | DuckDB 1.5                | Analitik Parquet lokal yang cepat         |
| BI Dashboard      | Streamlit 1.64            | Dashboard bisnis interaktif               |
| Format Data       | Parquet + Snappy          | Kolumnar, terkompresi, partition-prunable |
| EDA               | JupyterLab                | Profiling data & desain schema            |

---

## 📁 Struktur Direktori

```
project4-aws/
├── airflow/                         # Environment orkestrasi Airflow
│   ├── dags/
│   │   └── daily_retail_etl_dag.py  # DAG ETL utama
│   ├── docker-compose.yaml          # Official Airflow Docker Compose
│   └── .env                         # Environment vars Airflow
├── data/                            # Data lake lokal (cerminan struktur S3)
│   ├── raw/olist/                   # Bronze: 9 file CSV sumber
│   ├── silver/                      # Silver: Star Schema Parquet
│   └── gold/                        # Gold: Metrik bisnis teragregasi
├── src/
│   ├── jobs/                        # PySpark transformation jobs
│   │   ├── build_dim_date.py
│   │   ├── build_dim_product.py
│   │   ├── build_dim_customer.py
│   │   ├── build_dim_seller.py
│   │   ├── build_dim_geolocation.py
│   │   ├── build_fact_orders.py
│   │   ├── build_fact_order_payments.py
│   │   ├── build_fact_order_reviews.py
│   │   └── build_gold_aggregates.py
│   └── utils/
│       └── spark_session.py         # SparkSession factory (lokal + konfigurasi S3A)
├── dashboard/
│   └── app.py                       # Streamlit BI Dashboard
├── notebooks/
│   └── 01_eda_and_profiling.ipynb   # EDA & profiling dataset
├── tests/
│   ├── test_pipeline_jobs.py        # Uji import & struktur modul PySpark
│   ├── test_gold_outputs.py         # Validasi isi tabel Gold
│   └── test_dashboard.py            # Uji skema Gold via DuckDB
├── docs/
│   └── plan.md                      # Dokumen desain & keputusan teknis
├── pyproject.toml                   # Definisi dependency (uv)
└── README.md
```

---

## 📐 Data Model (Silver Layer — Kimball Star Schema)

### Tabel Dimensi & Fakta

| Tabel                 | Sumber CSV                                                             | Grain                         | Strategi SCD                    | Keterangan                                   |
| --------------------- | ---------------------------------------------------------------------- | ----------------------------- | ------------------------------- | -------------------------------------------- |
| `dim_date`            | Generated                                                              | date                          | Static                          | Kalender 2016–2018                           |
| `dim_product`         | `olist_products_dataset.csv` + `product_category_name_translation.csv` | product_id                    | SCD Type 1                      | Termasuk terjemahan kategori EN              |
| `dim_customer`        | `olist_customers_dataset.csv`                                          | customer_id + versi           | SCD Type 2                      | Melacak histori perubahan kota/negara bagian |
| `dim_seller`          | `olist_sellers_dataset.csv`                                            | seller_id                     | SCD Type 1                      | Diperkaya dengan data geolokasi              |
| `dim_geolocation`     | `olist_geolocation_dataset.csv`                                        | zip_code_prefix               | Reference Dim                   | Rata-rata lat/lng per kode pos               |
| `fact_orders`         | `olist_orders_dataset.csv` + `olist_order_items_dataset.csv`           | order_id + order_item_id      | Date-scoped partition overwrite | Grain transaksi inti                         |
| `fact_order_payments` | `olist_order_payments_dataset.csv`                                     | order_id + payment_sequential | Date-scoped partition overwrite | Analitik metode pembayaran                   |
| `fact_order_reviews`  | `olist_order_reviews_dataset.csv`                                      | review_id                     | Date-scoped partition overwrite | Kepuasan pelanggan                           |

### Urutan Dependency Job

```
dim_geolocation → dim_seller ─────────────────────────────┐
dim_date ─────────────────────────────────────────────────┤
dim_product ──────────────────────────────────────────────┤→ fact_orders ──┐
dim_customer ─────────────────────────────────────────────┘                 │
                                                    fact_order_payments ────┤→ gold_aggregates
                                                    fact_order_reviews ─────┘
```

### Gold Layer — Metrik Bisnis

| Tabel                      | Deskripsi                    | Metrik Utama                                      |
| -------------------------- | ---------------------------- | ------------------------------------------------- |
| `daily_sales_summary`      | KPI revenue & pesanan harian | `gross_revenue`, `order_count`, `avg_order_value` |
| `seller_performance_daily` | Revenue per seller harian    | `total_revenue`, `order_count`, `state`, `city`   |
| `category_performance`     | Peringkat kategori produk    | `total_revenue`, `order_count`, `avg_order_value` |
| `daily_review_summary`     | Tren skor ulasan harian      | `avg_review_score`, `review_count`                |

---

## 🚀 Cara Menjalankan Secara Lokal

### Prasyarat

- Python 3.13+
- Java 11 atau 17 (diperlukan oleh PySpark)
- [`uv`](https://docs.astral.sh/uv/) terinstal
- Docker Desktop (untuk Airflow)

### 1. Instal dependency

```bash
git clone <repo-url>
cd project4-aws
uv sync
```

### 2. Unduh dataset

Letakkan 9 file CSV Olist ke dalam `data/raw/olist/`:

```
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

> Unduh dari [Kaggle: Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### 3. Jalankan PySpark job (mode lokal)

Setiap job menerima argumen `--execution-date`, `--raw-path`, dan `--silver-path`:

```bash
uv run python3 src/jobs/build_dim_geolocation.py \
  --execution-date 2017-10-02 \
  --raw-path data/raw/olist \
  --silver-path data/silver
```

Jalankan semua job sesuai urutan dependency untuk satu tanggal eksekusi:

```bash
DATE=2017-10-02

uv run python3 src/jobs/build_dim_geolocation.py --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_dim_date.py        --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_dim_product.py     --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_dim_customer.py    --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_dim_seller.py      --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_fact_orders.py     --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_fact_order_payments.py --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_fact_order_reviews.py  --execution-date $DATE --raw-path data/raw/olist --silver-path data/silver
uv run python3 src/jobs/build_gold_aggregates.py --execution-date $DATE --silver-path data/silver --gold-path data/gold
```

### 4. Jalankan dashboard

```bash
uv run streamlit run dashboard/app.py
```

Buka [http://localhost:8501](http://localhost:8501) — dashboard membaca Gold Parquet dari `data/gold/`.

---

## 🔄 Orkestrasi dengan Airflow (Lokal)

Airflow mengatur urutan job, retry otomatis, dan backfill historis secara otomatis.

### Jalankan Airflow

```bash
docker context use desktop-linux
cd airflow
docker compose up -d
```

Buka [http://localhost:8080](http://localhost:8080) — login: `airflow` / `airflow`

### Trigger DAG secara manual

1. Aktifkan (unpause) DAG `daily_retail_etl`
2. Klik **Trigger DAG w/ config**
3. Set logical date ke tanggal yang memiliki data, misalnya `2017-10-02`
4. Airflow menjalankan semua job sesuai urutan dependency secara otomatis

### Backfill data historis

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill daily_retail_etl \
  --start-date 2016-09-01 \
  --end-date 2018-10-17
```

### Cek log

```bash
docker compose logs --tail=80 airflow-scheduler
```

---

## ☁️ AWS Runbook

### Prasyarat

- AWS CLI terkonfigurasi dengan profile (misalnya `project4-aws`)
- Izin IAM: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` pada bucket target
- Java 11/17 untuk koneksi Spark S3A

### Setup kredensial

```bash
export AWS_PROFILE=project4-aws
export AWS_REGION=ap-southeast-1
eval "$(aws configure export-credentials --profile project4-aws --format env)"
```

### Jalankan job terhadap S3

```bash
BUCKET=s3a://dataengineer-project-aws-olist-retail

PYSPARK_SUBMIT_ARGS="\
--packages org.apache.hadoop:hadoop-aws:3.5.0 \
--conf spark.hadoop.fs.s3a.endpoint=s3.ap-southeast-1.amazonaws.com \
--conf spark.hadoop.fs.s3a.endpoint.region=ap-southeast-1 \
--conf spark.hadoop.fs.s3a.path.style.access=false \
--conf spark.hadoop.fs.s3a.input.stream.type=classic \
pyspark-shell" \
uv run python3 src/jobs/build_dim_geolocation.py \
  --execution-date 2017-10-02 \
  --raw-path $BUCKET/raw/olist \
  --silver-path $BUCKET/silver
```

### Verifikasi output di S3

```bash
aws s3 ls s3://dataengineer-project-aws-olist-retail/silver/ --recursive --human-readable
aws s3 ls s3://dataengineer-project-aws-olist-retail/gold/   --recursive --human-readable
```

### Struktur bucket S3

```
s3://dataengineer-project-aws-olist-retail/
├── raw/olist/                  ← 9 CSV sumber (Bronze immutable)
├── silver/
│   ├── dim_date/
│   ├── dim_product/
│   ├── dim_customer/
│   ├── dim_seller/
│   ├── dim_geolocation/
│   ├── fact_orders/
│   ├── fact_order_payments/
│   └── fact_order_reviews/
└── gold/
    ├── daily_sales_summary/
    ├── seller_performance_daily/
    ├── category_performance/
    └── daily_review_summary/
```

> ⚠️ Kredensial AWS **tidak pernah** disimpan di source code, file `.env` yang di-commit ke git, atau dokumen ini. Gunakan AWS CLI profile untuk pengembangan lokal dan IAM role untuk compute AWS.

---

## 🧪 Menjalankan Tests

```bash
# Semua test
uv run python3 -m pytest tests/ -v

# Unit test: import job & struktur modul
uv run python3 -m pytest tests/test_pipeline_jobs.py -v

# Integration test: validasi isi tabel Gold
uv run python3 -m pytest tests/test_gold_outputs.py -v

# Test serving layer: uji skema Gold via DuckDB
uv run python3 -m pytest tests/test_dashboard.py -v
```

---

## ⚠️ Keterbatasan yang Diketahui

| Keterbatasan                      | Detail                                                                                                                                                |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Spark lokal**                   | PySpark berjalan dalam mode `local[*]`. Tidak ada EMR atau Glue cluster — cukup untuk portofolio namun bukan throughput skala produksi.               |
| **Fact table berbasis tanggal**   | `fact_orders` memproses satu tanggal eksekusi sekaligus. Data historis lengkap memerlukan backfill untuk semua tanggal dalam dataset.                 |
| **Gold rows kecil per satu run**  | Menjalankan untuk satu tanggal menghasilkan ringkasan Gold 1 baris. Data Gold penuh memerlukan backfill seluruh dataset.                              |
| **Dashboard membaca Gold lokal**  | `dashboard/app.py` secara default membaca dari `data/gold/`. Koneksi langsung ke Gold S3 memerlukan ekstensi `httpfs` DuckDB + credential AWS.        |
| **Tidak ada streaming real-time** | Pipeline hanya batch. CDC, Kafka, dan streaming di luar scope.                                                                                        |
| **Airflow lokal, bukan managed**  | Airflow berjalan via Docker Compose lokal. MWAA (Managed Airflow) di luar scope.                                                                      |
| **Dataset statis**                | Olist adalah dataset historis (2016–2018), bukan feed langsung. `schedule=None` pada Airflow DAG; aktivasi jadwal memerlukan sumber data inkremental. |

---

## 📄 Lisensi

Proyek ini dibuat untuk tujuan portofolio dan pembelajaran. Kredit dataset: [Olist](https://olist.com/) via Kaggle.
