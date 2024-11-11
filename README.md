# Data Extraction Pipeline using PySpark, Airflow, PostgreSQL, and HDFS

This project extracts data from a PostgreSQL database and stores it on HDFS in Parquet format. The pipeline is orchestrated with Apache Airflow and uses PySpark for data processing.

## Table of Contents
- [Requirements](#requirements)
- [Setup](#setup)
<!-- - [Project Structure](#project-structure) -->
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)

## Requirements
- Python 3.8+
- Virtual environment (recommended)
- Set up Hadoop hdfs in WSL2
- Set up Spark in WSL2
- Docker PostgreSQL (optional for HDFS )
- Download Jar Driver for PostgreSQL -> move and save them at jars folder in Spark

## Setup

1. **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd <repository_folder>
    ```

2. **Create a virtual environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up environment variables** for PostgreSQL and HDFS in `.env`:
    ```ini
    # .env
    POSTGRES_HOST=<your_postgres_host>
    POSTGRES_PORT=<your_postgres_port>
    POSTGRES_DB=<your_database_name>
    POSTGRES_USER=<your_username>
    POSTGRES_PASSWORD=<your_password>
    HDFS_NAMENODE_HOST=<your_hdfs_namenode_host>
    HDFS_PORT=<your_hdfs_port>
    ```

5. **Configure Airflow** (if it’s not already set up):
    - Initialize Airflow database:
      ```bash
      airflow db init
      ```
    - Start the Airflow web server:
      ```bash
      airflow webserver --port 8080
      ```
    - Start the Airflow scheduler in a separate terminal:
      ```bash
      airflow scheduler
      ```

<!-- ## Project Structure

project_folder/ 
├── dags/ # Folder containing Airflow DAGs 
├── scripts/ # PySpark scripts for data processing 
│ └── extract_data.py 
├── .env # Environment variables 
├── README.md 
└── requirements.txt # Python dependencies -->

## Configuration

Update the `.env` file with the necessary database and HDFS configurations.

## Running the Pipeline

1. **Activate the virtual environment**:
   ```bash
   source venv/bin/activate  # On Windows: venv\Scripts\activate
2. **Configure Airflow**
    ```bash
    airflow db init
    airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
3. **Run Airflow and HDFS**
    - Airflow:
    ```bash
    airflow scheduler &
    airflow webserver --port 8080

    - HDFS:
    ```bash
    Start-all.sh
