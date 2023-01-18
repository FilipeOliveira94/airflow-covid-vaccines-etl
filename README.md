# Consuming Brazil's COVID API with an ETL pipeline into three different destinations
## Description
This project aims to ingest data from Brazil's COVID vaccination database, which is [provided by openDataSUS](https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao) using Apache Airflow as the data pipeline orchestrator.

The dataset is then processed, a subset of columns is selected and it's loaded into three different destinations:
- An Amazon DynamoDB table
- An Amazon RDS PostgreSQL database
- A .parquet oriented filesystem on local storage, with partitions by year, month and day

Data is also encoded for better storage compression, and the mappings can be further saved in other tables for decoding.

## Instructions for deploy:
- [Install Docker and Docker-Compose](https://docs.docker.com/get-started/)
(run it preferably on Linux or WSL)

In the main folder, run:

`docker compose up airflow-init`

`docker-compose up`

This will start the apache airflow service and webserver, which can now be accessed on <http://localhost:8080/>

Log in using default username and password airflow:airflow and you're ready to go.

[Reference for deploying airflow on docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
