# Airflow ETL Pipeline

### Project Summary
Create an ETL pipeline using Airflow as the scheduler grab book list from `https://www.amazon.com/s?k=machine+learning+interview` and store the unique book list to postgres database including price, author, price, rating

## Tech Stack
- Python3
- Postgres
- Docker
- Airflow

## What is ETL?
ETL stands for Extract, Transform and Load.

## What is Airflow?
Apache Airflow is an open-source platform for orchestrating and scheduling complex workflows, particularly ETL pipelines and data processes. 

## What is DAG?
DAG (Directed Acyclic Graph) in Airflow is core structure defining a workflow, a DAG organized the tasks in a sequnce and in one direction so that each task won't depend on itself directly or indirectly. Each DAG contains Tasks, Operators and Dependencies. Tasks as units of work, Operators as Task Types like PythonOperator, BashOperator whereas Dependencies defining the execution order.

## ETL Flow
- Step 1: Create Books Table (PostgresOperator) 
- Step 1: Extract data from Amazon via Web Scraping (PostgresOperator) 
- Step 2: Transform data to book table for storing (PostgresOperator) 
- Step 3: Load the data to Postgres SQL

## Step by Step Guide on creating Airflow pipeline
1. Open Visual Studio and Create a Folder
2. Run below command for grabbing docker compose file
   ```sh
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.2/docker-compose.yaml'
   ```
3. Add below content on pg_admin so that we can create server and database
   ```yml
    services:  
      pgadmin:
      container_name: pgadmin4_container
      image: dpage/pgadmin4
      restart: always
      environment:
      PGADMIN_DEFAULT_EMAIL: admin@admin.com
      PGADMIN_DEFAULT_PASSWORD: root
      ports:
       - "5050:80"
   ```
   
4. Run below commands to set the right airflow user.
   ```sh
     mkdir -p ./dags ./logs ./plugins ./config
     echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
5. Create first user account via below command
```sh
docker compose up airflow-init
```

6. Run the docker compose
```sh
docker compose up
```

7. Get IP Address of the database
Get IPAddress of Database
### Unix OS
```sh
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker ps -q --filter "ancestor=postgres:13")
```
### Windows OS
```bat
FOR /F "tokens=*" %%i IN ('docker ps -q --filter "ancestor=postgres:13"') DO (
    docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" %%i
)
```
8. Set data in pgAdmin Web UI
- Open localhost:5050
- Username: `admin@admin.com`, password `root`
- Create server
Go to General Tab: Name: `ps_db`
- Set connection parameters
Go to Connection Tab:
Host name/address = <IP Address in step 7>
Port: 5432
Username: `airflow`
Password: `airflow`
- create database called `amazon-books`

9. Set DB Connection string in airflow
Go to `Admin` Tab, add a new connection.
Connection Id: `books_connection`
Connection Type: `Postgres`
Description: `Postgres Connection`
Host: <IP Address in step 7>
Database: `amazon-books`

10. Set Variable in airflow
Go to `Admin` Tab,  add a new variable
Variable: key = `amazon_book_search_term`, val: `<Your preferred term>`

11. Run the job
Go to `DAGs` Tab, in `Search DAGS`, search `fetch_and_store_amazon_books`
Hit Icon for running the job

12. (Optional) Clean up Environment
```sh
docker compose down --volumes --remove-orphansd
```
