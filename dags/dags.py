# dag -directed acyclic graph

#tasks : 1. create_table 2. 

#operators 

#hooks 

#dependencies
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dags
from airflow.models import Variable
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import os
import random

from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# step 0: define db connection name and use AirFlow Variable
db_connection = 'books_connection'
job_id = "fetch_and_store_amazon_books"
search_term = Variable.get("amazon_book_search_term", default_var="machine learning interview")
# step 1: define args for the airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def load_sql(file_name):
    """Utility function to load SQL from a file with an absolute path."""
    dag_directory = os.path.dirname(__file__)  # Directory of the current DAG file
    sql_path = os.path.join(dag_directory, "sql", file_name)
    with open(sql_path, "r") as file:
        return file.read()

# Load SQL queries from files
create_table_sql = load_sql("create_books_table.sql")
create_partition_sql = load_sql("create_partition.sql")
insert_data_sql = load_sql("insert_books_data.sql")

# step 2: define the dag with the default args
dag = DAG(
    job_id,
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule=timedelta(days=1),
    # Ensures templating within the DAG enables complex structures, like dictionary parameter passing, in Airflow 2.x.
    render_template_as_native_obj=True  # Enables complex templating
)

# step 3: Extract and Transform Amazon Book Data
user_agents = [
    # MacOS with Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    
    # Windows with Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    
    # Windows with Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    
    # Linux with Chrome
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    
    # Linux with Firefox
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    
    # iOS Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    
    # Android Chrome
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36",
    
    # Android Firefox
    "Mozilla/5.0 (Android 11; Mobile; LG-M255; rv:92.0) Gecko/92.0 Firefox/92.0",
    "Mozilla/5.0 (Android 10; Mobile; SM-A305F; rv:81.0) Gecko/81.0 Firefox/81.0",
    
    # Older browsers (for variation)
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/61.0"
]
accept_languages = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.8",
    "en-CA,en;q=0.8",
    "en-AU,en;q=0.7",
    "fr-FR,fr;q=0.8,en-US;q=0.5",
    "es-ES,es;q=0.8,en-US;q=0.5",
    "de-DE,de;q=0.8,en-US;q=0.5",
    "it-IT,it;q=0.8,en-US;q=0.5",
    "pt-BR,pt;q=0.8,en-US;q=0.5",
    "ja-JP,ja;q=0.8,en-US;q=0.5",
    "ko-KR,ko;q=0.8,en-US;q=0.5",
    "zh-CN,zh;q=0.8,en-US;q=0.5",
    "nl-NL,nl;q=0.8,en-US;q=0.5",
    "sv-SE,sv;q=0.8,en-US;q=0.5",
    "no-NO,no;q=0.8,en-US;q=0.5",
    "da-DK,da;q=0.8,en-US;q=0.5",
    "fi-FI,fi;q=0.8,en-US;q=0.5",
    "ru-RU,ru;q=0.8,en-US;q=0.5"
]

# extract amazon books from the website
def extract_amazon_books(num_books, search_term):
    headers = {
"Referer": "https://www.amazon.com/",
    "Sec-Ch-Ua": "\"Chromium\";v=\"107\", \"Not_A Brand\";v=\"99\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"macOS\"",
    "User-Agent": random.choice(user_agents),
    "Accept-Language": random.choice(accept_languages),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Connection": "keep-alive",
    }
    base_url = f"https://www.amazon.com/s?k={search_term.replace(' ', '+')}"
    books, seen_titles, page, consecutive_failures = [], set(), 1, 0

    while len(books) < num_books:
        retry_count = 0
        success = False
        # retry logic to handle the unsuccessful extract
        while retry_count < 3 and not success:
            try:
                response = requests.get(f"{base_url}&page={page}", headers=headers, timeout=10)
                response.raise_for_status()
                success = True
            except requests.RequestException as e:
                retry_count += 1
                wait_time = 2 ** retry_count + random.uniform(1, 3)  # Exponential backoff
                print(f"Network error on page {page}, retrying {retry_count}/3: {e}. Retrying in {wait_time:.2f} seconds.")
                time.sleep(wait_time)

        if not success:
            print(f"Failed to retrieve page {page} after 3 retries. Moving to next page.")
            consecutive_failures += 1
            if consecutive_failures >= 3:
                raise Exception("Consecutive page failures reached threshold. Halting scraping process.")
                break
            page += 1
            continue

        # Reset consecutive failures after a successful page fetch
        consecutive_failures = 0

        # Parsing and data extraction
        try:
            soup = BeautifulSoup(response.content, "html.parser")
            book_containers = soup.find_all("div", {"class": "s-result-item"})
            if len(book_containers) == 0:
                print(f"With {search_term}, no books are found")
                return
        except Exception as e:
            print(f"Parsing error on page {page}: {e}")
            page += 1
            continue

        for book in book_containers:
            try:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                if title and author and price and rating:
                    book_title = title.text.strip()
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })
            except AttributeError as e:
                print(f"Data extraction error for one of the items on page {page}: {e}")
                continue

        page += 1

    return books[:num_books]
   
def get_amazon_data_books(num_books, ti):
    
    try:
        books = extract_amazon_books(num_books, search_term)
        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(books)
        
        try:
            # Remove duplicates based on 'Title' column
            df.drop_duplicates(subset="Title", inplace=True)
        except Exception as e:
            print(f"Data transformation error: {e}")
        
        # Push the DataFrame to XCom
        try:
            ti.xcom_push(key='book_data', value=df.to_dict('records'))
        except Exception as e:
            print(f"Failed to push data to xCom: {e}")
    except requests.RequestException as e:
        print(f"Failed to fetch book data from Amazon: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

#4) create and store data in table on postgres (load)
def prepare_insert_query(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    search_term = Variable.get("amazon_book_search_term")  # Fetch from Airflow Variable
    if not book_data:
        print("No new books found.")
        return None

    transformed_data = []
    for book in book_data:
        # Convert price to numeric
        price = re.sub(r'[^\d.]', '', book['Price'])
        price = float(price) if price else None

        # Convert rating to numeric
        rating_match = re.search(r'\d+(\.\d+)?', book['Rating'])
        rating = float(rating_match.group()) if rating_match else None

        # Format datetime
        updated_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Escape single quotes by doubling them
        book_title = book['Title'].replace("'", "''")
        book_author = book['Author'].replace("'", "''")

        transformed_data.append(
            f"('{book_title}', '{book_author}', {price}, {rating}, '{search_term}', '{updated_datetime}')"
        )

    # Build the final SQL query with transformed data
    values = ", ".join(transformed_data)
    insert_query = f"""
    INSERT INTO books (title, authors, price, rating, search_term, updated_datetime)
    VALUES {values}
    """
    return insert_query

fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[20],  # Number of books to fetch
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id=db_connection,
    sql=create_table_sql,
    dag=dag,
)

def create_partition_params():
    current_date = datetime.now()
    next_month = current_date + timedelta(days=30)
    partition_name = f"books_{current_date.strftime('%Y_%m')}"
    return {
        "partition_name": partition_name,
        "current_month_start": current_date.strftime('%Y-%m-01'),
        "next_month_start": next_month.strftime('%Y-%m-01'),
    }
params = create_partition_params()
create_partition_sql_final = create_partition_sql.format(**params)

create_partition_task = SQLExecuteQueryOperator(
    task_id="create_partition",
    conn_id=db_connection,
    sql=create_partition_sql_final,
    dag=dag,
)

# Task to prepare the query for SQLExecuteQueryOperator
prepare_insert_query_task = PythonOperator(
    task_id='prepare_insert_query',
    python_callable=prepare_insert_query,
    dag=dag,
)

insert_book_data_task = SQLExecuteQueryOperator(
    task_id="insert_book_data",
    conn_id=db_connection,
    sql="{{ task_instance.xcom_pull(task_ids='prepare_insert_query') }}",  # Retrieve SQL directly from XCom
    dag=dag,
)

#dependencies
create_table_task >>  create_partition_task >> fetch_book_data_task >> prepare_insert_query_task >> insert_book_data_task