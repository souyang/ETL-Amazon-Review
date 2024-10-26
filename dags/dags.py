# dag -directed acyclic graph

#tasks : 1. create_table 2. 

#operators 

#hooks 

#dependencies
import time
from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# step 0: 
db_connection = 'books_connection'

# step 1: defin args for the airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# step 2: define the dag with the default args
dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

# step 3: Extract and Transform Amazon Book Data

# extract amazon books from the website
def extract_amazon_books(num_books):
    headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_Time",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }
    base_url = "https://www.amazon.com/s?k=machine+learning+interview"
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
                print(f"Network error on page {page}, retrying {retry_count}/3: {e}")
                time.sleep(2)

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
        books = extract_amazon_books(num_books)
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
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        print("No new books found.")
        return

    postgres_hook = PostgresHook(postgres_conn_id=db_connection)
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))

fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id=db_connection,
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies
create_table_task >> fetch_book_data_task >> insert_book_data_task