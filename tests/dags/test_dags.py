import pytest
from airflow.models import DagBag
from datetime import timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_loaded(dag_bag):
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")
    assert dag is not None, "DAG fetch_and_store_amazon_books should be loaded successfully"
    assert len(dag.tasks) == 5, "DAG fetch_and_store_amazon_books should have 5 tasks"

def test_dag_default_args(dag_bag):
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")
    assert dag.default_args['owner'] == 'airflow'
    assert dag.default_args['depends_on_past'] is False
    assert dag.default_args['retries'] == 1
    assert dag.default_args['retry_delay'] == timedelta(minutes=10)

def test_task_ids(dag_bag):
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")
    task_ids = [task.task_id for task in dag.tasks]
    expected_task_ids = [
        "fetch_book_data", "create_table", 
        "prepare_insert_query", "insert_book_data", "create_partition"
    ]
    for task_id in expected_task_ids:
        assert task_id in task_ids, f"{task_id} should be present in the DAG"

def test_task_properties(dag_bag):
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")

    # Test create_table_task properties
    create_table_task = dag.get_task("create_table")
    assert isinstance(create_table_task, SQLExecuteQueryOperator)
    assert create_table_task.conn_id == 'books_connection'
    assert create_table_task.sql is not None

    # Test fetch_book_data_task properties
    fetch_book_data_task = dag.get_task("fetch_book_data")
    assert isinstance(fetch_book_data_task, PythonOperator)
    assert fetch_book_data_task.python_callable.__name__ == "get_amazon_data_books"

    # Test prepare_insert_query properties
    prepare_insert_query_task = dag.get_task("prepare_insert_query")
    assert isinstance(prepare_insert_query_task, PythonOperator)
    assert prepare_insert_query_task.python_callable.__name__ == "prepare_insert_query"

def test_dependencies(dag_bag):
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")
    assert_dag_dict_equal(
        {
            'create_table': ['create_partition'],
            'create_partition': ['fetch_book_data'],
            'fetch_book_data': ['prepare_insert_query'],
            'prepare_insert_query': ['insert_book_data'],
            'insert_book_data': []
        },
        dag,
    )

# Helper function for DAG dependency assertions
def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)
