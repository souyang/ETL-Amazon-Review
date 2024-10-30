import pytest
from airflow.models import DagBag

# Fixture to load DAGs
@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)

'''
    Unit Testing on Loading DAG
'''
def test_dag_loaded(dag_bag: DagBag):
    """Test if DAG is correctly loaded in Airflow"""
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")
    assert dag is not None, "DAG fetch_and_store_amazon_books should be loaded successfully"
    assert len(dag.tasks) == 5, "DAG fetch_and_store_amazon_books should have 5 tasks"

'''
    Unit Testing on DAG Task List
'''
def test_task_ids(dag_bag: DagBag):
    """Test if specific tasks exist in the DAG"""
    dag = dag_bag.get_dag("fetch_and_store_amazon_books")
    task_ids = [task.task_id for task in dag.tasks]
    assert "fetch_book_data" in task_ids, "fetch_book_data task should be present in the DAG"
    assert "create_table" in task_ids, "create_table task should be present in the DAG"
    assert "prepare_insert_query" in task_ids, "prepare_insert_query task should be present in the DAG"
    assert "insert_book_data" in task_ids, "insert_book_data task should be present in the DAG"
    assert "create_partition" in task_ids, "create_partition_task task should be present in the DAG"

def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)

'''
    Unit Test on DAG Hierarchy
'''
def test_dependencies(dag_bag: DagBag):
    """Test if task dependencies are set correctly"""
    assert_dag_dict_equal(
            {
                'create_table': ['create_partition'],
                'create_partition': 
                ['fetch_book_data'],
                'fetch_book_data': ['prepare_insert_query'],
                'prepare_insert_query': ['insert_book_data'],
                'insert_book_data': []
            },
            dag_bag.get_dag("fetch_and_store_amazon_books"),
        )
    
