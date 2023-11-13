from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

@task
def export_data_to_xcom(conn_id, sql_query):
    export_task = PostgresOperator(
        task_id='export_data_task',
        postgres_conn_id=conn_id,
        sql=sql_query,
    )
    return export_task