from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from get_csv import get
from conn_sql import create_db
from transforming_data import executor

default_args = {
    'owner': 'rdghenrique',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['rdghenrique94@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}
with DAG(
        dag_id='workflow_pagseguro',
        default_args=default_args,
        tags=['airflow', 'mysql' 'pandas, kaggle, tranasctions, analytics'],
        description='Criar schemas, limpar e importar dados para o banco'
) as dag:

    create_db_mysql_task = PythonOperator(
        task_id="create_db_mysql",
        python_callable=create_db,
    )
    get_csv_task = PythonOperator(
        task_id="get_csv",
        python_callable=get,
    )
    processing_data = PythonOperator(
        task_id="cleaning_data",
        python_callable=executor,
    )
    create_analytic_mysql_task = MySqlOperator(
        task_id="create_analytic_mysql",
        mysql_conn_id="airflow",
        sql="sql_scripts/create_tables.sql",
    )
    insert_analytic_mysql_task = MySqlOperator(
        task_id="insert_values_analytic",
        mysql_conn_id="airflow",
        sql="sql_scripts/insert_data.sql",
    )
    create_db_mysql_task >> get_csv_task >> processing_data >> create_analytic_mysql_task >> insert_analytic_mysql_task
