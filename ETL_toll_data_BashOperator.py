# importing the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Abdallah',
    'start_date': days_ago(0),
    'email': ['myemail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data_BashOperator',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# defining the task 'unzip_data'
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz --directory=/home/project/airflow/dags/finalassignment/staging/',
    dag=dag,
)

# defining the task 'extract_data_from_csv'
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f1-4 -d"," /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)

# defining the task 'extract_data_from_tsv'
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)

# defining the task 'extract_data_from_fixed_width'
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cat /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr -s " " | cut -d" " -f10-11 > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag,
)

# defining the task 'consolidate_data'
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    dag=dag,
)

# defining the task 'transform_data'
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "a-z" "A-Z" < /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)
execute_check = BashOperator(
    task_id='check',
    bash_command='cat /home/project/airflow/dags/finalassignment/staging/transformed_data.csv | while read line; do echo "$line"; done',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data >> execute_check
