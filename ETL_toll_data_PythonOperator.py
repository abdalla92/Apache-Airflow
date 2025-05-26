# importing the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests
import subprocess
import os
import tarfile


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

# defining the DAG definition
dag = DAG(
    'ETL_toll_data_python',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

input_file = 'tolldata.tgz'
csv_data = 'csv_data.csv'
tsv_data = 'tsv_data.csv'
fixed_width_data ='fixed_width_data.csv'
extracted_data = 'extracted_data.csv'
transformed_data = 'transformed_data.csv'

def download_file():
    url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for HTTP errors
        response.raise_for_status()
        # Open a local file in binary write mode
        with open(input_file, 'wb') as file:
            # Write the content to the local file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {input_file}")

def download_dataset_py():
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
    output_path = '/home/project/airflow/dags/finalassignment/staging/tolldata.tgz'
    
    # Check if the file already exists
    if os.path.exists(output_path):
        print(f"File already exists: {output_path}")
        return
    
    # Download the file using curl
    command = ['curl', url, '-o', output_path]
    subprocess.run(command, check=True)
    print(f"File downloaded successfully: {output_path}")

def untar_dataset_py():
    # Path to the tar file
    tar_path = input_file
    # Destination directory
    extract_path = '/home/project/airflow/dags/finalassignment/staging/'
    
    # Open the tar file
    with tarfile.open(tar_path, 'r:gz') as tar:
        # Extract all contents to the destination directory
        tar.extractall(path=extract_path)
    print(f"File extracted successfully to: {extract_path}")


def extract_data_from_csv():
    # Read the contents of the file into a string
    with open('/home/project/airflow/dags/finalassignment/staging/vehicle-data.csv', 'r') as infile, \
            open('/home/project/airflow/dags/finalassignment/staging/'+csv_data, 'w') as outfile:
        for line in infile:
            fields = line.split(',')
            field_1 = fields[0]
            field_2 = fields[1]
            field_3 = fields[3]
            field_4 = fields[4]
            outfile.write(field_1 + "," + field_2 + "," + field_3 + "," + field_4  + "\n")

def extract_data_from_tsv():
    # Read the contents of the file into a string
    with open('/home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv', 'r') as infile, \
            open('/home/project/airflow/dags/finalassignment/staging/'+tsv_data, 'w') as outfile:
        for line in infile:
            fields = line.split('\t')
            field_5 = fields[4]
            field_6 = fields[5]
            field_7 = fields[6]
            outfile.write(field_5 + "," + field_6 + "," + field_7 + "\n")

def extract_data_from_fixed_width():
    # Read the contents of the file into a string
    with open('/home/project/airflow/dags/finalassignment/staging/payment-data.txt', 'r') as infile, \
            open('/home/project/airflow/dags/finalassignment/staging/'+ fixed_width_data, 'w') as outfile:
        for line in infile:
            # Split by whitespace to get columns
            fields = line.strip().split()
            # Extract the last two columns
            field_6 = fields[-2]
            field_7 = fields[-1]
            outfile.write(field_6 + "," + field_7 + "\n")
def consolidate_data():
    # Paths to the input files
    csv_path = '/home/project/airflow/dags/finalassignment/staging/' + csv_data
    tsv_path = '/home/project/airflow/dags/finalassignment/staging/' + tsv_data
    fixed_path = '/home/project/airflow/dags/finalassignment/staging/' + fixed_width_data
    output_path = '/home/project/airflow/dags/finalassignment/staging/' + extracted_data

    # Open all files simultaneously
    with open(csv_path, 'r') as csvfile, \
         open(tsv_path, 'r') as tsvfile, \
         open(fixed_path, 'r') as fixedfile, \
         open(output_path, 'w') as outfile:
        # Iterate through all files line by line
        for csv_line, tsv_line, fixed_line in zip(csvfile, tsvfile, fixedfile):
            # Remove trailing newlines and spaces
            csv_line = csv_line.strip()
            tsv_line = tsv_line.strip()
            fixed_line = fixed_line.strip()
            # Concatenate with commas (like paste -d,)
            combined_line = f"{csv_line} {tsv_line} {fixed_line}\n"
            outfile.write(combined_line)
    print(f"Data consolidated successfully into: {outfile}")

def transform_data():
    # Read the contents of the file into a string
    with open('/home/project/airflow/dags/finalassignment/staging/' + extracted_data, 'r') as infile, \
        open('/home/project/airflow/dags/finalassignment/staging/' + transformed_data, 'w') as outfile:
        for line in infile:
            transformed_line = line.upper()
            outfile.write(transformed_line + '\n')

def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open('/home/project/airflow/dags/finalassignment/staging/' + transformed_data, 'r') as infile:
        for line in infile:
            print(line)


# Define the task named execute_download_dataset to call the `download_dataset` function
execute_download_dataset = BashOperator(
    task_id='download_dataset',
    bash_command='curl -o /home/project/airflow/dags/finalassignment/staging/tolldata.tgz https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    dag=dag,
)
# Define the task named execute_untar_dataset to call the `untar_dataset_py` function
execute_untar_dataset = BashOperator(
    task_id='untar_dataset',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz --directory=/home/project/airflow/dags/finalassignment/staging/',
    dag=dag,
)
# Define the task named execute_extract_data_from_csv to call the `extract_data_from_csv` function
execute_extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)
# Define the task named execute_extract_data_from_tsv to call the `extract_data_from_tsv` function
execute_extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
# Define the task named execute_extract_data_from_fixed_width to call the `extract_data_from_fixed_width` function
execute_extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)
# Define the task named execute_consolidate_data to call the `consolidate_data` function
execute_consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

# Define the task named execute_consolidate_data to call the `transform_data` function
execute_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)
# Define the task named execute_check to call the `check` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

# Set the task dependencies
execute_download_dataset >> execute_untar_dataset >> execute_extract_data_from_csv >> execute_extract_data_from_tsv >> \
    execute_extract_data_from_fixed_width >> execute_consolidate_data >> execute_transform_data >> execute_check
