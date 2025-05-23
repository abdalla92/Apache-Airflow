# importing the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
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
    'email': ['cabdalla@gmail.com'],
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
    output_path = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    
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
    extract_path = '/home/project/airflow/dags/python_etl/staging/'
    
    # Open the tar file
    with tarfile.open(tar_path, 'r:gz') as tar:
        # Extract all contents to the destination directory
        tar.extractall(path=extract_path)
    print(f"File extracted successfully to: {extract_path}")


def extract_data_from_csv():
    # Read the contents of the file into a string
    with open('vehicle-data.csv', 'r') as infile, \
            open(csv_data, 'w') as outfile:
        for line in infile:
            fields = line.split(',')
            field_1 = fields[0]
            field_2 = fields[1]
            field_3 = fields[3]
            field_4 = fields[4]
            outfile.write(field_1 + "," + field_2 + "," + field_3 + "," + field_4  + "\n")

def extract_data_from_tsv():
    # Read the contents of the file into a string
    with open('tollplaza-data.tsv', 'r') as infile, \
            open(tsv_data, 'w') as outfile:
        for line in infile:
            fields = line.split('\t')
            field_5 = fields[4]
            field_6 = fields[5]
            field_7 = fields[6]
            outfile.write(field_5 + "," + field_6 + "," + field_7 + "\n")

def extract_data_from_fixed_width():
    # Read the contents of the file into a string
    with open('payment-data.txt', 'r') as infile, \
            open(fixed_width_data, 'w') as outfile:
        for line in infile:
            # Extract the last two columns based on fixed-width positions
            field_6 = line[-9:-4].strip()  # Adjust positions as per the file structure
            field_7 = line[-3:].strip()
            outfile.write(field_6 + "," + field_7 + "\n")

def consolidate_data():
    # Open the output file in write mode
    with open(extracted_data, 'w') as outfile:
        # Read and write data from csv_data.csv
        with open(csv_data, 'r') as csvfile:
            for line in csvfile:
                outfile.write(line)
        
        # Read and write data from tsv_data.csv
        with open(tsv_data, 'r') as tsvfile:
            for line in tsvfile:
                outfile.write(line)
        
        # Read and write data from fixed_width_data.csv
        with open(fixed_width_data, 'r') as fixedfile:
            for line in fixedfile:
                outfile.write(line)
    print(f"Data consolidated successfully into: {extracted_data}")

def transform_data():
    # Read the contents of the file into a string
    with open(extracted_data, 'r') as infile, \
        open(transformed_data, 'w') as outfile:
        for line in infile:
            transformed_line = line.upper()
            outfile.write(transformed_line + '\n')

def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(transformed_data, 'r') as infile:
        for line in infile:
            print(line)


# Define the task named execute_download_dataset to call the `download_dataset` function
execute_download_dataset = PythonOperator(
    task_id='download_dataset',
    bash_command='curl -o /home/project/airflow/dags/python_etl/staging/tolldata.tgz https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    dag=dag,
)
# Define the task named execute_untar_dataset to call the `untar_dataset_py` function
execute_untar_dataset = PythonOperator(
    task_id='untar_dataset',
    bash_command='tar -xvf /home/project/airflow/dags/python_etl/staging/$input_file -C /home/project/airflow/dags/python_etl/staging/',
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
execute_download_dataset >> execute_untar_dataset >> execute_extract_data_from_csv, execute_extract_data_from_tsv, \
    execute_extract_data_from_fixed_width >> execute_consolidate_data >> execute_transform_data >> execute_check
