# Apache-Airflow
This project aims to build ETL Data Pipelines by extracting, transforming, and loading data into a database by using the BashOperator and PythonOperator of Apache Airflow

## Scenario
I am a data engineer at a data analytics consulting company. I have been assigned a project to decongest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. my job is to collect data available in different formats and consolidate it into a single file.

## Objectives
In this project, I will develop an Apache Airflow DAG that will:

Extract data from a csv file

Extract data from a tsv file

Extract data from a fixed-width file

Transform the data

Load the transformed data into the staging area

This project is based on the following tasks:

### Section 1: Create imports, DAG argument and definition

Task 1.1: Define DAG arguments

Task 1.2: Define the DAG

### Section 2: Create the tasks using BashOperator

Task 2.1: Create a task to unzip data.

Task 2.2: Create a task to extract data from csv file

Task 2.3: Create a task to extract data from tsv file

Task 2.4: Create a task to extract data from fixed-width file

Task 2.5: Create a task to consolidate data extracted from previous tasks

Task 2.6: Transform the data

Task 2.7: Define the task pipeline

### Section 3: Getting the DAG operational

Task 3.1: Submit the DAG

Task 3.2: Unpause and trigger the DAG

Task 3.3: List the DAG tasks

Task 3.4: Monitor the DAG


#### Note about screenshots
Throughout this project, I took screenshots of each task and named them accordingly.
