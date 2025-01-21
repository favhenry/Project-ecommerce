from datetime import datetime,timedelta

# Define DAG arguments
default_args = {
    'owner': 'Henry',
    'start_date': datetime(2024, 12, 19),  # Set the start date
    'email': [''],  # Set the email for notifications
    'email_on_failure': True,  # Notify on failure
    'email_on_retry': False,  # No notification on retries
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
}

from airflow import DAG

# Define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='A DAG to process web logs and perform ETL tasks',
    schedule_interval='@daily',  # Runs daily
)

from airflow.operators.python import PythonOperator

# Task to extract data
def extract_data_func():
    with open('home/project/airflow/dags/capstone/accesslog.txt', 'r') as infile:
        lines = infile.readlines()

    extracted_lines = []
    for line in lines:
        if line.endswith('html\n'):  # Filter lines ending with 'html'
            parts = line.split()  # Split line into parts
            ip_address = parts[0]  # IP address field (first field)
            timestamp = parts[3]  # Timestamp field (fourth field)
            size = parts[6]  # Size field (the seventh field)
            extracted_lines.append(f"{ip_address} {timestamp} {size}\n")

    with open('/home/project/airflow/dags/capstone/extracted_data.txt', 'w') as outfile:
        outfile.writelines(extracted_lines)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_func,
    dag=dag,
)

# Task to transform data
def transform_data_func():
    with open('/home/project/airflow/dags/capstone/extracted_data.txt', 'r') as infile:
        lines = infile.readlines()

    # Filter out specific IP address
    filtered_ips = [line for line in lines if line.strip() != '198.46.149.143']

    transformed_lines = []
    for line in lines:
        ip_address, timestamp, size = line.split()  # Extract ip, timestamp, and size
        size_in_mb = int(size) / (1024 * 1024)  # Convert bytes to MB
        transformed_lines.append(f"{ip_address} {timestamp} {size_in_mb:.2f} MB\n")


    with open('/home/project/airflow/dags/capstone/transformed_data.txt', 'w') as outfile:
        outfile.writelines(transformed_lines)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_func,
    dag=dag,
)

import tarfile

# Task to load data (archive into tar file)
def load_data_func():
    with tarfile.open('/home/project/airflow/dags/capstone/weblog.tar', 'w') as tar:
        tar.add('/home/project/airflow/dags/capstone/transformed_data.txt', arcname='transformed_data.txt')

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_func,
    dag=dag,
)

# Define task pipeline
extract_data >> transform_data >> load_data

