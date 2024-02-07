#Importing libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Defining arguments
default_args = {
    'owner': 'Sam Otu',
    'start_date': days_ago(0),
    'email': ['sam.o@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG instantiation
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1)
)

# Defining task 1 to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag
)

# Defining task 2 to  extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1-4 ./vehicle-data.csv > ./csv_data.csv',
    dag=dag
)

#Defining task 3 to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_tsv',
    bash_command='cut -d$"\t" -f5-7 ./tollplaza-data.tsv > ./tsv_data.csv',
    dag=dag
)

#Defining task 4 to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_fwidth',
    bash_command='cut -c59-62,63- ./payment-data.txt > ./fixed_width_data.csv',
    dag=dag
)

#Defining task to merge data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag
)

# Defining task to transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk \'BEGIN {FS=OFS=","} {$1=toupper($1)}1\' extracted_data.csv > transformed_data.csv',
    dag=dag
)

# Task pipeline
unzip_data >> extract_data_from_csv
extract_data_from_csv >> extract_data_from_tsv
extract_data_from_tsv >> extract_data_from_fixed_width
extract_data_from_fixed_width >> consolidate_data
consolidate_data >> transform_data