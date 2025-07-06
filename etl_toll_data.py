# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Sali',
    'start_date': datetime.now(),
    'email': ['your_email_here'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)


download_task = BashOperator(
    task_id='download_dataset',
    bash_command='wget -O /home/project/airflow/dags/python_etl/staging/tolldata.tgz >'
    dag=dag,
)

unzip_task = BashOperator(
        task_id='unzip_dataset',
        bash_command='tar -xzvf /home/project/airflow/dags/python_etl/staging/tolldat>'
        dag=dag,
    )


extract_data_from_csv_task = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
        cut -d, -f 1,2,3,4 /home/project/airflow/dags/python_etl/staging/vehicle-data>
    """,
    dag=dag,
)


extract_task = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command="""awk -F'\t' '{print $1","$2","$3}' /home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv > /home/project/airflow/dags/python_etl/staging/tsv_data.csv""",
        dag=dag,
    )

extract_data_from_fixed_width_task = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command="""cut -c1-10,11-15 /home/project/airflow/dags/python_etl/staging/payment-data.txt | awk '{print $1","$2}' > /home/project/airflow/dags/python_etl/staging/fixed_width_data.csv""",
        dag=dag,
    )



consolidate_task = BashOperator(
        task_id='consolidate_data',
        bash_command="""
            # Add header to the extracted_data.csv
            echo 'Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,Tollplaza id,Tollplaza code,Type of Payment code,Vehicle Code' > /home/project/airflow/dags/python_etl/staging/extracted_data.csv;
            
            # Concatenate csv_data.csv, tsv_data.csv, and fixed_width_data.csv, skipping headers from subsequent files
            tail -n +2 /home/project/airflow/dags/python_etl/staging/csv_data.csv >> /home/project/airflow/dags/python_etl/staging/extracted_data.csv;
            tail -n +2 /home/project/airflow/dags/python_etl/staging/tsv_data.csv >> /home/project/airflow/dags/python_etl/staging/extracted_data.csv;
            tail -n +2 /home/project/airflow/dags/python_etl/staging/fixed_width_data.csv >> /home/project/airflow/dags/python_etl/staging/extracted_data.csv;
        """,
        dag=dag,
    )

transform_task = BashOperator(
    task_id='transform_data',
    bash_command="""
        # Transform the 'vehicle_type' field (4th column) to uppercase using tr and save to transformed_data.csv
        # Extract the columns, transform the 4th column, and reassemble the line

        # Extract the first three columns and the 4th column
        cut -d, -f1-3 /home/project/airflow/dags/python_etl/staging/extracted_data.csv > /home/project/airflow/dags/python_etl/staging/temp_first_three.csv
        cut -d, -f4 /home/project/airflow/dags/python_etl/staging/extracted_data.csv | tr 'a-z' 'A-Z' > /home/project/airflow/dags/python_etl/staging/temp_vehicle_type.csv
        
        # Combine the columns back into a single CSV
        paste -d, /home/project/airflow/dags/python_etl/staging/temp_first_three.csv /home/project/airflow/dags/python_etl/staging/temp_vehicle_type.csv > /home/project/airflow/dags/python_etl/staging/transformed_data.csv

        # Clean up temporary files
        rm /home/project/airflow/dags/python_etl/staging/temp_first_three.csv /home/project/airflow/dags/python_etl/staging/temp_vehicle_type.csv
    """,
    dag=dag,
)

##testing the PR branch

download_task >> unzip_task >> extract_task  >> extract_data_from_fixed_width_task >> consolidate_task >> transform_task
