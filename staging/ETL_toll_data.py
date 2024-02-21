import os
import requests
import tarfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd

def unzip_data():
    # Source tgz file path
    tgz_file_path = '/home/project/airflow/dags/finalassignment/tolldata.tgz'
    
    # Destination directory
    destination_dir = "/home/project/airflow/dags/finalassignment/staging/tolldata"  # Replace with your actual destination directory
    
    # Create the destination directory if it doesn't exist
    os.makedirs(destination_dir, exist_ok=True)
    
    # Extract the tgz file
    with tarfile.open(tgz_file_path, "r:gz") as tar:
        tar.extractall(destination_dir)
    
    print("Data unzipped successfully")

def extract_data_from_csv():
    # Input CSV file path
    input_csv_path = '/home/project/airflow/dags/finalassignment/staging/tolldata/vehicle-data.csv'  # Replace with the actual path to your CSV file
    
    # Output CSV file path
    output_csv_path = '/home/project/airflow/dags/finalassignment/staging/csv_data.csv'  # Replace with the desired path for the output CSV file
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Vehicle code']

    # Read the CSV file
    df = pd.read_csv(input_csv_path, names=column_names, encoding='latin1')
    
    # Extract specific fields (Rowid, Timestamp, Anonymized Vehicle number, Vehicle type)
    extracted_data = df[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    
    # Save the extracted data to a new CSV file
    extracted_data.to_csv(output_csv_path, index=False)
    
    print("CSV Data extracted and saved successfully")

def extract_data_from_tsv():
    # Input TSV file path
    input_tsv_path = '/home/project/airflow/dags/finalassignment/staging/tolldata/tollplaza-data.tsv'  
    
    # Output CSV file path
    output_csv_path = '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv'  
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code']
    df = pd.read_csv(input_tsv_path, sep='\t', encoding='latin1', header=None, names=column_names)
    
    # Extract specific fields (Number of axles, Tollplaza id, Tollplaza code)
    extracted_data = df[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
    
    # Save the extracted data to a new CSV file
    extracted_data.to_csv(output_csv_path, index=False)
    
    print("TSV Data extracted and saved successfully")

def extract_data_from_fixed_width():
    # Input fixed width file path
    input_fixed_width_path = '/home/project/airflow/dags/finalassignment/staging/tolldata/payment-data.txt' 
    
    # Output CSV file path
    output_csv_path = '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv'  
    
    # Define column widths for the fixed-width file
    col_widths = [6, 20, 15, 8, 10, 20, 10]  # Adjust the widths based on the actual widths of each field
    
    # Read the fixed-width file
    df = pd.read_fwf(input_fixed_width_path, widths=col_widths, header=None)
    
    # Extract only the required fields
    extracted_df = df.iloc[:, 5:]  # Assuming the Type of Payment code is in the 6th column and Vehicle Code is in the 7th column
    
    # Assign column names to the extracted data
    extracted_df.columns = ['Type of Payment code', 'Vehicle Code']
    
    # Save the extracted data to a new CSV file
    extracted_df.to_csv(output_csv_path, index=False)
    
    print("Text Data extracted and saved successfully")



def transform_data():
    # Input CSV file path
    input_csv_path = '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv'
    
    # Output CSV file path
    output_csv_path = '/home/project/airflow/dags/finalassignment/staging/transformed_data.csv'
    
    try:
        # Read the extracted data from CSV
        df = pd.read_csv(input_csv_path)
        
        # Check if 'Vehicle type' column exists
        if 'Vehicle type' in df.columns:
            # Transform the 'Vehicle type' field to uppercase
            df['Vehicle type'] = df['Vehicle type'].str.upper()
            
            # Save the transformed data to a new CSV file
            df.to_csv(output_csv_path, index=False)
            
            print("Data transformed and saved successfully")
        else:
            print("Error: 'Vehicle type' column not found in the input CSV file.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


# Define your default arguments for the DAG
default_args = {
    'owner': 'Akhil',
    'start_date': datetime.today(),
    'email': 'akhil@example.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define your DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    schedule_interval='@daily',
    description='Apache Airflow Final Assignment'
)

# Define the tasks using PythonOperator
unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag
)

extract_data_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag
)

extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag
)

extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

consolidate_data_task = BashOperator(
    task_id='consolidate_data',
    bash_command='cd /home/project/airflow/dags/finalassignment/staging/ && paste -d "," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag
)

# Define the task dependencies correctly using the task objects
unzip_data_task >> extract_data_from_csv_task >> extract_data_from_tsv_task >> extract_data_from_fixed_width_task >> consolidate_data_task >> transform_data_task

