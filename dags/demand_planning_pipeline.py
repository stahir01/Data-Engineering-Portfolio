import os
from airflow import DAG
import pandas as pd
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')

default_args = {
    'owner': 'burny',
    'depands_on_past': True,
    'email_on_failure': True,
    'email': [EMAIL_ADDRESS],
    'retries': 3,
    'retries_delay': timedelta(minutes=5)
    }

dag = DAG(
    'demand_planning_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract data and load into MySQL database',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
    )

def find_path(file_path):
    """Returns the full path of a file relative to the specified directory."""
    print(f"Current working directory: {os.getcwd()}")
    full_path = os.path.join(os.getcwd(), file_path)
    print(f"Full path: {full_path}")
    return full_path

def read_planning_file(**kwargs):
    """Reads the demand planning file from the specified path."""
    ti = kwargs['ti']
    full_path = ti.xcom_pull(task_ids='find_file_path')  

    if not full_path:
        raise ValueError("File path not found from the previous task.")

    # Build the expected file name
    todays_date = date.today().strftime("%d_%m_%Y")
    file_name = f"Demand_Planning_Data_{todays_date}.csv"

    file_with_path = os.path.join(full_path, file_name)

    # Check if the file exists
    if os.path.exists(file_with_path):
        print(f"File found: {file_with_path}")
        df = pd.read_csv(file_with_path)
        return df.to_dict() 
    else:
        print(f"File not found: {file_with_path}")
        return None

def reformat_data(**kwargs):
    """Data Transformation"""
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(task_ids='read_file')  
    
    if data_dict is None:
        raise ValueError("No data was returned from read_file.")
    
    # Convert the dictionary back to a DataFrame
    data = pd.DataFrame.from_dict(data_dict)

    # Transformation logic
    data = data.drop(columns=['Product'])  
    data = data.rename(columns={
        'Reorder_Level': 'Reorder_Level_Predicted',
        'Profit': 'Profit_Predicted'
    })

    data['Reorder_Level'] = data['Actual_Demand'] - data['Forecasted_Demand']
    data.insert(8, 'Reorder_Level', data.pop('Reorder_Level'))

    data['Profit'] = data['Revenue'] - data['Cost']
    profit_column = data.pop('Profit')
    data['Profit'] = profit_column     

    columns_to_round = ['Revenue', 'Cost', 'Profit_Predicted', 'Profit']
    data[columns_to_round] = data[columns_to_round].round(2)

    print("Transformed Data:")
    print(data.head())  
    return data.to_dict()  


def load_data_to_demandplanning_db(**kwargs):
    """Load the transformed data into the demand_planning_fact table."""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')

    if transformed_data is None:
        raise ValueError("No transformed data found to load into the database.")
    

    transform_data = pd.DataFrame.from_dict(transformed_data)


    try:
        postgres_conn = PostgresHook(postgres_conn_id='demand_planning_postgres_conn')
        print("PostgreSQL connection successful.")
        
        # Log connection URI
        conn_uri = postgres_conn.get_uri()
        print(f"Connection URI: {conn_uri}")

        # Check and log the current search path
        with postgres_conn.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW search_path;")
                search_path = cursor.fetchone()
                print(f"Current search path: {search_path}")
    except Exception as e:
        raise ValueError(f"Failed to establish connection to PostgreSQL: {e}")


    insert_query = """
        INSERT INTO dem_plan_sch.demand_planning_fact (
            date, category, region, customer, forecasted_demand, actual_demand,
            stock_available, reorder_level_predicted, reorder_level, revenue,
            cost, profit_predicted, profit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows_inserted = 0
    for _, row in transform_data.iterrows():
        try:
            postgres_conn.run(insert_query, parameters=(
                row['Date'], row['Category'], row['Region'], row['Customer'],
                row['Forecasted_Demand'], row['Actual_Demand'], row['Stock_Available'],
                row['Reorder_Level_Predicted'], row['Reorder_Level'], row['Revenue'],
                row['Cost'], row['Profit_Predicted'], row['Profit']
            ))
            rows_inserted += 1
        except Exception as e:
            print(f"Error inserting row: {row}. Error: {e}")

    print(f"Successfully inserted {rows_inserted} rows into demand_planning_fact table.")






file_path = PythonOperator(
    task_id= "find_file_path",
    python_callable=find_path,
    op_kwargs={'file_path': 'dataset/Demand_Planning_Data'},
    retries=3,
    dag=dag,
)

read_file = PythonOperator(
    task_id= "read_file",
    provide_context=True,
    python_callable=read_planning_file,
    retries=3,
    dag=dag,
)

transform_data = PythonOperator(
    task_id= "transform_data",
    provide_context=True,
    python_callable=reformat_data,
    retries=3,
    dag=dag,
)

load_data = PythonOperator(
    task_id= "Load_into_demandplanning_db",
    provide_context=True,
    python_callable=load_data_to_demandplanning_db,
    retries=3,
    dag=dag,
)



file_path >> read_file >> transform_data >> load_data
