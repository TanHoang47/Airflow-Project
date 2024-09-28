import os
import json
import requests
import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define default arguments
default_args = {
    'owner': 'hoanglht2',
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

def get_data_from_api(exec_date):
    headers = {
        "apikey": os.getenv('API_KEY')
    }

    # Get symbols
    symbols_url = "https://api.apilayer.com/exchangerates_data/symbols"
    symbols_response = requests.get(symbols_url, headers=headers)
    
    if symbols_response.status_code == 200:
        try:
            symbols_data = symbols_response.json()
            symbols_list = list(symbols_data["symbols"].keys())
            symbols_str = ",".join(symbols_list)
        except requests.exceptions.JSONDecodeError:
            print("Response is not JSON or no response from GET symbols")
            return None
    else:
        print(f"API Error in GET symbols: {symbols_response.status_code}")
        return None

    # Get exchange rates for the given date
    rate_url = f"https://api.apilayer.com/exchangerates_data/{exec_date}?symbols={symbols_str}&base=USD"
    rate_response = requests.get(rate_url, headers=headers)
    
    if rate_response.status_code == 200:
        try:
            rate_data = rate_response.json().get('rates', {})
        except requests.exceptions.JSONDecodeError:
            print("Response is not JSON or no response from GET rates")
            return None
    else:
        print(f"API Error in GET rates: {rate_response.status_code}")
        return None
    csv_file_path = f'/opt/airflow/dags/rate_data.csv'
    df = pd.DataFrame(list(rate_data.items()), columns=['rate_symbols', 'rate_values'])
    df['rate_date'] = exec_date
    # print(df)
    # Write to a new file
    if os.path.exists(csv_file_path):
        existing_df = pd.read_csv(csv_file_path)
        combined_df = pd.concat([existing_df, df]).drop_duplicates()
        combined_df.to_csv(csv_file_path, index=False)
        print(f"Data appended to {csv_file_path}")
    else:
        df.to_csv(csv_file_path, index=False)
        print(f"Data saved to {csv_file_path}")

    return csv_file_path



# Function to insert data into Postgres
def insert_data_into_db(**context):
    # Get the file path from the previous task
    csv_file_path = context['task_instance'].xcom_pull(task_ids='crawl_data')
    if csv_file_path is None:
        raise ValueError("File path is none")
    # Read the CSV data
    df = pd.read_csv(csv_file_path)
    # Prepare data for insertion
    rate_symbols = df['rate_symbols'].tolist()
    rate_values = df['rate_values'].tolist()
    rate_date= df['rate_date'].tolist()

    # Insert into the database
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONN_ID")

    insert_query = """
    INSERT INTO rates (rate_date, rate_symbols, rate_values)
    VALUES (%s, %s, %s)
    """
    for i in range(len(rate_symbols)):
        # Convert rate_value to JSON if the column is JSONB
        json_rate_value = json.dumps(rate_values[i])
        postgres_hook.run(insert_query, parameters=(rate_date[i], rate_symbols[i], json_rate_value))

# Using pandas to calculate data
def calculate_max_range_and_save():
    postgres_hook = PostgresHook(postgres_conn_id="POSTGRES_CONN_ID")
    query = "SELECT * FROM rates"
    df = pd.read_sql(query, postgres_hook.get_conn())
    
    rate_date_list = df['rate_date'].unique()
    result_dfs = []

    for i in rate_date_list:
        temp_df_1 = df[df['rate_date'] == i]
        temp_df_2 = df[df['rate_date'] != i]
        temp_df_1 = temp_df_1.rename(columns={'rate_values': 'rate_values_1'})

        result = pd.merge(temp_df_1, temp_df_2, on='rate_symbols', how='outer')
        result['rate_values_diff'] = (result['rate_values'] - result['rate_values_1']).abs()
        result = result.drop(columns=['rate_values', 'rate_values_1'])
        idx = result.groupby('rate_symbols')['rate_values_diff'].idxmax()

        result_df = result.loc[idx].reset_index(drop=True)
        result_dfs.append(result_df)

    # Concatenate all result DataFrames
    final_result_df = pd.concat(result_dfs, ignore_index=True)
    print(final_result_df)
    # Save results to CSV
    result_csv_path = "/opt/airflow/dags/final_result.csv"
    
    if os.path.exists(result_csv_path):
        os.remove(result_csv_path)
        print(f"Old file {result_csv_path} removed.")
    
    final_result_df.to_csv(result_csv_path, index=False)
    print(f"Results saved to {result_csv_path}")
def send_data_to_telegram():
    current_date = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    chat_id = os.getenv("CHAT_ID")
    bot_token = os.getenv("BOT_TOKEN")
    send_document = "https://api.telegram.org/bot" + bot_token + "/sendDocument?"
    data = {
        "chat_id": chat_id,
        "parse_mode": "HTML",
        "caption": current_date + "_final_result.csv",
    }
    r = requests.post(
        send_document, data=data, files={"document": open("/opt/airflow/dags/final_result.csv", "rb")}
    )
    if r.status_code == 200:
        print(f"File successfully sent to telegram")
    else:
        print(f"Failed to send file. Status code: {r.status_code}")
        print("Response:", r.text)

# Define the DAG
with DAG(
    'Airflow Project',
    default_args=default_args,
    start_date=datetime(2024, 9, 1),
    schedule_interval='0 1 * * *',
    max_active_runs = 1,
    catchup=True
) as dag:
    
    # Task to fetch data from the API for each execution date
    crawl_data = PythonOperator(
        task_id='crawl_data',
        python_callable=get_data_from_api,
        op_args=['{{ ds }}'],
        provide_context=True
    )
    
    # Task to create the table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id="POSTGRES_CONN_ID",
        sql=""" 
        CREATE TABLE IF NOT EXISTS rates (
            rate_date DATE NOT NULL,
            rate_symbols TEXT NOT NULL,
            rate_values NUMERIC NOT NULL
        );
        
        TRUNCATE TABLE rates;
        """
    )

    # Task to insert data into the table
    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_into_db,
        provide_context=True
    )

    # Task to calculate the maximum range and save to CSV
    calculate_and_save = PythonOperator(
        task_id='calculate_and_save',
        python_callable=calculate_max_range_and_save
    )

    # Task to send data to telegram
    send_telegram_data = PythonOperator(
        task_id='send_telegram_data',
        python_callable=send_data_to_telegram
    )

    # Define task dependenciesi
    crawl_data >> create_table >> insert_data >> calculate_and_save >> send_telegram_data
