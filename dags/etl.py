from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from datetime import datetime, timedelta

with DAG(
    dag_id="nasa-apod-postgres",
    default_args = {
    'start_date': datetime.now() - timedelta(days=1),
    'schedule_interval': "@daily",
    },
    catchup=False
) as dag:
    ## Step 1: Create a table in PostgresSQL if not created
    @task
    def create_table():
        # initialize the hook
        hook = PostgresHook(postgres_conn_id='postgres_conn') 

        # create table query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        # execute the table creation query
        hook.run(create_table_query)
    
    ## Step 2: EXTRACT: Fetch Data from NASA API
    extract_apod = SimpleHttpOperator(
        task_id='extract-apod',
        http_conn_id="nasa_api", # Connection ID defined in Airflow for NASA API
        endpoint="planetary/apod", # NASA API Endpoint
        method='GET',
        data={"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"}, # Use the API KEY from the conn
        response_filter=lambda response: response.json()
    )
    
    ## Step 3: TRANSFORM: Process the data 
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get("title", ""),
            'explanation': response.get("explanation", ""),
            'url': response.get("url", ""),
            'date': response.get("date", ""),
            'media_type': response.get("media_type", ""),
        }

        return apod_data

    ## Step 4: LOAD: Load the data into PostgresSQL
    @task
    def load_data_to_postgres(apod_data):
        # initialize the postgres hook
        hook = PostgresHook(
            postgres_conn_id="postgres_conn"
        )

        # define the insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        # execute the sql query
        hook.run(insert_query, parameters=(
            apod_data["title"],
            apod_data["explanation"],
            apod_data["url"],
            apod_data["date"],
            apod_data["media_type"]
        ))

    # Step 5: Define the task dependencies
    ### Extract
    create_table() >> extract_apod
    api_response = extract_apod.output # all the details of extract_apod are stored in variable output
    ### Transform
    transformed_data = transform_apod_data(api_response)
    ### Load
    load_data_to_postgres(transformed_data) 