from airflow.sdk import dag, task
from airflow import DAG
from datetime import datetime,timedelta
import json
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from airflow.operators.python import PythonOperator

default_args={
    'owner': "Jaswinder",
    'depends_on_past':False,
    'start_date':datetime(2026,1,1),
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

#Python core fucntion
def fetch_region_data(region_name, **kwargs):
    ti=kwargs['ti']

    url = "https://raw.githubusercontent.com/mledoze/countries/master/dist/countries.json"

    response = requests.get(url,timeout=15)

    if response.status_code!=200:
        raise Exception(f"Failed to fetch live data from this dummy url jaswinder. Status: {response.status_code}")
    
    all_countries =response.json()

    parsed_data=[]

    for country in all_countries:
        if not isinstance(country,dict):
            continue

        current_region =country.get("region", "Unknown")

        if current_region.lower()==region_name.lower():
            name_obj = country.get("name")
            country_name=name_obj.get("common",'Unknown') if isinstance(name_obj,dict) else str(name_obj or "Unknown")

            capital_obj=country.get("capital",["None"])
            capital=capital_obj[0] if isinstance(capital_obj,list) and len(capital_obj)>0 else str(capital_obj or "None")

            try:
                population = int(country.get("population",0))
            except (ValueError,TypeError):
                population=0

            parsed_data.append({
                "country_name": country_name,
                "capital":capital,
                "region":current_region,
                "population":population
            })

    print(f"Live data Successs: Extracted {len(parsed_data)} row for region '{region_name}'")
    #do pass parsed_Data somehere
    ti.xcom_push(key=f'{region_name}_data_key',value=parsed_data)


def combine_and_load_to_snowflake(**kwargs):
    ti=kwargs['ti']

    europe_data = ti.xcom_pull(task_ids='fetch_europe_data',key='europe_data_key') or []
    asia_data = ti.xcom_pull(task_ids='fetch_asia_data',key='asia_data_key') or []

    combined_list= europe_data + asia_data

    if not combined_list:
        raise ValueError("Pipeline execution stopped Jassi")
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    insert_queries=[]

    for item in combined_list:
        clean_name=item['country_name'].replace("'","''")
        clean_capital=item['capital'].replace("'","''")

        query= f"""
                INSERT INTO DEMO_DB.PUBLIC.COUNTRY_DATA(country_name,capital,region,population)
                VALUES ('{clean_name}','{clean_capital}','{item['region']}',{item['population']});
                """
        
        insert_queries.append(query)

    if insert_queries:
        snowflake_hook.run(insert_queries)
        print(f"Jaswinder Database Transaction commited: {len(insert_queries)}")


def extract_high_population_to_s3(**kwargs):
    snowflake_hook =SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sql_query = "Select country_name,capital,region,population from DEMO_DB.PUBLIC.COUNTRY_DATA;"

    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(sql_query)
    rows=cursor.fetchall()

    df=pd.DataFrame(rows,columns=['country_name','capital','region','population'])

    print(f"Exporting all {len(df)} rows from Snowflake to Jaswinder's S3.")

    local_csv_path = '/tmp/high_population_countries.csv'
    df.to_csv(local_csv_path,index=False)

    s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
    s3_hook.load_file(
        filename=local_csv_path,
        key='exports/high_population_countries.csv',
        bucket_name='airflow-snowflake-output',
        replace=True
    )

    print("File is loaded jaswinder....")

with DAG(
    'e2e_data_pipeline',
    default_args=default_args,
    description="E2e pipeline for Data2Dollars",
    schedule=None,
    catchup=False
) as dag:
    
    fetch_europe_data = PythonOperator(
        task_id='fetch_europe_data',
        python_callable=fetch_region_data,
        op_kwargs = {'region_name':'europe'},

    )

    fetch_asia_data = PythonOperator(
        task_id='fetch_asia_data',
        python_callable=fetch_region_data,
        op_kwargs = {'region_name':'asia'},

    )

    load_to_snowflake = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=combine_and_load_to_snowflake,
    )


    export_to_s3 = PythonOperator(
        task_id='export_to_s3',
        python_callable=extract_high_population_to_s3,
    )



    [fetch_europe_data,fetch_asia_data] >> load_to_snowflake >> export_to_s3




        









            





