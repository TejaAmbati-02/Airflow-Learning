from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


# from airflow.operators.python import get_current_context
from airflow.providers.standard.operators.python import get_current_context




# Automatic Way
@dag(dag_id="XComs_Pipeline")
def xcoms_demo():

    @task
    def generate_data():
        print("Generating data...")
        return {"Proocessed_records":1550, "Status":"Success"}

    @task
    def process_data(data:dict):
        print(f"Received: {data}")
        record = data['Proocessed_records']
        print(f"Processing record: {record}")

    pulled_metrics = generate_data()
    process_data(pulled_metrics)



    # Manual Way
    @task
    def generate_manual_data():
        print("Generating data...")
        data = get_current_context()
        ti = data['ti']
        ti.xcom_push(key="generated_data", value={"Proocessed_records":1550, "Status":"Success"})

    @task
    def process_manual_data():
        context = get_current_context()
        ti = context['ti']
        data = ti.xcom_pull(key="generated_data", task_ids="generate_manual_data")
        print(f"Received: {data}")
        if data:
            record = data['Proocessed_records']
            print(f"Processing record: {record}")
            print(f"Successfully Processed {record} records")
        else:
            print("No data received.")

    generate_manual_data = generate_manual_data()
    process_manual_data = process_manual_data()


    generate_manual_data >> process_manual_data




xcoms_demo()