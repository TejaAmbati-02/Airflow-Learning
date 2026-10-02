from airflow.sdk import dag,task

@dag(dag_id="Xcom_Pipeline")
def taskflow_xcom():

    # @task
    # def generate_data():
    #     print("Generating....")
    #     return {"processed_records":1550 ,"status":"SUCCESS"}
    
    # @task
    # def process_data(jaswinder: dict):
    #     print(f"Received: {jaswinder}")
    #     record=jaswinder["processed_records"]

    # pulled_metrics=generate_data()
    # process_data(pulled_metrics)

    #Manual Way

    @task
    def generate_manual_data(**context):
        print("Generating....")
        ti=context['ti']
       
        ti.xcom_push(key="custom_metrics",value={"processed_records":1550 ,"status":"SUCCESS"})
    
    @task
    def process_data(**context):
        ti=context['ti']
        jaswinder=ti.xcom_pull(task_ids="generate_manual_data",key="custom_metrics")
        print(f"Received : {jaswinder}")
        if jaswinder:
            record=jaswinder["processed_records"]
            print(f"Successfully processed {record} records")
        else:
            print("empty")
    

    generate_manual_data() >> process_data()


taskflow_xcom()



    
