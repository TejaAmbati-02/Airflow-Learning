from airflow.sdk import dag,task

@dag(dag_id="Parallel_Task_Braching")
def pipeline_demo():

    @task
    def fetch_data():
        print("Fetching data from source...")
        return {"record_count":250,"status":"READY"}
    
    # Conditional Branching Task=

    @task.branch
    def check_quality_branch(data:dict):
        count=data["record_count"]
        print(f"Evaluating quality for {count} records...")

        if count<1000:
            print("Low data volumn detected.")
            return "low_volume_alert"
        else:
            print("High data volume detected")
            return ["process_segment_a","process_segment_b"]
        
    @task
    def low_volume_alert():
        print("Warning: received low data")

    # Parallel Tasks    
    @task 
    def process_segment_a():
        print("Processing A")

    @task 
    def process_segment_b():
        print("Processing B")
    
    #Joining Parallel Paths

    @task(trigger_rule="none_failed_min_one_success")
    def join_summary():
        print("Pipeline execution completed...")

    #Defining the dependencies
    upstream_data=fetch_data()

    branch_decision=check_quality_branch(upstream_data) 


    # define individual paths out of the branches
    alert_path=low_volume_alert()
    parallel_path_a=process_segment_a()
    parallel_path_b=process_segment_b()

    branch_decision >> [alert_path,parallel_path_a,parallel_path_b] >> join_summary()

pipeline_demo()

