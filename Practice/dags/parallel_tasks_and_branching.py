from airflow.sdk import task, dag

@dag(dag_id="Parallel_Tasks_and_Branching")
def pipeline_demo():

    @task
    def fetch_data():
        print("Fetching data...")
        return {"records": 250, "status": "success"}


    # Conditional Branching task
    @task.branch
    def check_data_quality(data: dict):
        count = data['records']
        print(f"Evaluating quality for {count} records")
        print(f"Checking data quality for: {data}")
        if count > 2000:
            print("High volume data detected.")
            return ['process_segment_a', 'process_segment_b', 'high_quality_task']
        else:
            print("Low volume data detected.")
            return "low_volume_alert"

    @task
    def low_volume_alert():
        print("Warning : Low volume data detected. Sending alert to the team.")

    @task
    def high_quality_task():
        print("High quality data detected. Proceeding with processing.")


    @task
    def process_segment_a():
        print("Processing segment A of the data...")

    @task
    def process_segment_b():
        print("Processing segment B of the data...")


    # Joining Parallel Paths
    @task(trigger_rule="none_failed_min_one_success")
    def join_summary():
        print("Pipeline completed. Generating summary report...")

    # Defining the task dependencies
    upstream_data = fetch_data()
    branch_decision = check_data_quality(upstream_data)

    # Define individual paths out of the branches
    alert_path = low_volume_alert()
    parallel_path_a = high_quality_task()
    parallel_path_b = process_segment_a()
    parallel_path_c = process_segment_b()

    join_summary = join_summary()


    branch_decision >> [alert_path, parallel_path_a, parallel_path_b, parallel_path_c] >> join_summary

pipeline_demo()