from airflow.sdk import dag, task

@dag(dag_id="Pipeline_1")
def first_dag():

    @task(task_id="Job_1")
    def task_1():
        print("This is my demo T1")
    
    @task()
    def task_2():
        print("This is my Second Task")

    @task()
    def task_3():
        print("This is my Third Task")

    t1=task_1()
    t2=task_2()
    t3=task_3()

    t1 >> t2 >> t3

demo_initializer = first_dag()
