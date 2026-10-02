import sqlite3

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(dag_id="Pipeline_Operator")
def operators_demo():

    # BashOperator
    bash_operator = BashOperator(
        task_id="Pinging_Google",
        bash_command="echo google.com",
    )

    # Python TaskFlow task
    @task(task_id="load_sql")
    def run_sql_query():

        # Absolute path inside the Airflow container
        db_path = "/opt/airflow/airflow.db"

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""CREATE TABLE IF NOT EXISTS DEMO_TBL (REPORT_DATE TEXT,USER_SIGNUP INTEGER);""")

        # Insert today's date and signup count
        cursor.execute("""INSERT INTO DEMO_TBL (REPORT_DATE,USER_SIGNUP)VALUES (date('now'),?);""", (100,))

        conn.commit()

        print("SQL Query executed.....")

        # Read data
        cursor.execute("""SELECT * FROM DEMO_TBL;""")

        rows = cursor.fetchall()

        print("\n" + "*" * 40)
        print("DATA NOW")
        print("*" * 40 + "\n")

        for row in rows:
            print(
                f"Date : {row[0]} | "
                f"User Signups : {row[1]}"
            )

        conn.close()

    # IMPORTANT:
    # Calling run_sql_query() creates the actual Airflow task.
    sql_task = run_sql_query()

    # Task dependency
    bash_operator >> sql_task


# Instantiate the DAG
operators_demo()