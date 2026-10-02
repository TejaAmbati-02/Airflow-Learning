import sqlite3
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="Pipeline_Operator")
def operators_demo():

    jaswinder = BashOperator(
        task_id="Pinging_Data-2-dollars",
        bash_command="echo https://www.youtube.com/@data2dollars"
    )

    @task(task_id="load_sql")
    def run_sql_query():
        db_path = "/opt/airflow/airflow.db"  # Fixed missing leading slash
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()  # Fixed typo from cursor-con.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DEMO_TBL(
                REPORT_DATE TEXT,
                USER_SIGNUP INTEGER
            );
            """
        )

        cursor.execute(
            """
            INSERT INTO DEMO_TBL (REPORT_DATE,USER_SIGNUP)
            VALUES (date('now'),150);
            """
        )
        conn.commit()
        print("sql query executed....")

        cursor.execute("SELECT * FROM DEMO_TBL;")
        rows = cursor.fetchall()

        print("\n" + "="*40)
        print("DATA NOW")
        print("="*40)
        for row in rows:
            print(f"Date: {row[0]} | SignUps: {row[1]}")
        print("="*40 + "\n")

        conn.close()

    jaswinder >> run_sql_query()

operators_demo()