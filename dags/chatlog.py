from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator,BranchPythonOperator

with DAG(
    'chatlog',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='inspect chatlog and summarize',
    schedule="10 0 * * *",
    start_date=datetime(2024, 8, 23),
    end_date=datetime(2024, 8, 27),
    catchup=True,
    tags=["pyspark","chat"],
) as dag:

    def check_exist():
        import os

        home_path = os.path.expanduser("~")
        if(os.path.exists(f"{home_path}/codes/teamchat/team6_240826_1_messages.json")):
            return "read.success"
        else:
            return "read.fail"

    def process_exist():
        import os

        home_path = os.path.expanduser("~")
        if(os.path.exists(f"{home_path}/codes/teamchat/process.txt")):
            return "process.success"
        else:
            return "process.fail"

    task_process_success = BashOperator(
            task_id="process.success",
            bash_command="""
                    echo "process success"
                    echo "kakao success alarm"
                """
            )

    task_process_fail = BashOperator(
            task_id="process.fail",
            bash_command="""
                    echo "process fail"
                    echo "kakao fail alarm"
                """
            )

    task_process_branch = BranchPythonOperator(
            task_id="process.branch",
            python_callable=process_exist
            )

    task_read_success = BashOperator(
            task_id="read.success",
            bash_command="""
                    echo "read success"
                    echo "kakao success alarm"
                """
            )

    task_read_fail = BashOperator(
            task_id="read.fail",
            bash_command="""
                    echo "read fail"
                    echo "kakao fail alarm"
                """
            )

    task_process = BashOperator(
            task_id="process.chatlog",
            bash_command="""
                    echo "read chat log json"
                    echo "run zeppelin and look stats"
                    echo "export result file"
                """
            )

    task_check_exist = BranchPythonOperator(
            task_id="check.exist",
            python_callable=check_exist
            )

    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")

    task_start >> task_check_exist >> [task_read_success, task_read_fail]
    task_read_success >> task_process
    task_process >> task_process_branch >>[task_process_success, task_process_fail] >> task_end
    task_read_fail >> task_end
