from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
#from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
#from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

import sys 
import os

# Add the directory containing notify.py to the Python path
# 그냥 import notify를 하기에는 모듈을 가져오는 경로(sys.path)에 해당하는 모듈이 없기 때문에 ModuleNotFoundError가 뜬다
# 그래서 경로를 아래코드로 추가
sys.path.append(os.path.join(os.path.dirname(__file__), '../py'))

from notify import producer_alarm

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

    task_process_success = PythonOperator(
            task_id="process.success",
            python_callable=producer_alarm,
            op_args=["[Notify!!] print the result of chatlog stats successful!"]
            )

    task_process_fail = PythonOperator(
            task_id="process.fail",
            python_callable=producer_alarm,
            op_args=["[Notify!!] print the result of chatlog stats failed!"]
            )

    task_process_branch = BranchPythonOperator(
            task_id="process.branch",
            python_callable=process_exist
            )

    task_read_success = PythonOperator(
            task_id="read.success",
            python_callable=producer_alarm,
            op_args=["[Notify!!] 채팅 로그 json file을 성공적으로 읽었습니다!"] 
            )

    task_read_fail = PythonOperator(
            task_id="read.fail",
            python_callable=producer_alarm,
            op_args=["[Notify!!] 채팅 로그 json file을 읽지 못했습니다!"]
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
            python_callable=check_exist,
            trigger_rule="one_failed"
            )

    task_scrap_chatlog = BashOperator(
            task_id="scrap.chatlog",
            bash_command="""
                $SPARK_HOME/bin/spark-submit ~/codes/airflow_chat/py/get_json.py
            """
            )

    task_start = EmptyOperator(task_id="start")        
    task_end = EmptyOperator(task_id="end")

    task_start >> task_scrap_chatlog >> task_check_exist >> [task_read_success, task_read_fail]
    task_read_success >> task_process
    task_process >> task_process_branch >>[task_process_success, task_process_fail] >> task_end
    task_read_fail >> task_end
