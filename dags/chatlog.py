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
# 그냥 import notify를 하기에는 모듈을 가져오는 경로(sys.path)에 해당하는 모듈이 없기 때문에
# ModuleNotFoundError가 뜬다
# 그래서 경로를 아래코드로 추가
sys.path.append(os.path.join(os.path.dirname(__file__), '../py'))

# 시스템 챗봇 기능이 있는 notify 모듈 import
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
    #schedule="10 0 * * *",
    schedule_interval=timedelta(hours=12), #8월 27일부터 12시간마다 계속 실행
    start_date=datetime(2024, 8, 27),
    #end_date=datetime(2024, 8, 29),
    catchup=False, #뭔지 공부 필요,
    tags=["pyspark","chat"],
) as dag:
    
    def check_existed():
        import os

        home_path = os.path.expanduser("~")
        airflow_home = os.environ.get("AIRFLOW_HOME", "")
        print(f"{airflow_home}/chat_messages.json")
        if(os.path.exists(f"{airflow_home}/chat_messages.json")):
            return "remove.json"
        else:
            return "scrap.chatlog"

    def check_produced():
        import os

        home_path = os.path.expanduser("~")
        airflow_home = os.environ.get("AIRFLOW_HOME", "")
        print(f"{airflow_home}/chat_messages.json")
        if(os.path.exists(f"{airflow_home}/chat_messages.json")):
            return "read.success"
        else:
            return "read.fail"

    def process_exist():
        import os

        home_path = os.path.expanduser("~")
        airflow_home = os.environ.get("AIRFLOW_HOME", "")
        if(os.path.exists(f"{airflow_home}/output.pdf")):
            return "process.success"
        else:
            return "process.fail"

    task_process_success = PythonOperator(
            task_id="process.success",
            python_callable=producer_alarm,
            op_args=["[Notify!!] 제플린으로 통계보고서를 성공적으로 저장했습니다!"]
            )

    task_process_fail = PythonOperator(
            task_id="process.fail",
            python_callable=producer_alarm,
            op_args=["[Notify!!] 제플린 통계보고서 저장을 실패했습니다!"]
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
                    $SPARK_HOME/bin/spark-submit $AIRFLOW_HOME/py/process.py
                    echo "run zeppelin and look stats"
                    echo "export result file"
                """
            )

    task_check_produced_json = BranchPythonOperator(
            task_id="check.produced_json",
            python_callable=check_produced
            )

    task_scrap_chatlog = BashOperator(
            task_id="scrap.chatlog",
            bash_command="""
                $SPARK_HOME/bin/spark-submit $AIRFLOW_HOME/py/get_json.py
            """,
            trigger_rule="none_failed"
            )

    task_remove_json = BashOperator(
            task_id="remove.json",
            bash_command="""
                    rm $AIRFLOW_HOME/chat_messages.json
                """
            )

    task_check_existed_json = BranchPythonOperator(
            task_id="check.exist_json",
            python_callable=check_existed,
            )

    task_start = BashOperator(
            task_id="start",
            bash_command="echo 12시간마다 실행중",
            )        
    task_end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    task_start >> task_check_existed_json 
    
    task_check_existed_json >> task_remove_json >> task_scrap_chatlog
    task_check_existed_json >> task_scrap_chatlog

    task_scrap_chatlog >> task_check_produced_json >> [task_read_success, task_read_fail]

    task_read_success >> task_process
    
    task_process >> task_process_branch >>[task_process_success, task_process_fail] >> task_end
    
    task_read_fail >> task_end
