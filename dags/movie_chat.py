from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator
        )

os.environ['LC_ALL'] = 'C'

with DAG(
     'movie_chat',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='hello world DAG',
    schedule="@yearly",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2023, 12, 31),
    catchup=True,
    tags=['chat', 'movie'],
) as dag:


    def save_json(year):
        from teamchat.call import save_movies, save_json
        import os
        home_path = os.path.expanduser("~")
        data = save_movies(year)
        file_path = f"{home_path}/data/mov_data/{year}_data.json"
        save = save_json(data, file_path)

        print("save json")

        return save


    def branch_fun(year):
        import os
        home_path = os.path.expanduser("~")
        path = os.path.join(home_path, f"{home_path}/data/mov_data/{year}_data.json")
        
        print('*' * 30)
        print(path)
        print('*' * 30)
           
        if os.path.exists(path):
            print('존재')
            return "rm.dir" #rmdir.task_id

        else:
            print('존재x')
            return "save.json"


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")


    branch_op = BranchPythonOperator(
            task_id='branch.op',
            python_callable=branch_fun,
            op_args=["{{logical_date.strftime('%Y')}}"]
            )


    save_json = PythonVirtualenvOperator(
            task_id='save.json',
            python_callable=save_json,
            requirements=["git+https://github.com/asset-No-1/teamchat.git@api/d2.0.0"],
            system_site_packages=False,
            op_args=["{{logical_date.strftime('%Y')}}"],
            trigger_rule="all_done"
            )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command="""
                rm -rf ~/data/mov_data/{{logical_date.strftime('%Y')}}_data.json
            """
            )


    notify_success = BashOperator(
            task_id='notify.success',
            bash_command="""
                echo "notify.success"
                curl -X POST -H 'Authorization: Bearer mo6ux0e446tQ5tcw6gsvHbdAdPdehM0NYvD3XixCjxf' -F 'message=saved success' https://notify-api.line.me/api/notify
            """,
            trigger_rule="all_done"
            )

    notify_fail = BashOperator(
            task_id='notify.fail',
            bash_command="""
                echo "notify.fail"
                curl -X POST -H 'Authorization: Bearer mo6ux0e446tQ5tcw6gsvHbdAdPdehM0NYvD3XixCjxf' -F 'message=try again' https://notify-api.line.me/api/notify
            """,
            trigger_rule='one_failed'
            )


    start >> branch_op
    
    branch_op >> save_json
    branch_op >> rm_dir

    save_json >> notify_success >> end
    save_json >> notify_fail >> end

    rm_dir >> save_json

