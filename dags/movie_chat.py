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
    schedule="10 2 * * *",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    tags=['chat', 'movie'],
) as dag:


    def save_json(year):
        from teamchat.call import list2df, save_json
        import os
        home_path = os.path.expanduser("~")
        data = list2df(year)
        file_path = f"{home_path}/data/mov_data/year={year}/data.json"
        save = save_json(data, file_path)

        print("save json")

        return save


       def branch_fun(year):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"{home_path}/data/mov_data/year={year}/data.json")
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
    end = EmptyOperator(task_id='end')


    branch_op = BranchPythonOperator(
            task_id='branch.op',
            python_callable=branch_fun
            )


    save_json = EmptyOperator(task_id='save.json')

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command="""
                rm -rf ~/data/mov_data/year={{logical_date.strftime('%Y')}}
            """
            )


    start >> branch_op
    
    branch_op >> save_json
    branch_op >> rm_dir

    save_json >> end
    rm_dir >> end

