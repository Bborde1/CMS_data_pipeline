from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "ds4a_capstone_run",
    default_args = {
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "end_date=": datetime(2023, 7, 15)
    },
    description= "Capstone airflow",
    schedule=timedelta(hours=4),
    start_date=datetime(2023, 7, 9)
) as dag:
    t1=BashOperator(
        task_id="find_directory",
        bash_command = "cd DS4A_Team_14_Capstone"
    )

    t2= BashOperator(
        task_id = "ensure_reqs",
        bash_command = "python3 -m pip install requirements.txt"
    )

    t3 = BashOperator(
        task_id = "extract",
        bash_command = "python3 extraction_demo.py"
    )

    t4 = BashOperator(
        task_id = "transform",
        bash_command = "python3 transform.py"

    )

t1 >> t2 >> t3 >> t4