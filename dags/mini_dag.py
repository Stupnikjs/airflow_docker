from airflow.operators.bash import BashOperator
from airflow import DAG


dag = DAG(
    'mini_dag',
    # start_date=days_ago(0),
    # schedule_interval='0 0 * * *',
    catchup=False
) 

with dag: 
    first_task = BashOperator(
        task_id ='first_task', 
        bash_command= 'echo michel', 
        dag=dag
    )

    first_task