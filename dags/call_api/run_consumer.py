from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
import pendulum

with DAG(
        dag_id='run_consumer',
        schedule=None,  # 매주 정각 수행
        start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
        tags=['Maple','Consumer'],
        description="메이플스토리 캐릭터 정보 consumer 실행 DAG",
        catchup=False
) as dag:

    run_consumer = BashOperator(
        task_id = 'run_consumer',
        bash_command='pwd whoami ls -al',
    )