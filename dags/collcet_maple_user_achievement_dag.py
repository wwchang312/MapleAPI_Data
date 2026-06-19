from airflow.sdk import Dag
from common.operators.MapleApiOperator import MapleApiOperator
import pendulum

with DAG(
        dag_id='collect_maple_user_achievement_dag',
        schedule='0 0 * * 4',  # 매주 목요일 수행
        start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
        tags=['Maple', 'User Achievement'],
        description="메이플스토리 업적 정보",
        catchup=False
) as dag:
    Maple_User_Achievement_ETL_Task = MapleApiOperator(
        task_id='Maple_User_Achievement_ETL_Task',
        data_nm='user/achievement'
    )