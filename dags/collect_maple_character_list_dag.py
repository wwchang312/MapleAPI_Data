from airflow import DAG
from common.operators import MapleApiOperator
import pendulum

with DAG(
        dag_id='DAG_Maple_Character_List_API',
        schedule='0 0 * * *',  # 매일 정각 수행
        start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
        tags=['Maple', 'Character List'],
        description="메이플스토리 계정별 캐릭터 목록 추출",
        catchup=False
) as dag:

    Maple_Character_List_ETL_Task = MapleApiOperator(
        task_id='Maple_Character_List_ETL_Task',
        data_nm='character/list'
    )
