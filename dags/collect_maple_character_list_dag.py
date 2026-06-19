from airflow.sdk import DAG
from common.operators.MapleApiOperator import MapleApiOperator
import pendulum

with DAG(
        dag_id='collect_maple_character_list_dag',
        schedule='0 0 * * *',  # 매일 정각 수행
        start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
        tags=['Maple', 'Character List'],
        description="메이플스토리 계정별 캐릭터 목록 추출",
        catchup=False
) as dag:
    '''
    메이플스토리 Open API 중 계정정보와 관련된 사항은 별도의 파라미터를 요구하지 않는다.
    또한 캐릭터의 게임내 정보와는 직접적인 연관이 없고, 캐릭터의 추가/삭제가 수시로 발생하지 않는다는점
    이 두가지 사항을 고려하여, 매일 하루 한번 배치로 데이터를 호출하여, 업데이트를 수행한다.
    '''
    Maple_Character_List_ETL_Task = MapleApiOperator(
        task_id='Maple_Character_List_ETL_Task',
        data_nm='character/list'
    )
