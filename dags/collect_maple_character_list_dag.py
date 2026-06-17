from airflow import DAG
import pendulum
from airflow.decorators import task
from confluent_kafka import Producer
import uuid
import json

'''
캐릭터 리스트는 별도의 입력 파라미터를 요구하지 않으므로 매일 단독 배치로 호출하여 갱신한다.
캐릭터 상세 조회 시 매번 캐릭터명으로 API를 호출해 ocid를 조회하는 대신,
DB에 저장된 character_list 테이블에서 캐릭터명에 해당하는 ocid를 조회하도록 구성한다.
이를 통해 불필요한 API 호출을 줄이고, 호출 제한 부담과 응답 지연을 낮출 수 있다.
'''

with DAG(
        dag_id='collect_maple_character_list_dag',
        schedule='0 0 * * *',  # 매일 정각마다.
        start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
        tags=['Maple', 'Character List'],
        description="메이플스토리 계정별 캐릭터 목록 추출",
        catchup=False
) as dag:

    BROKER_LIST = 'broker01:9092,broker02:9092,broker03:9092'

    @task
    def publish_message(**context):

        job_id = str(uuid.uuid4())
        run_id = context['run_id']

        message = {
            "job_id" : job_id,
            "run_id" : run_id,
            "api_name" : 'collect_maple_character_list_dag'
        }

        producer = Producer({'bootstrap_servers' : BROKER_LIST})

        producer.produce(
            topic='maple_character_list',
            value=json.dumps(message).encode('utf-8')
        )

        producer.poll(0)

        producer.flush()


    publish_message()


