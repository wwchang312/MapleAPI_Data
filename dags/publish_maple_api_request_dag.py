from airflow.sdk import DAG,Param
from airflow.decorators import task
from confluent_kafka import Producer
from datetime import datetime,timedelta,date
import json
import uuid
import pendulum
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
        catchup=False,
        params={
            "character_name" :Param(
                type = 'string',
                title= "호출 대상 캐릭터명",
                description= "캐릭터 이름 입력"
            ),
            "from_date" : Param(
                date.today().strftime("%Y-%m-%d"),
                type = ["null","string"],
                format = "date",
                title = "조회 시작일",
                description= "조회 기준일 시작일자"
            ),
            "to_date" : Param(
                date.today().strftime("%Y-%m-%d"),
                type = ["null","string"],
                format = "date",
                title = "조회 종료일",
                description= "조회 기준일 마지막일자"
            ),
            "data_nm" : Param(
                type = ["null","string"],
                title = "호출 데이터셋 지정",
                description= "특정 dataset에 대하여 API 호출을 시도시, 해당 API 명 입력 \n 입력방식은 character/ability와 같이 api 호출 url의 ''vi/'' 이후 부분 작성"
            )
        }
) as dag:

    BROKER_LIST = 'broker01:9092,broker02:9092,broker03:9092'

    @task
    def publish_message(**context):

        job_id = str(uuid.uuid4())
        run_id = context['run_id']

        message = {
            "job_id" : job_id,
            "run_id" : run_id,
            "api_name" : 'collect_maple_character_list_dag',
            "character_name" : context.get('params').get('character_name'),
            "from_date" : context.get('params').get('from_date'),
            "to_date" : context.get('params').get('to_date'),
            "data_nm" : context.get('params').get('data_nm')
        }

        producer = Producer({'bootstrap.servers' : BROKER_LIST})

        def delivery_report(err, msg):
            if err is not None:
                raise Exception(f"Message delivery failed: {err}")
            else:
                print(
                    f"Message delivered to {msg.topic()} "
                    f"[partition={msg.partition()}] "
                    f"offset={msg.offset()}"
                )


        producer.produce(
            topic='collect_maple_character_list_dag',
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )

        producer.flush(10)


    publish_message()


