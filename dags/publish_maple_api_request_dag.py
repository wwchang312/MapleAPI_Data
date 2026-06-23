from airflow.sdk import DAG,Param
from airflow.decorators import task
from confluent_kafka import Producer
from datetime import date
from common.utils.date_param import DateParamBuild
from itertools import product
import json
import uuid
import pendulum


with DAG(
        dag_id='publish_maple_api_request_dag',
        schedule=None,
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
                description= "메이플스토리 openapi 중 캐릭터 정보 조회에 대한 API 호출을 시도시, 해당 API 명 입력 \n 입력방식은 character/ability와 같이 api 호출 url의 ''vi/'' 이후 부분 작성 \n 단, id는 제외"
            )
        }
) as dag:

    BROKER_LIST = 'broker01:9092,broker02:9092,broker03:9092'

    @task
    def publish_message(**context):

        job_id = str(uuid.uuid4())
        run_id = context['run_id']
        character_name_lst = context.get('params').get('character_name').split(',')   #복수개의 캐릭터 명 입력시 split
        data_nm_lst = context.get('params').get('data_nm').split(',')
        from_date = context.get('params').get('from_date')
        to_date = context.get('params').get('to_date')
        date_param_lst = DateParamBuild(from_date,to_date)

        for character_name,date_param,data_nm in product(character_name_lst,date_param_lst,data_nm_lst):
            msg = {"job_id" : job_id,
                   "run_id" : run_id,
                   "character_name" : character_name,
                   "date" : date_param,
                   "data_nm" : data_nm
                   }

            producer = Producer({'bootstrap.servers' : BROKER_LIST})

            def delivery_report(err, message):
                if err is not None:
                    raise Exception(f"Message delivery failed: {err}")
                else:
                    print(
                        f"Message delivered to {message.topic()} "
                        f"[partition={message.partition()}] "
                        f"offset={message.offset()}"
                    )


            producer.produce(
                topic='collect_maple_character_list_dag',
                value=json.dumps(msg).encode('utf-8'),
                callback=delivery_report
            )

            producer.flush(10)


    publish_message()


