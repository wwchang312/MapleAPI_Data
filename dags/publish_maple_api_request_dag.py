from airflow.sdk import DAG,Param
from airflow.decorators import task
from confluent_kafka import Producer
from datetime import date
from common.utils.date_param import DateParamBuild
from common.utils.change_param import ChangeParma
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
                description="미입력시, 전체 캐릭터 정보 조회 API 호출 \n 입력시에는, api의 endpoint 부분을 입력해줌. (ex: character/basic)"
                )
        }
) as dag:

    BROKER_LIST = 'broker01:9092,broker02:9092,broker03:9092'

    @task
    def publish_message(**context):
        #실행정보
        job_id = str(uuid.uuid4())
        run_id = context['run_id']
        #파라미터
        character_name_lst = context.get('params').get('character_name').split(',')   #복수개의 캐릭터 명 입력시 split

        #데이터셋 미지정시 None으로 값 처리
        data_nm_lst = context.get('params').get('data_nm').split(',') if context.get('params').get('data_nm') else None
        # data_nm_builder=ChangeParma(data_nm_lst,'character_info_dataset')
        # data_nm_param_lst=data_nm_builder.mapping_array_alias()

        #입력받은 날짜 계산 및 파라미터 생성
        from_date = context.get('params').get('from_date')
        to_date = context.get('params').get('to_date')
        date_param_builder = DateParamBuild(from_date,to_date)
        date_param_lst=date_param_builder.make_date_list()

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


