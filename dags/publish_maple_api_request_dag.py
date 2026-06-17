from airflow import DAG
from airflow.decorators import task
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.sdk import Param
from datetime import datetime,timedelta,date
import json
import uuid
from confluent_kafka import Producer

with DAG(
    dag_id='publish_maple_api_request_dag',
    schedule= None,
    start_date=datetime(2025, 12, 1),
    description='Publish Maple API request',
    tags={"publish_maple_api"},
    params=
        {
        'character_name' : Param(
                type= ['null','string'],
                title= '호출대상 캐릭터명',
                description= "캐릭터명 입력"
        ),
        'from_date' : Param(
                date.today().strftime("%Y-%m-%d"),
                type= ["null","string"],
                format= "date",
                title = "시작날짜",
                description="조회 기준일 시작일자"
        ),
        'to_date' : Param(
                date.today().strftime("%Y-%m-%d"),
                type= ["null","string"],
                format= "date",
                title = "마지막날짜",
                description="조회 기준일 종료일자"
        ),
    }
) as dag:
    # extract character ocid
    def get_ocid_list(**kwargs):
        """
        DAG Params 입력 여부에 따른 호출 대상 캐릭터 선별
        DAG_Maple_Character_List_API로 호출한 캐릭터 목록이 호출 대상이 된다.
        params에 입력된 캐릭터 이름을 기준으로 vw_character_list에서 해당 캐릭터의 ocid를 가져온다.
        아무런 이름이 입력되지 않으면, vw_character_list에 있는 전체가 대상이 된다.
        """
        char_nm = kwargs.get('params').get('character_name')

        hook = OdbcHook(odbc_conn_id='conn-db-mssql-maple',
                        driver="ODBC Driver 18 for SQL Server")  # Airflow connection정보
        sql = "SELECT ocid FROM vw_character_list WHERE 1=1"

        if char_nm:
            clause, params = build_in_clause(char_nm)
            sql += f' AND character_name IN {clause}'

        rows = hook.get_records(sql, parameters=params)
        return_value = [r[0] for r in rows]

        return return_value  # ocid 리스트 형태로 적재


    # view date
    def task_run_from_to_retriever(**kwargs):
        from_date = kwargs.get('params').get('from_date') or date.today().strftime("%Y-%m-%d")
        to_date = kwargs.get('params').get('to_date') or date.today().strftime("%Y-%m-%d")

        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%Y-%m-%d")

        print(f'{from_date}부터 {to_date}까지 정보를 조회합니다.')

        return_value = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in
                        range((to_date - from_date).days + 1)]

        return return_value