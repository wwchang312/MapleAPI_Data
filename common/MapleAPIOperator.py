from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime
from airflow.providers.odbc.hooks.odbc import OdbcHook
import requests
import json

class MapleApiOperator(BaseOperator):

    template_fields= ('data_nm','ocid','date','character_skill_grade')

    def __init__(self,data_nm,date:str | None=None,ocid: str | None = None,character_skill_grade: str | None = None,**kwargs):
        '''
        메이플스토리 OpenAPI에서 캐릭터 정보를 불러오기 위한 파라미터 입력
        data_nm : 호출하고자 하는 데이터의 API 종류  "/"로 구분
        예시: 캐릭터 목록 조회 api 호출시,
        data_nm = "character/list"
        '''
        super().__init__(**kwargs)
        self.base_url = 'https://open.api.nexon.com/maplestory/v1/'
        self.data_nm = data_nm
        self.headers =  {"x-nxopen-api-key" : Variable.get("x-nxopen-api-key")} #api key는 airflow의 variable로 관리
        self.ocid = ocid
        self.date = date
        self.character_skill_grade = character_skill_grade

    def execute(self, context):

        con = self._call_api(self.base_url, self.data_nm, self.headers, self.date, self.ocid,
                             self.character_skill_grade)

        # 메이플 API에서 제공하고 있는 skill 파라미터에서, 일부 직업은 해당사항이 없어 빈값이 들어오는 경우, 가져오지 않기 위한 로직 추가
        if 'character_skill' in con.keys():
            if not con['character_skill']:
                raise AirflowSkipException("직업에 해당하는 스킬 정보가 존재하지 않습니다.")

        # Mssql Server connect
        hook = OdbcHook(odbc_conn_id='conn-db-mssql-maple',
                        driver="ODBC Driver 18 for SQL Server")  # Airflow connection정보
        sql = "EXEC SP_INSERT_DATA @table_nm = ? , @json =?"

        json = data
        table_nm = self.data_nm.replace('/', '_').replace('-', '_')
        params = (table_nm, json)
        print(f'테이블 {table_nm}으로 데이터를 업데이트합니다.')
        hook.run(sql, parameters=params)

    def _call_api(self, base_url, data_nm, headers, date: str | None = None, ocid: str | None = None,
                  character_skill_grade: str | None = None):

        request_url = base_url + data_nm

        # date를 파라미터로 받을 때, 오늘 날짜는 date 파라미터를 받지 않기 때문에 None으로 처리한다.
        if self.date == datetime.now().strftime("%Y-%m-%d"):
            date = None

        if ocid is not None and date is not None:
            request_url += '?ocid=' + ocid + '&date=' + date
        elif ocid is not None:
            request_url += '?ocid=' + ocid
        elif date is not None:
            request_url += '?' + 'date=' + date

        # character_skill_grade는 캐릭터 스킬 API에서만 사용하는 전직 차수 파라미터이기 때문에, 해당 API를 호출할 때에만 파라미터로 받도록 한다.
        if character_skill_grade is not None:
            request_url += '&character_skill_grade=' + character_skill_grade

        response = requests.get(request_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code}, {response.text}")

        contents = json.loads(response.text)

        # ocid 추가
        if ocid is not None:
            contents['ocid'] = ocid  # ocid를 파라미터로 받는 경우 별도로 받는 ocid 컬럼이 없으므로 임의로 추가함.

        # date 추가
        '''
        조회 당일 날짜의 경우 date의 값이 null이기 때문에 이를 임의로 이전 날짜 형태와 동일하게 생성하여 추가함.
        이력 테이블에 호출 시간 날짜를 별도로 기록하고 있기 때문에, 모든 시간을 00:00으로 통일해도 지장 없다고 판단
        '''
        if 'date' in contents.keys():  # date 파라미터가 있는 경우에만 로직을 타도록 변경
            if contents['date'] is None:
                contents['date'] = datetime.now().strftime("%Y-%m-%dT00:00+09:00")

        return contents