from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk import Variable
from datetime import datetime
import json
import boto3


class MapleApiOperator(BaseOperator):
    template_fields = ['data_nm']

    def __init__(self, data_nm, **kwargs):
        """
        data_nm : 호출하고자 하는 데이터의 API 종류  "/"로 구분
        예시: 캐릭터 목록 조회 api 호출시,
        data_nm = "character/list"
        """
        super().__init__(**kwargs)
        self.base_url = 'https://open.api.nexon.com/maplestory/v1/'
        self.data_nm = data_nm
        self.headers = {"x-nxopen-api-key": Variable.get("x-nxopen-api-key")}



    def execute(self, context):

        con = self._call_api(self.base_url, self.data_nm, self.headers)
        data = self.json_dumping(con)

        #minio 연결
        s3= boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=Variable.get("minio-access-key"),
            aws_secret_access_key=Variable.get("minio-secret-key")
        )

        logical_date=context['logical_date'].strftime("%Y%m%d")

        # MinIO 저장
        s3.put_object(
            Bucket="maple-character-api",
            Key=f"{self.data_nm}/{logical_date}/data.json",
            Body=data,
            ContentType="application/json"
        )


    def _call_api(self, base_url, data_nm, headers):
        import requests

        request_url = base_url + data_nm

        response = requests.get(request_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"API request failed: {response.status_code}, {response.text}")

        contents = json.loads(response.text)

        return contents

    ## json 문자열 dumping
    def json_dumping(self, contents: dict):

        empty_list = []

        if len(contents.keys()) == 1:
            for v in contents.values():
                empty_list.extend(v)

        else:
            empty_list = contents

        json_str = json.dumps(empty_list, ensure_ascii=False).encode('utf-8')

        return json_str