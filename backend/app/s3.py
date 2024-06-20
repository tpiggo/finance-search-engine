import os

import boto3
import pandas as pd

from backend.app.mapper import Mapper
from backend.app.model import S3Key


class S3Service:
    _client = boto3.client("s3", aws_access_key_id=os.environ["S3_ID"], aws_secret_access_key=os.environ["S3_PASSWORD"])
    _bucket = 'monoceros-datasets'
    _path = 'binance/crypto/ALL/bars/1h'
    _out = 'CSV'

    def get_all_sets(self):
        return [S3Key.from_string(item['Key'], self._path)
                for item in self._client.list_objects_v2(Bucket=self._bucket, Prefix=self._path)['Contents']]

    def request_for_set(self, key: str, extension: str):
        mapper = Mapper()
        resp = self._client.select_object_content(
            Bucket=self._bucket,
            Key=fr'{key}',
            ExpressionType='SQL',
            Expression=f'SELECT {mapper.produce_query()} FROM s3object s',
            InputSerialization={extension: {}},
            OutputSerialization={self._out: {}},
        )
        list_of_strings = [event['Records']['Payload'].decode('utf-8')
                           for event in resp['Payload']
                           if 'Records' in event]
        joined = ''.join(list_of_strings).strip()
        return pd.DataFrame([mapper.convert_string_to_object(i, index) for index, i in enumerate(joined.split("\n"))],
                            columns=mapper.get_order())
