import os

import boto3
import pandas as pd

from backend.mapper import Mapper


class S3Service:
    _client = boto3.client("s3", aws_access_key_id=os.environ["S3_ID"], aws_secret_access_key=os.environ["S3_PASSWORD"])
    _bucket = 'monoceros-datasets'
    _path = 'binance/crypto/ALL/bars/1h'
    _extension = 'Parquet'
    _out = 'CSV'

    def request_for_on_set(self, key: str, extension: str = None):
        extension = extension or self._extension
        mapper = Mapper()
        resp = self._client.select_object_content(
            Bucket=self._bucket,
            Key=fr'{self._path}/{key}/{key}.{extension.lower()}',
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
