from concurrent.futures import ThreadPoolExecutor, wait
import pandas as pd

from app.s3 import S3Service

from app.model import S3Key, update_dataframe_for_date_limits


class Cache:
    def __init__(self, keys: list[S3Key]):
        self.s3_objects: list[S3Key] = keys
        self.loaded_data: dict[str, pd.DataFrame] = {}

    def load_for(self, keys: list[S3Key], s3: S3Service):
        def load_key(_key: S3Key):
            print(f'Loading data from {_key.name}')
            return _key.name, update_dataframe_for_date_limits(s3.request_for_set(_key.key, _key.extension_name))

        with ThreadPoolExecutor(max_workers=10) as pool:
            for key, value in pool.map(load_key, keys):
                self.loaded_data[key] = value

    def load_cache(self, s3: S3Service):
        self.load_for(self.s3_objects, s3)
