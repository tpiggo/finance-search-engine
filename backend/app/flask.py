import pandas as pd
from flask import Flask, current_app
from flask_cors import CORS

from backend.app.model import S3Key, update_dataframe_for_date_limits
from backend.app.s3 import S3Service


class Cache:
    def __init__(self, keys: list[S3Key]):
        self.s3_objects: list[S3Key] = keys
        self.loaded_data: dict[str, pd.DataFrame] = {}

    def load_for(self, keys: list[S3Key], s3: S3Service):
        for key in keys:
            current_app.logger.info(f'Loading data from {key.name}')
            self.loaded_data[key.name] = update_dataframe_for_date_limits(s3.request_for_set(key.key,
                                                                                             key.extension_name))

    def load_cache(self, s3: S3Service):
        self.load_for(self.s3_objects, s3)


class Config:
    S3 = S3Service()
    CACHE = Cache(S3.get_all_sets())

    def __init__(self):
        self.CACHE.load_cache(self.S3)


def create_app():
    app = Flask(__name__)
    with app.app_context():
        app.config.from_object(Config())
    CORS(app)

    # Route for getting data from a query
    @app.route('/', methods=['GET'])
    def get_data():
        pass

    return app
