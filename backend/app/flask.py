import os
from logging import INFO

from flask import Flask, Blueprint
from flask_cors import CORS
from flask_restx import Api

from .cache import Cache
from .controller import ns

from app.s3 import S3Service


class Config:
    S3 = S3Service()
    should_load = os.environ.get('SHOULD_LOAD', 'true').lower() == 'true'
    CACHE = Cache(S3.get_all_sets())

    def __init__(self):
        self.CACHE.load_cache(self.S3)


def create_app():
    app = Flask(__name__)
    with app.app_context():
        app.config.from_object(Config())
        app.logger.setLevel(INFO)
    CORS(app)
    bp = Blueprint('api', __name__, url_prefix='/api/v1')
    api = Api(bp)
    api.add_namespace(ns)
    app.register_blueprint(bp)
    return app
