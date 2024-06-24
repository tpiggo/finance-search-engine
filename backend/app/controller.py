import pandas
from flask_restx import Resource, Namespace, fields
from flask import current_app

from app.cache import Cache
from app.model import S3Key, Tick
from app.util import convert_class_to_model

ns = Namespace('data-namespace', path='/underlyings')

s3_key = convert_class_to_model(ns, S3Key)

tick = convert_class_to_model(ns, Tick)


@ns.route('')
class AllController(Resource):
    @ns.marshal_with(s3_key)
    def get(self):
        with current_app.app_context():
            cache: Cache = current_app.config['CACHE']
            return cache.s3_objects


@ns.route("/<string:underlying>")
class UnderlyingController(Resource):
    @ns.marshal_list_with(tick)
    def get(self, underlying: str):
        with current_app.app_context():
            cache: Cache = current_app.config['CACHE']
            return cache.loaded_data[underlying].to_dict(orient='records')


@ns.route("/do-query")
class QueryController(Resource):
    def get(self):
        pass
