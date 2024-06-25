import pandas
from flask_restx import Resource, Namespace, fields
from flask import current_app, request

from app.cache import Cache
from app.model import S3Key, Tick, Query
from app.util import convert_class_to_model

ns = Namespace('data-namespace', path='/underlyings')

s3_key = convert_class_to_model(ns, S3Key)
tick = convert_class_to_model(ns, Tick)
query = convert_class_to_model(ns, Query)
query_result = ns.model("QueryResult", {
    "results": fields.List(fields.Nested({
        'name': fields.String,
        'delta': fields.Float,
        'start': fields.Date,
        'end': fields.Date,
    }))
})


class CacheResource(Resource):
    @classmethod
    def get_cache(cls) -> Cache:
        with current_app.app_context():
            return current_app.config['CACHE']


@ns.route('')
class AllController(CacheResource):
    @ns.marshal_with(s3_key)
    def get(self):
        return self.get_cache().s3_objects


@ns.route("/<string:underlying>")
class UnderlyingController(CacheResource):
    @ns.marshal_list_with(tick)
    def get(self, underlying: str):
        return self.get_cache().loaded_data[underlying].to_dict(orient='records')


@ns.route("/do-query")
class QueryController(CacheResource):
    @ns.expect(query)
    @ns.marshal_with(query_result)
    def post(self):
        built_query = Query.build(request.get_json())
        if not built_query.items:
            built_query.items = list(map(lambda key: key.name, self.get_cache().s3_objects))
        to_return = []
        comparison_fn = built_query.direction.get_fn(built_query.size)
        for item in built_query.items:
            df = self.get_cache().loaded_data[item]
            df['delta'] = (df['open'] - df['close'].shift(built_query.num_days)) / df['open']
            df['end'] = df['open_time'].shift(built_query.num_days)
            df.rename({'open_time': 'start'}, axis=1, inplace=True)
            df['name'] = item
            cleaned: pandas.DataFrame = df.loc[comparison_fn(df['delta'])]
            if not cleaned.empty:
                cleaned.to_dict(orient='records')
                to_return.append(cleaned.to_dict(orient='records'))
        return {'results': [ii for i in to_return for ii in i]}
