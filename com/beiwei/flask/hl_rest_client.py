from flask import request
from flask_restplus import Resource, Namespace
from flask_jsonpify import jsonify

from elasticsearch_dsl import Search, Index
from elasticsearch_dsl.query import Q, Match
from elasticsearch_dsl.aggs import A, DateHistogram, MovingFn, Max

from com.beiwei.client.config.hlclient import HLClient

hl_es = HLClient.get_instance()
name_space_hl = Namespace('api/hlclient', description='演示 elasticsearch-dsl')


@name_space_hl.route("/dsl_indexSettings/<index>")
@name_space_hl.doc(params={'index': '索引名称'})
class HLIndexSettings(Resource):
    @staticmethod
    def get(index):
        response = Index(name=index, using=hl_es).get_settings()
        if response is not None:
            return jsonify(response)
        else:
            return {}


@name_space_hl.route("/dsl_search/<index>")
@name_space_hl.doc(params={'index': '索引名称'})
class HLSearch(Resource):

    @staticmethod
    @name_space_hl.doc(params={'field_name': '字段名称', 'field_value': '字段值', 'operator': '逻辑运算符 AND|OR'})
    def post(index):
        field_name = request.args.get('field_name')
        field_value = request.args.get('field_value')
        operator = request.args.get('operator')
        search = Search(index=index, using=hl_es)[0:10]
#       (1)	使用Q方法
#       search.query = Q('match', **{field_name: {'query': field_value, 'operator': operator}})
#       search = search.query(Q('match', ** {field_name: {'query': field_value, 'operator': operator}}))
#       (2)	使用Search提供的query查询方法
#       search = search.query('match', ** {field_name: {'query': field_value, 'operator': operator}})
#       (3) 使用合适的Query类构造函数
        search = search.query(Match(** {field_name: {'query': field_value, 'operator': operator}}))
        search = search.source(['name', 'ts_code'])
        response = search.execute()
        if response['hits']['total']['value'] > 0:
            return jsonify(response.to_dict())
        else:
            return {}


@name_space_hl.route("/dsl_aggs/<index>")
@name_space_hl.doc(params={'index': 'index name'})
class HLAggr(Resource):
    @name_space_hl.doc(params={'ts_code': '基金代码', 'field_name': '字段名称', 'alpha': '0-1', 'beta': '0-1',
                               'gamma': '0-1', 'period': '2', 'window': '滑动窗口'})
    def post(self, index):
        ts_code = request.args.get('ts_code')
        field_name = request.args.get('field_name')
        alpha = request.args.get('alpha')
        beta = request.args.get('beta')
        gamma = request.args.get('gamma')
        period = request.args.get('period')
        window = request.args.get('window')
        search = Search(index=index, using=hl_es)[0:0]
        search = search.filter('term', **{'ts_code.keyword': ts_code})
        daily_report = A(DateHistogram(field='end_date', calendar_interval='1d',  min_doc_count=1))
        daily_adj_nav = A(Max(field='adj_nav'))
        ewma = A(MovingFn(script='MovingFunctions.ewma(values, ' + alpha + ')',  window=window, shift=1,
                                    buckets_path='daily_adj_nav'))
        holt = A(MovingFn(script='MovingFunctions.holt(values, ' + alpha + ',' + beta + ')', window=window,
                                    shift=1, buckets_path='daily_adj_nav'))
        holtwinters = A(MovingFn(script='if (values.length>=5) {MovingFunctions.holtWinters(values,' +
                                            alpha + ',' + beta + ',' + gamma + ',' + period + ',false)}', window=window,
                                            shift=1, buckets_path='daily_adj_nav'))
        search.aggs.bucket('daily_report', daily_report).metric('daily_adj_nav', daily_adj_nav) \
            .pipeline('ewma', ewma).pipeline('holt', holt).pipeline('holtWinters', holtwinters)
        response = search.execute()
        if len(response['aggregations']['daily_report']['buckets']) > 0:
            return jsonify(response.to_dict())
        else:
            return {}
