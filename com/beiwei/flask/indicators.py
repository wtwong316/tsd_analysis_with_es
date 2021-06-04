from flask_restplus import Resource, Namespace
from flask import request, Response
import json
from flask_restplus import fields
from com.beiwei.indicators.bollingerband import bollinger_band, convert_response_to_df
import pandas as pd
from com.beiwei.plots.plot import plot_bollingerband


name_space_indicators = Namespace('api/indicator', description='技术性指标')

indicators_model = name_space_indicators.model("indicator_post_model",
            {
             'index': fields.String(required=True, description="索引名称"),
             'ts_code': fields.String(required=True, description="基金代码"),
             'field_name': fields.String(required=True, description="字段名称"),
             'date_name': fields.String(required=True, description ="日期名称"),
             'start_date': fields.String(required=False, description="开始日期"),
             'end_date': fields.String(required=False, description="结束日期"),
             'window': fields.String(required=False, description="滑动窗口的天数"),
             'target_index': fields.String(required=False, description="目标索引名称")
             })


@name_space_indicators.route("/get_bollinger_band_width")
class GetBollingerBandWidth(Resource):
    @name_space_indicators.doc(body=indicators_model)
    def post(self):
        index, ts_code, field_name, date_name, start_data, end_date, window, target_index = get_url_parameters(request)
        response = bollinger_band(index, ts_code, field_name, date_name, start_data, end_date, window)
        df, text = convert_response_to_df(response, date_name)
        return Response(df.to_json(), mimetype='application/json')


@name_space_indicators.route("/plot_bollinger_band_width")
class PlotBollingerBandWidth(Resource):
    @name_space_indicators.doc(body=indicators_model)
    def post(self):
        index, ts_code, field_name, date_name, start_data, end_date, window, target_index = get_url_parameters(request)
        response = bollinger_band(index, ts_code, field_name, date_name, start_data, end_date, window)
        df, text = convert_response_to_df(response, date_name)
        title1= ts_code + u'在20200301和20210430之间的布林带'
        title2 = ts_code + u'在20200301和20210430之间的布林带宽度指标'
        return Response(plot_bollingerband(df, title1, title2, date_name, text), mimetype='image/png')


@name_space_indicators.route("/write_bollinger_band_width")
class WriteBollingerBandWidth(Resource):
    @name_space_indicators.doc(body=indicators_model)
    def post(self):
        index, ts_code, field_name, date_name, start_data, end_date, window, target_index = get_url_parameters(request)
        response = bollinger_band(index, ts_code, field_name, date_name, start_data, end_date, window)
        df, text = convert_response_to_df(response, date_name)


def get_url_parameters(req):
    request_data = req.get_json()
    index = request_data['index']
    ts_code = request_data['ts_code']
    field_name = request_data['field_name']
    date_name = request_data['date_name']
    start_date = request_data['start_date'] if 'start_date' in request_data else None
    end_date = request_data['end_date'] if 'end_date' in request_data else None
    window = request_data['window'] if 'window' in request_data else None
    target_index = request_data['target_index'] if 'target_index' in request_data else None
    return index, ts_code, field_name, date_name, start_date, end_date, window, target_index