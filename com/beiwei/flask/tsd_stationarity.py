from flask_restplus import Resource, Namespace
from flask import request, Response
from flask_jsonpify import jsonify
from flask_restplus import fields
from com.beiwei.es.search import es_get_data
from com.beiwei.plots.plot import plot_data, plot_rolling_mean, plot_rolling_std, plot_data_w_text, \
    plot_seasonal_decompose
from com.beiwei.stats.stats import adf_test, kpss_test, decompose
import numpy as np
import json

name_space_tsd_stationarity = Namespace('api/tsd_stationarity', description='演示时间序列数据的均值，方差，协方差')

tsd_stationarity_model = name_space_tsd_stationarity.model("tsd_stationary_post_model",
            {
             'index': fields.String(required=True, description="索引名称"),
             'ts_code': fields.String(required=True, description="基金代码"),
             'field_name': fields.String(required=True, description="字段名称"),
             'date_name': fields.String(required=True, description ="日期名称"),
             'start_date': fields.String(required=False, description="开始日期"),
             'end_date': fields.String(required=False, description="结束日期")
             })

@name_space_tsd_stationarity.route("/plot_data")
class PlotData(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        title = ts_code + ' data plot'
        return Response(plot_data(df, title, field_name, date_name), mimetype='image/png')


@name_space_tsd_stationarity.route("/mean")
class Mean(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        title = ts_code + ' rolling mean data plot'
        return Response(plot_rolling_mean(df, title, field_name, date_name), mimetype='image/png')


@name_space_tsd_stationarity.route("/std")
class Std(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        title = ts_code + ' rolling std data plot'
        return Response(plot_rolling_std(df, title, field_name, date_name), mimetype='image/png')


@name_space_tsd_stationarity.route("/adfuller")
class Adfuller(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        return jsonify(adf_test(df[field_name]))


@name_space_tsd_stationarity.route("/kpss")
class Kpss(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        return jsonify(kpss_test(df[field_name]))


@name_space_tsd_stationarity.route("/detrend_by_1o_diff")
class DetrendByFirstDiff(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        df['1_diff'] = df[field_name].diff()
        df_1_diff = df['1_diff'].dropna()
        report = {}
        report['adfuller'] = adf_test(df_1_diff)
        report['kpss'] =  kpss_test(df_1_diff)
        title = ts_code + ' detrend by 1st order diff'
        text = json.dumps(report, sort_keys=True, indent=1)
        return Response(plot_data_w_text(df, title, '1_diff', date_name, text), mimetype='image/png')



@name_space_tsd_stationarity.route("/detrend_by_2o_diff")
class DetrendBySecondDiff(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        df['2_diff'] = df[field_name].diff().diff()
        df_2_diff = df['2_diff'].dropna()
        report = {}
        report['adfuller'] = adf_test(df_2_diff)
        report['kpss'] = kpss_test(df_2_diff)
        title = ts_code + ' detrend by 2nd order diff'
        text = json.dumps(report, sort_keys=True, indent=1)
        return Response(plot_data_w_text(df, title, '2_diff', date_name, text), mimetype='image/png')


@name_space_tsd_stationarity.route("/detrend_by_3o_diff")
class DetrendByThirdDiff(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        df['3_diff'] = df[field_name].diff().diff().diff()
        df_3_diff = df['3_diff'].dropna()
        report = {}
        report['adfuller'] = adf_test(df_3_diff)
        report['kpss'] = kpss_test(df_3_diff)
        title = ts_code + ' detrend by_3rd order diff'
        text = json.dumps(report, sort_keys=True, indent=1)
        return Response(plot_data_w_text(df, title, '3_diff', date_name, text), mimetype='image/png')


@name_space_tsd_stationarity.route("/detrend_by_log_diff")
class DetrendByLogDiff(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        df_log = np.log(df[field_name])
        df['log_diff'] = df_log - df_log.shift()
        df_log_diff = df['og_diff'].dropna()
        report = {}
        report['adfuller'] = adf_test(df_log_diff)
        report['kpss'] = kpss_test(df_log_diff)
        title = ts_code + ' detrend_by_log'
        text = json.dumps(report, sort_keys=True, indent=1)
        return Response(plot_data_w_text(df, title, 'log_diff', date_name, text), mimetype='image/png')


@name_space_tsd_stationarity.route("/seasonal_decompose")
class SeasonalDecompose(Resource):
    @name_space_tsd_stationarity.doc(body=tsd_stationarity_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        df.set_index('end_date')
        trend, seasonal, residual = decompose(df[field_name])
        df['trend'] = trend
        df['seasonal'] = seasonal
        df['residual'] = residual
        title = ts_code + ' seasonal decomposition'
        if (residual == 0).all():
            text = None
        else:
            report = {}
            report['adfuller'] = adf_test(residual)
            report['kpss'] = kpss_test(residual)
            text = json.dumps(report, sort_keys=True, indent=1)
        return Response(plot_seasonal_decompose(df, title, field_name, date_name, text), mimetype='image/png')


def get_data(request):
    request_data = request.get_json()
    index = request_data['index']
    ts_code = request_data['ts_code']
    field_name = request_data['field_name']
    date_name = request_data['date_name']
    start_date = request_data['start_date'] if 'start_date' in request_data else None
    end_date = request_data['end_date'] if 'end_date' in request_data else None
    df = es_get_data(index, ts_code, field_name, date_name, start_date, end_date)
    return df, ts_code, field_name, date_name, start_date, end_date

