from flask_restplus import Resource, Namespace
from flask import request, Response
import json
from flask_restplus import fields
from com.beiwei.indicators.bollingerband import bollinger_band, iex_bollinger_band, iex_bollinger_band_trend, \
        bollinger_band_trend, convert_response_to_df, iex_convert_response_to_df, \
        iex_convert_bbtrend_response_to_df, convert_bbtrend_response_to_df
from com.beiwei.indicators.macd import macd, iex_macd, iex_macd_bb, convert_macd_response_to_df, \
        convert_macd_bb_response_to_df, macd_bb
from com.beiwei.indicators.rsi import rsi_bb, iex_rsi_bb, convert_rsi_bb_response_to_df
from com.beiwei.plots.plot import plot_bollingerband, iex_plot_bbtrend, plot_bbtrend, \
        plot_macd, plot_macd_bb, plot_rsi_bb

from datetime import datetime, timedelta

from flask_restplus import marshal


name_space_indicators = Namespace('api/indicator', description='技术性指标')

indicators_model = name_space_indicators.model("indicator_post_model",
            {
             'index': fields.String(required=True, description="索引名称",  example='nav'),
             'fund_code': fields.String(required=True, description="基金代码", example='010685.OF'),
             'start_date': fields.String(required=False, description="开始日期", example='20210201'),
             'end_date': fields.String(required=False, description="结束日期", example='20210430'),
             'window': fields.Integer(required=False, description="滑动窗口的天数", example=20),
             'target_index': fields.String(required=False, description="目标索引名称")
             })

iex_indicators_model = name_space_indicators.model("iex_indicator_post_model",
            {
             'index': fields.String(required=True, description="索引名称",  example='fidelity28_fund'),
             'fund_code': fields.String(required=True, description="基金代码", example='FDEV'),
             'start_date': fields.String(required=False, description="开始日期", example='2021-02-01'),
             'end_date': fields.String(required=False, description="结束日期", example='2021-05-31'),
             'window': fields.Integer(required=False, description="滑动窗口的天数", example=20),
             'target_index': fields.String(required=False, description="目标索引名称")
             })


macd_histogram_model = name_space_indicators.model("histogram_post_model",
            {
             'index': fields.String(required=True, description="索引名称",  example='nav'),
             'fund_code': fields.String(required=True, description="基金代码", example='010685.OF'),
             'start_date': fields.String(required=False, description="开始日期", example='20210201'),
             'end_date': fields.String(required=False, description="结束日期", example='20210430'),
             'target_index': fields.String(required=False, description="目标索引名称")
             })

iex_macd_histogram_model = name_space_indicators.model("iex_histogram_post_model",
            {
             'index': fields.String(required=True, description="索引名称",  example='fidelity28_fund'),
             'fund_code': fields.String(required=True, description="基金代码", example='FDEV'),
             'start_date': fields.String(required=False, description="开始日期", example='2021-02-01'),
             'end_date': fields.String(required=False, description="结束日期", example='2021-05-31'),
             'target_index': fields.String(required=False, description="目标索引名称")
             })

rsi_bb_model = name_space_indicators.model("rsi_bb_post_model",
            {
             'index': fields.String(required=True, description="索引名称", example='nav'),
             'fund_code': fields.String(required=True, description="基金代码", example='010685.OF'),
             'start_date': fields.String(required=False, description="开始日期", example='20210201'),
             'end_date': fields.String(required=False, description="结束日期", example='20210430'),
             'rsi_period': fields.Integer(required=True, description="RSI Period", example=14),
             'sma_period': fields.Integer(required=True, description="SMA Period", example=20),
             'target_index': fields.String(required=False, description="目标索引名称")
             })

iex_rsi_bb_model = name_space_indicators.model("iex_rsi_bb_post_model",
            {
             'index': fields.String(required=True, description="索引名称", example='fidelity28_fund'),
             'fund_code': fields.String(required=True, description="基金代码", example='FDEV'),
             'start_date': fields.String(required=False, description="开始日期", example='2021-02-01'),
             'end_date': fields.String(required=False, description="结束日期", example='2021-05-31'),
             'rsi_period': fields.Integer(required=True, description="RSI Period", example=14),
             'sma_period': fields.Integer(required=True, description="SMA Period", example=20),
             'target_index': fields.String(required=False, description="目标索引名称")
             })


@name_space_indicators.route("/get_bollinger_band_width")
class GetBollingerBandWidth(Resource):
    @name_space_indicators.doc(indicators_model)
    def post(self):
        index, fund_code, start_date, end_date, window, target_index = \
            get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = bollinger_band(index, fund_code, new_start_date, end_date, window)
        df, text = convert_response_to_df(response,  start_date)
        return Response(df.to_json(), mimetype='application/json')


@name_space_indicators.route("/plot_bollinger_band_width")
class PlotBollingerBandWidth(Resource):
    @name_space_indicators.doc(body=indicators_model)
    def post(self):
        index, fund_code,  start_date, end_date, window, target_index = \
            get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = bollinger_band(index, fund_code,  new_start_date, end_date, window)
        df, avg_BBW = convert_response_to_df(response,  start_date)
        title1= u'基金代码 ' + fund_code + u'在2021-02-01和2021-04-30之间的布林带'
        title2 = u'基金代码 ' + fund_code + u'在2021-02-01和2021-04-30之间的布林带宽度'
        return Response(plot_bollingerband(df, title1, title2, 'end_date', avg_BBW), mimetype='image/png')


@name_space_indicators.route("/plot_bollinger_band_trend")
class PlotBollingerBandTrend(Resource):
    @name_space_indicators.doc(body=indicators_model)
    def post(self):
        index, fund_code, start_date, end_date, window, target_index = \
            get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=62)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = bollinger_band_trend(index, fund_code,  new_start_date, end_date, window)
        df = convert_bbtrend_response_to_df(response, 'end_date', start_date)
        title1= u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间20天和50天的布林带'
        title2 = u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间的布林带趋势'
        return Response(plot_bbtrend(df, title1, title2, 'end_date'), mimetype='image/png')


@name_space_indicators.route("/plot_iex_bollinger_band_width")
class PlotIEXBollingerBandWidth(Resource):
    @name_space_indicators.doc(body=iex_indicators_model)
    def post(self):
        index, fund_code, start_date, end_date, window, target_index = \
            get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y-%m-%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y-%m-%d')
        response = iex_bollinger_band(index, fund_code, new_start_date, end_date, window)
        df, avg_BBW = iex_convert_response_to_df(response,start_date)
        title1=  u'Bollinger Band (BB) Between 2021-02-01 and 2021-04-30 for ' + fund_code + u' ETF'
        title2 = u'Bollinger Band Width (BBW) Between 2021-02-01 and 2021-04-30 for ' + fund_code + u' ETF'
        return Response(plot_bollingerband(df, title1, title2, 'date', avg_BBW), mimetype='image/png')


@name_space_indicators.route("/plot_iex_bollinger_band_trend")
class PlotIEXBollingerBandTrend(Resource):
    @name_space_indicators.doc(body=iex_indicators_model)
    def post(self):
        index, fund_code, start_date, end_date, window, target_index = \
            get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y-%m-%d')
        new_start_datetime = start_datetime - timedelta(days=90)
        new_start_date = new_start_datetime.strftime('%Y-%m-%d')
        response = iex_bollinger_band_trend(index, fund_code, new_start_date, end_date, window)
        df = iex_convert_bbtrend_response_to_df(response, 'date', start_date)
        title1=  u'Bollinger Band (BB) Between 2021-03-01 and 2021-05-31 for ' + fund_code + u' ETF'
        title2 = u'Bollinger Band Trend (BBTrend) Between 2021-03-01 and 2021-05-31 for ' + fund_code + u' ETF'
        return Response(iex_plot_bbtrend(df, title1, title2, 'date'), mimetype='image/png')


@name_space_indicators.route("/write_bollinger_band_width")
class WriteBollingerBandWidth(Resource):
    @name_space_indicators.doc(body=indicators_model)
    def post(self):
        index, fund_code, start_date, end_date, window, target_index = get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = bollinger_band(index, fund_code, field_name,  new_start_date, end_date, window)
        df, text = convert_response_to_df(response,  start_date)


@name_space_indicators.route("/plot_macd_histogram")
class PlotMACDHistogram(Resource):
    @name_space_indicators.doc(body=macd_histogram_model)
    def post(self):
        index, fund_code,  start_date, end_date, window, target_index = \
            get_url_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = macd(index, fund_code, new_start_date, end_date)
        df = convert_macd_response_to_df(response, 'end_date', start_date)
        title0= u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间的Daily和MACD'
        title1= u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间的MACD和signal'
        title2 = u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间的MACD-Histogram'
        return Response(plot_macd(df, title0, title1, title2, 'end_date'), mimetype='image/png')


@name_space_indicators.route("/plot_macd_bb")
class PlotMACDBB(Resource):
    @name_space_indicators.doc(body=macd_histogram_model)
    def post(self):
        index, fund_code, start_date, end_date, target_index = \
            get_macd_histogram_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = macd_bb(index, fund_code, new_start_date, end_date)
        df = convert_macd_bb_response_to_df(response, 'end_date', start_date)
        title0= u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间的MACD和MACD BB'
        title1= u'基金代码 ' + fund_code + u'在2021-01-01和2021-04-30之间的MACD和Daily'
        return Response(plot_macd_bb(df, title0, title1, 'end_date'), mimetype='image/png')


@name_space_indicators.route("/plot_rsi_bb")
class PlotRSIBB(Resource):
    @name_space_indicators.doc(body=rsi_bb_model)
    def post(self):
        index, fund_code, start_date, end_date, rsi_period, sma_period, target_index = \
            get_rsi_bb_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y%m%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y%m%d')
        response = rsi_bb(index, fund_code, new_start_date, end_date, rsi_period, sma_period)
        df = convert_rsi_bb_response_to_df(response, 'end_date', start_date)
        title0=u'基金代码 ' + fund_code +  u'在2021-01-01和2021-04-30之间的RSI and RSI BB'
        title1=u'基金代码 ' + fund_code +  u'在2021-01-01和2021-04-30之间的Daily adj_nav and RSI'
        title2 =u'基金代码 ' + fund_code +  u'在2021-01-01和2021-04-30之间的Daily and BB'
        return Response(plot_rsi_bb(df, title0, title1, title2, 'end_date'), mimetype='image/png')


@name_space_indicators.route("/plot_iex_macd_bb")
class PlotIEXMACDBB(Resource):
    @name_space_indicators.doc(body=iex_macd_histogram_model)
    def post(self):
        index, fund_code, start_date, end_date, target_index = \
            get_macd_histogram_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y-%m-%d')
        new_start_datetime = start_datetime - timedelta(days=45)
        new_start_date = new_start_datetime.strftime('%Y-%m-%d')
        response = iex_macd_bb(index, fund_code, new_start_date, end_date)
        df = convert_macd_bb_response_to_df(response, 'date', start_date)
        title0= u'MACD and MACD BB between 2021-02-01 and 2021-05-31 for ' + fund_code + u' ETF'
        title1= u'Daily typical price and MACD between 2021-02-01 and 2021-05-31 for ' + fund_code + u' ETF'
        return Response(plot_macd_bb(df, title0, title1, 'date'), mimetype='image/png')


@name_space_indicators.route("/plot_iex_macd_histogram")
class PlotIEXMACDHistogram(Resource):
    @name_space_indicators.doc(body=iex_macd_histogram_model)
    def post(self):
        index, fund_code, start_date, end_date, target_index = \
            get_macd_histogram_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y-%m-%d')
        new_start_datetime = start_datetime - timedelta(days=45)
        new_start_date = new_start_datetime.strftime('%Y-%m-%d')
        response = iex_macd(index, fund_code, new_start_date, end_date)
        df = convert_macd_response_to_df(response, 'date', start_date)
        title0= u'Daily typical price and MACD between 2021-01-15 and 2021-05-31 for ' + fund_code + u' ETF'
        title1= u'MACD and Signal between 2021-01-15 and 2021-05-31 for ' + fund_code + u' ETF'
        title2 = u'MACD Histogram between 2021-01-15 and 2021-05-31 for ' + fund_code + u' ETF'
        return Response(plot_macd(df, title0, title1, title2, 'date'), mimetype='image/png')


@name_space_indicators.route("/plot_iex_rsi_bb")
class PlotIEXRSIBB(Resource):
    @name_space_indicators.doc(body=iex_rsi_bb_model)
    def post(self):
        index, fund_code, start_date, end_date, rsi_period, sma_period, target_index = \
            get_rsi_bb_parameters(request)
        start_datetime = datetime. strptime(start_date, '%Y-%m-%d')
        new_start_datetime = start_datetime - timedelta(days=30)
        new_start_date = new_start_datetime.strftime('%Y-%m-%d')
        response = iex_rsi_bb(index, fund_code, new_start_date, end_date, rsi_period, sma_period)
        df = convert_bb_response_to_df(response, 'date', start_date)
        title0= u'RSI and RSI BB between 2021-02-01 and 2021-05-31 for ' + fund_code + u' ETF'
        title1= u'Daily typical price and RSI between 2021-02-01 and 2021-05-31 for ' + fund_code + u' ETF'
        title2 = u'Daily and BB between 2021-02-01 and 2021-05-31 for ' + fund_code + u' ETF'
        return Response(plot_rsi_bb(df, title0, title1, title2, 'date'), mimetype='image/png')


def get_url_parameters(req):
    request_data = req.get_json()
    index = request_data['index']
    fund_code = request_data['fund_code']
    start_date = request_data['start_date'] if 'start_date' in request_data else None
    end_date = request_data['end_date'] if 'end_date' in request_data else None
    window = request_data['window'] if 'window' in request_data else None
    target_index = request_data['target_index'] if 'target_index' in request_data else None
    return index, fund_code, start_date, end_date, window, target_index


def get_macd_histogram_parameters(req):
    request_data = req.get_json()
    index = request_data['index']
    fund_code = request_data['fund_code']
    start_date = request_data['start_date'] if 'start_date' in request_data else None
    end_date = request_data['end_date'] if 'end_date' in request_data else None
    target_index = request_data['target_index'] if 'target_index' in request_data else None
    return index, fund_code, start_date, end_date, target_index


def get_rsi_bb_parameters(req):
    request_data = req.get_json()
    index = request_data['index']
    fund_code = request_data['fund_code']
    start_date = request_data['start_date'] if 'start_date' in request_data else None
    end_date = request_data['end_date'] if 'end_date' in request_data else None
    rsi_period = request_data['rsi_period'] if 'rsi_period' in request_data else None
    sma_period = request_data['sma_period'] if 'sma_period' in request_data else None
    target_index = request_data['target_index'] if 'target_index' in request_data else None
    return index, fund_code, start_date, end_date, rsi_period, sma_period, target_index



