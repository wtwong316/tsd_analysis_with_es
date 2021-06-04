from flask_restplus import Resource, Namespace
from flask_restplus import fields
from flask import request, Response
from flask_jsonpify import jsonify
from com.beiwei.es.search import es_get_data
from com.beiwei.stats.eemd import eemd_analysis, ceemd_analysis
from com.beiwei.plots.plot import plot_eemd
from com.beiwei.es.data_in import indexing_df

name_space_tsd_eemd = Namespace('api/tsd_eemd', description='演示时间序列数据的 enhanced empirical decomposition')

tsd_eemd_model = name_space_tsd_eemd.model("tsd_eemd_post_model",
            {
             'index': fields.String(required=True, description="索引名称"),
             'ts_code': fields.String(required=True, description="基金代码"),
             'field_name': fields.String(required=True, description="字段名称"),
             'date_name': fields.String(required=True, description ="日期名称"),
             'start_date': fields.String(required=False, description="开始日期"),
             's_num': fields.String(required=False, description="结束日期"),
             'max_sifting': fields.String(required=False, description="max number of siftings of EEMD"),
             'noise_strength': fields.String(required=False, description="noise strength amplitude"),
             'size_k': fields.String(required=False, description="the size of the ensemble")
            })

tsd_eemd_model_2_es = name_space_tsd_eemd.model("tsd_eemd_post_model",
            {
             'index': fields.String(required=True, description="索引名称"),
             'ts_code': fields.String(required=True, description="基金代码"),
             'field_name': fields.String(required=True, description="字段名称"),
             'date_name': fields.String(required=True, description ="日期名称"),
             'start_date': fields.String(required=False, description="开始日期"),
             's_num': fields.String(required=False, description="结束日期"),
             'max_sifting': fields.String(required=False, description="max number of siftings of EEMD"),
             'noise_strength': fields.String(required=False, description="noise strength amplitude"),
             'size_k': fields.String(required=False, description="the size of the ensemble"),
             'target_index': fields.String(required=True, description="目标索引名称")
            })


@name_space_tsd_eemd.route("/plot_eemd")
class PlotEEMD(Resource):
    @name_space_tsd_eemd.doc(body=tsd_eemd_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        s_num, max_sifting, noise_strength, size_k = get_parameter(request)
        title = ts_code + ' eemd imf plot'
        imf = eemd_analysis(df[field_name], s_num, max_sifting, noise_strength, size_k)
        imf_size= imf.shape[0]
        for i in range(imf_size):
            df['imf' + str(i)] = imf[i]
        text=None
        return Response(plot_eemd(df, imf_size, title, date_name, text), mimetype='image/png')


@name_space_tsd_eemd.route("/plot_ceemd")
class PlotCEEMD(Resource):
    @name_space_tsd_eemd.doc(body=tsd_eemd_model)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        s_num, max_sifting, noise_strength, size_k = get_parameter(request)
        title = ts_code + ' ceemd imf plot'
        imf = ceemd_analysis(df[field_name], s_num, max_sifting, noise_strength, size_k)
        imf_size= imf.shape[0]
        for i in range(imf_size):
            df['imf' + str(i)] = imf[i]
        text=None
        return Response(plot_eemd(df, imf_size, title, date_name, text), mimetype='image/png')


@name_space_tsd_eemd.route("/ceemd_2_es")
class ESCEEMD(Resource):
    @name_space_tsd_eemd.doc(body=tsd_eemd_model_2_es)
    def post(self):
        df, ts_code, field_name, date_name, start_date, end_date = get_data(request)
        s_num, max_sifting, noise_strength, size_k = get_parameter(request)
        target_index = get_dest_index(request)
        imf = ceemd_analysis(df[field_name], s_num, max_sifting, noise_strength, size_k)
        imf_size= imf.shape[0]
        for i in range(imf_size):
            df['imf' + str(i)] = imf[i]

        response = indexing_df(target_index, df)
        if response is not None:
            return jsonify(response)
        else:
            return {}


def get_data(req):
    request_data = req.get_json()
    index = request_data['index']
    ts_code = request_data['ts_code']
    field_name = request_data['field_name']
    date_name = request_data['date_name']
    start_date = request_data['start_date'] if 'start_date' in request_data else None
    end_date = request_data['end_date'] if 'end_date' in request_data else None
    df = es_get_data(index, ts_code, field_name, date_name, start_date, end_date)
    return df, ts_code, field_name, date_name, start_date, end_date


def get_parameter(req):
    request_data = req.get_json()
    s_num = request_data['s_num'] if 's_num' in request_data else None
    max_sifting = request_data['max_sifting'] if 'max_sifting' in request_data else None
    noise_strength = request_data['noise_strength'] if 'noise_strength' in request_data else None
    size_k = request_data['size_k'] if 'size_k' in request_data else None
    return s_num, max_sifting, noise_strength, size_k


def get_dest_index(req):
    request_data = req.get_json()
    return request_data['target_index']

