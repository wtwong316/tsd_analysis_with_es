from com.beiwei.client.config.hlclient import HLClient
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Q, Bool, Range, Term
from elasticsearch_dsl.aggs import A, DateHistogram, ScriptedMetric, Avg, Min, MovingFn, BucketScript,  \
                                    BucketSelector, AvgBucket, Derivative
import pandas as pd
from datetime import datetime
import json

hl_es = HLClient.get_instance()


def bollinger_band(index, ts_code, start_date, end_date, window):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(end_date={'gte': start_date, 'lte': end_date}), Term(ts_code=ts_code)]))
    aggs = A(DateHistogram(field='end_date', fixed_interval='1d', format='yyyyMMdd'))
    aggs_price = A(Avg(field='adj_nav'))
    aggs_datestr = A(Min(field='end_date'))
    aggs_mavg = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=window, buckets_path='Daily'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))', window=window,
                         buckets_path='Daily'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + 2*params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - 2*params.SD'))
    aggs_bbw = A(BucketScript(buckets_path={'SMA': 'SMA', 'BBU': 'BBU', 'BBL': 'BBL'},
                              script='(params.BBU - params.BBL)/params.SMA'))
    aggs_pb = A(BucketScript(buckets_path={'Daily':'Daily', 'BBU': 'BBU', 'BBL': 'BBL'},
                             script='(params.BBU-params.Daily)/(params.BBU-params.BBL)'))
    start_epoch_milli = int(datetime.strptime(start_date, '%Y%m%d').timestamp()*1000)
    aggs_sbbw = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    aggs_avg = A(AvgBucket(buckets_path='BBI>BBW'))
    search.aggs.bucket('BBI', aggs).metric('Daily', aggs_price).metric('DateStr', aggs_datestr).\
        pipeline('SMA', aggs_mavg).pipeline('SD', aggs_sd).pipeline('BBU', aggs_bbu).\
        pipeline('BBL', aggs_bbl).pipeline('BBW', aggs_bbw).pipeline('PercentB', aggs_pb).pipeline('SBBW', aggs_sbbw)
    search.aggs.bucket('avg_BBW', aggs_avg)
    response = search.execute()
    return response


def iex_bollinger_band(index, symbol, start_date, end_date, window):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(date={'gte': start_date, 'lte': end_date}), Term(symbol=symbol)]))
    aggs = A(DateHistogram(field='date', fixed_interval='1d', format='yyyy-MM-dd'))
    aggs_tp = A(ScriptedMetric(init_script='state.totals=[]',
                map_script='state.totals.add((doc.high.value+doc.low.value+doc.close.value)/3)',
                combine_script='def total=0.0; for (t in state.totals) {total += t} return total',
                reduce_script='return states[0]'))
    aggs_datestr = A(Min(field='date'))
    aggs_stp = A(BucketSelector(buckets_path={'count': '_count'}, script='params.count > 0'))
    aggs_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=window, buckets_path='Daily.value'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                         window=window, buckets_path='Daily.value'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + 2*params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - 2*params.SD'))
    aggs_bbw = A(BucketScript(buckets_path={'SMA': 'SMA', 'BBU': 'BBU', 'BBL': 'BBL'},
                              script='(params.BBU - params.BBL)/params.SMA'))
    aggs_pb = A(BucketScript(buckets_path={'Daily': 'Daily.value', 'BBU': 'BBU', 'BBL': 'BBL'},
                             script='(params.BBU-params.Daily)/(params.BBU-params.BBL)'))

    start_epoch_milli = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()*1000)
    aggs_sbbw = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    aggs_avg_bbw = A(AvgBucket(buckets_path='BBI>BBW'))
    search.aggs.bucket('BBI', aggs).metric('Daily', aggs_tp).pipeline('STP', aggs_stp).\
        metric('DateStr', aggs_datestr).pipeline('SMA', aggs_sma).pipeline('SD', aggs_sd).\
        pipeline('BBU', aggs_bbu).pipeline('BBL', aggs_bbl).pipeline('BBW', aggs_bbw).pipeline('SBBW', aggs_sbbw).\
        pipeline('PercentB', aggs_pb)
    search.aggs.bucket('avg_BBW', aggs_avg_bbw)
    response = search.execute()
    return response


def bollinger_band_trend(index, ts_code, start_date, end_date, window):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(end_date={'gte': start_date, 'lte': end_date}), Term(ts_code=ts_code)]))
    aggs = A(DateHistogram(field='end_date', fixed_interval='1d', format='yyyyMMdd'))
    aggs_price = A(Avg(field='adj_nav'))
    aggs_datestr = A(Min(field='end_date'))
    aggs_sma20 = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=window, buckets_path='Daily'))
    aggs_sd20 = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                         window=window, buckets_path='Daily'))
    aggs_bbu20 = A(BucketScript(buckets_path={'SMA': 'SMA20', 'SD': 'SD20'}, script='params.SMA + 2*params.SD'))
    aggs_bbl20 = A(BucketScript(buckets_path={'SMA': 'SMA20', 'SD': 'SD20'}, script='params.SMA - 2*params.SD'))

    aggs_sma50 = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=50, buckets_path='Daily'))
    aggs_sd50 = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                         window=50, buckets_path='Daily'))
    aggs_bbu50 = A(BucketScript(buckets_path={'SMA': 'SMA50', 'SD': 'SD50'}, script='params.SMA + 2*params.SD'))
    aggs_bbl50 = A(BucketScript(buckets_path={'SMA': 'SMA50', 'SD': 'SD50'}, script='params.SMA - 2*params.SD'))
    aggs_bbtrend = A(BucketScript(buckets_path={'BBL20': 'BBL20', 'BBU20': 'BBU20', 'SMA20': 'SMA20',
                                                'BBL50': 'BBL50', 'BBU50': 'BBU50'},
            script='(Math.abs(params.BBL20 - params.BBL50) - Math.abs(params.BBU20 - params.BBU50))/params.SMA20'))
    aggs_bbtrend_diff = A(Derivative(buckets_path='BBTrend'))
    aggs_bbtrend_type = A(BucketScript(buckets_path={'BBTrend': 'BBTrend', 'BBTrendDiff': 'BBTrendDiff'},
                                       script='(params.BBTrend > 0) ? (params.BBTrendDiff > 0 ? 3:4) : '
                                              '(params.BBTrend < 0) ? (params.BBTrendDiff > 0 ? 2:1):0'))
    start_epoch_milli = int(datetime.strptime(start_date, '%Y%m%d').timestamp()*1000)
    aggs_sbbtrend = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('BBI', aggs).metric('Daily', aggs_price).\
        metric('DateStr', aggs_datestr).pipeline('SMA20', aggs_sma20).pipeline('SD20', aggs_sd20).\
        pipeline('BBU20', aggs_bbu20).pipeline('BBL20', aggs_bbl20).pipeline('SMA50', aggs_sma50).\
        pipeline('SD50', aggs_sd50).pipeline('BBU50', aggs_bbu50).pipeline('BBL50', aggs_bbl50).\
        pipeline('BBTrend', aggs_bbtrend).pipeline('BBTrendDiff', aggs_bbtrend_diff).\
        pipeline('BBTrendType', aggs_bbtrend_type).pipeline('SBBTrend', aggs_sbbtrend)

    response = search.execute()
    return response


def iex_bollinger_band_trend(index, symbol, start_date, end_date, window):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(date={'gte': start_date, 'lte': end_date}), Term(symbol=symbol)]))
    aggs = A(DateHistogram(field='date', fixed_interval='1d', format='yyyy-MM-dd'))
    aggs_tp = A(ScriptedMetric(init_script='state.totals=[]',
                map_script='state.totals.add((doc.high.value+doc.low.value+doc.close.value)/3)',
                combine_script='def total=0.0; for (t in state.totals) {total += t} return total',
                reduce_script='return states[0]'))
    aggs_datestr = A(Min(field='date'))
    aggs_stp = A(BucketSelector(buckets_path={'count': '_count'}, script='params.count > 0'))
    aggs_sma20 = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=window, buckets_path='Daily.value'))
    aggs_sd20 = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                         window=window, buckets_path='Daily.value'))
    aggs_bbu20 = A(BucketScript(buckets_path={'SMA': 'SMA20', 'SD': 'SD20'}, script='params.SMA + 2*params.SD'))
    aggs_bbl20 = A(BucketScript(buckets_path={'SMA': 'SMA20', 'SD': 'SD20'}, script='params.SMA - 2*params.SD'))

    aggs_sma50 = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=50, buckets_path='Daily.value'))
    aggs_sd50 = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                         window=50, buckets_path='Daily.value'))
    aggs_bbu50 = A(BucketScript(buckets_path={'SMA': 'SMA50', 'SD': 'SD50'}, script='params.SMA + 2*params.SD'))
    aggs_bbl50 = A(BucketScript(buckets_path={'SMA': 'SMA50', 'SD': 'SD50'}, script='params.SMA - 2*params.SD'))
    aggs_bbtrend = A(BucketScript(buckets_path={'BBL20': 'BBL20', 'BBU20': 'BBU20', 'SMA20': 'SMA20',
                                                'BBL50': 'BBL50', 'BBU50': 'BBU50'},
            script='(Math.abs(params.BBL20 - params.BBL50) - Math.abs(params.BBU20 - params.BBU50))/params.SMA20'))
    aggs_bbtrend_diff = A(Derivative(buckets_path='BBTrend'))
    aggs_bbtrend_type = A(BucketScript(buckets_path={'BBTrend': 'BBTrend', 'BBTrendDiff': 'BBTrendDiff'},
                                       script='(params.BBTrend > 0) ? (params.BBTrendDiff > 0 ? 3:4) : '
                                              '(params.BBTrend < 0) ? (params.BBTrendDiff > 0 ? 2:1):0'))
    start_epoch_milli = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()*1000)
    aggs_sbbtrend = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('BBI', aggs).metric('Daily', aggs_tp).pipeline('STP', aggs_stp).\
        metric('DateStr', aggs_datestr).pipeline('SMA20', aggs_sma20).pipeline('SD20', aggs_sd20).\
        pipeline('BBU20', aggs_bbu20).pipeline('BBL20', aggs_bbl20).pipeline('SMA50', aggs_sma50).\
        pipeline('SD50', aggs_sd50).pipeline('BBU50', aggs_bbu50).pipeline('BBL50', aggs_bbl50).\
        pipeline('BBTrend', aggs_bbtrend).pipeline('BBTrendDiff', aggs_bbtrend_diff).\
        pipeline('BBTrendType', aggs_bbtrend_type).pipeline('SBBTrend', aggs_sbbtrend)

    response = search.execute()
    return response


def convert_response_to_df(response, start_date):

    df = pd.DataFrame()
    if len(response['aggregations']['BBI']['buckets']) > 0:
        buckets = response['aggregations']['BBI']['buckets']
        for bucket in buckets:
            if 'BBL' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list['end_date'] = bucket['key_as_string']
                    row_list['BBL'] = bucket['BBL'].value
                    row_list['BBU'] = bucket['BBU'].value
                    row_list['BBW'] = bucket['BBW'].value
                    row_list['SMA'] = bucket['SMA'].value
                    row_list['PercentB'] = bucket['PercentB'].value
                    row_list['Daily'] = bucket['Daily'].value
                    df = df.append(row_list, ignore_index=True)
    if 'avg_BBW' in response['aggregations']:
       avg_BBW  = response['aggregations']['avg_BBW'].value

    return df, avg_BBW


def iex_convert_response_to_df(response,  start_date):
    df = pd.DataFrame()
    if len(response['aggregations']['BBI']['buckets']) > 0:
        buckets = response['aggregations']['BBI']['buckets']
        for bucket in buckets:
            if 'BBL' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list['date'] = bucket['key_as_string']
                    row_list['BBL'] = bucket['BBL'].value
                    row_list['BBU'] = bucket['BBU'].value
                    row_list['BBW'] = bucket['BBW'].value
                    row_list['SMA'] = bucket['SMA'].value
                    row_list['PercentB'] = bucket['PercentB'].value
                    row_list['Daily'] = bucket['Daily'].value
                    df = df.append(row_list, ignore_index=True)
    if 'avg_BBW' in response['aggregations']:
       avg_BBW  = response['aggregations']['avg_BBW'].value

    return df, avg_BBW


def iex_convert_bbtrend_response_to_df(response, date_name, start_date):
    df = pd.DataFrame()
    if len(response['aggregations']['BBI']['buckets']) > 0:
        buckets = response['aggregations']['BBI']['buckets']
        for bucket in buckets:
            if 'BBL20' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list[date_name] = bucket['key_as_string']
                    row_list['BBL20'] = bucket['BBL20'].value
                    row_list['BBU20'] = bucket['BBU20'].value
                    row_list['SMA20'] = bucket['SMA20'].value
                    row_list['BBL50'] = bucket['BBL50'].value
                    row_list['BBU50'] = bucket['BBU50'].value
                    row_list['SMA50'] = bucket['SMA50'].value
                    row_list['Daily'] = bucket['Daily'].value
                    row_list['BBTrend'] = bucket['BBTrend'].value
                    row_list['BBTrendDiff'] = bucket['BBTrendDiff'].value
                    row_list['BBTrendType'] = bucket['BBTrendType'].value
                    df = df.append(row_list, ignore_index=True)
    return df


def convert_bbtrend_response_to_df(response, date_name, start_date):

    df = pd.DataFrame()
    if len(response['aggregations']['BBI']['buckets']) > 0:
        buckets = response['aggregations']['BBI']['buckets']
        for bucket in buckets:
            if 'BBL20' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list[date_name] = bucket['key_as_string']
                    row_list['BBL20'] = bucket['BBL20'].value
                    row_list['BBL50'] = bucket['BBL50'].value
                    row_list['BBU20'] = bucket['BBU20'].value
                    row_list['BBU50'] = bucket['BBU50'].value
                    row_list['SMA20'] = bucket['SMA20'].value
                    row_list['SMA50'] = bucket['SMA50'].value
                    row_list['Daily'] = bucket['Daily'].value
                    row_list['BBTrend'] = bucket['BBTrend'].value
                    row_list['BBTrendDiff'] = bucket['BBTrendDiff'].value
                    row_list['BBTrendType'] = bucket['BBTrendType'].value
                    df = df.append(row_list, ignore_index=True)

    return df


if __name__ == "__main__":
    bbi = bollinger_band('fund_nav_o', '000793.OF', 'adj_nav', 'end_date', '20141022', '20210517', '50')
    print(bbi)

