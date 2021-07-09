from com.beiwei.client.config.hlclient import HLClient
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Q, Bool, Range, Term
from elasticsearch_dsl.aggs import A, DateHistogram, ScriptedMetric, Avg, Min, MovingFn, BucketScript,  \
                                    BucketSelector, Derivative
import pandas as pd
from datetime import datetime

hl_es = HLClient.get_instance()


def macd(index, ts_code, start_date, end_date):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(end_date={'gte': start_date, 'lte': end_date}), Term(ts_code=ts_code)]))
    aggs = A(DateHistogram(field='end_date', fixed_interval='1d', format='yyyyMMdd'))
    aggs_price = A(Avg(field='adj_nav'))
    aggs_datestr = A(Min(field='end_date'))
    aggs_ema12 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(12+1))', window=12, buckets_path='Daily'))
    aggs_ema26 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(26+1))', window=26, buckets_path='Daily'))
    aggs_macd = A(BucketScript(buckets_path={'EMA12': 'EMA12', 'EMA26': 'EMA26'},
            script='params.EMA12 - params.EMA26'))
    aggs_signal = A(MovingFn(script='MovingFunctions.ewma(values,2/(9+1))', window=9, buckets_path='macd'))
    aggs_histo = A(BucketScript(buckets_path={'macd': "macd", "signal": "signal"},
                                     script='params.macd - params.signal'))
    aggs_histo_diff = A(Derivative(buckets_path='Histo'))
    aggs_macd_type = A(BucketScript(buckets_path={'Histo': 'Histo', 'HistoDiff': 'HistoDiff'},
                                       script='(params.Histo > 0) ? (params.HistoDiff > 0 ? 3:4) : '
                                              '(params.Histo< 0) ? (params.HistoDiff > 0 ? 2:1):0'))
    start_epoch_milli = int(datetime.strptime(start_date, '%Y%m%d').timestamp()*1000)
    aggs_smacd = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('MACD', aggs).metric('Daily', aggs_price).\
        metric('DateStr', aggs_datestr).pipeline('EMA12', aggs_ema12).pipeline('EMA26', aggs_ema26).\
        pipeline('macd', aggs_macd).pipeline('signal', aggs_signal).\
        pipeline('Histo', aggs_histo).pipeline('HistoDiff', aggs_histo_diff).\
        pipeline('MACDType', aggs_macd_type).pipeline('SMACD', aggs_smacd)

    response = search.execute()
    return response


def iex_macd(index, symbol, start_date, end_date):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(date={'gte': start_date, 'lte': end_date}), Term(symbol=symbol)]))
    aggs = A(DateHistogram(field='date', fixed_interval='1d', format='yyyy-MM-dd'))
    aggs_tp = A(ScriptedMetric(init_script='state.totals=[]',
                map_script='state.totals.add((doc.high.value+doc.low.value+doc.close.value)/3)',
                combine_script='def total=0.0; for (t in state.totals) {total += t} return total',
                reduce_script='return states[0]'))
    aggs_datestr = A(Min(field='date'))
    aggs_stp = A(BucketSelector(buckets_path={'count': '_count'}, script='params.count > 0'))
    aggs_ema12 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(12+1))', window=12,
                            buckets_path='Daily.value'))
    aggs_ema26 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(26+1))', window=26,
                            buckets_path='Daily.value'))
    aggs_macd = A(BucketScript(buckets_path={'EMA12': 'EMA12', 'EMA26': 'EMA26'},
            script='params.EMA12 - params.EMA26'))
    aggs_signal = A(MovingFn(script='MovingFunctions.ewma(values,2/(9+1))', window=9, buckets_path='macd'))
    aggs_histo = A(BucketScript(buckets_path={'macd': "macd", "signal": "signal"},
                                     script='params.macd - params.signal'))
    aggs_histo_diff = A(Derivative(buckets_path='Histo'))
    aggs_macd_type = A(BucketScript(buckets_path={'Histo': 'Histo', 'HistoDiff': 'HistoDiff'},
                                       script='(params.Histo > 0) ? (params.HistoDiff > 0 ? 3:4) : '
                                              '(params.Histo< 0) ? (params.HistoDiff > 0 ? 2:1):0'))

    start_epoch_milli = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()*1000)
    aggs_smacd = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('MACD', aggs).metric('Daily', aggs_tp).pipeline('STP', aggs_stp).\
        metric('DateStr', aggs_datestr).pipeline('EMA12', aggs_ema12).pipeline('EMA26', aggs_ema26).\
        pipeline('macd', aggs_macd).pipeline('signal', aggs_signal).\
        pipeline('Histo', aggs_histo).pipeline('HistoDiff', aggs_histo_diff).\
        pipeline('MACDType', aggs_macd_type).pipeline('SMACD', aggs_smacd)

    response = search.execute()
    return response


def macd_bb(index, ts_code, start_date, end_date):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(end_date={'gte': start_date, 'lte': end_date}), Term(ts_code=ts_code)]))
    aggs = A(DateHistogram(field='end_date', fixed_interval='1d', format='yyyyMMdd'))
    aggs_price = A(Avg(field='adj_nav'))
    aggs_datestr = A(Min(field='end_date'))
    aggs_ema12 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(12+1))', window=12,
                            buckets_path='Daily.value'))
    aggs_ema26 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(26+1))', window=26,
                            buckets_path='Daily.value'))
    aggs_macd = A(BucketScript(buckets_path={'EMA12': 'EMA12', 'EMA26': 'EMA26'},
                               script='params.EMA12 - params.EMA26'))
    aggs_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=10, buckets_path='MACD'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                           window=10, buckets_path='MACD'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - params.SD'))
    aggs_macd_diff = A(Derivative(buckets_path='MACD'))
    aggs_macd_type = A(BucketScript(buckets_path={'MACD': 'MACD', 'MACD_Diff': 'MACD_Diff',
                        'BBU': 'BBU', 'BBL': 'BBL'},
                        script='(params.MACD > params.BBU) ? (params.MACD_Diff > 0 ? 3:4) : '
                               '(params.MACD < params.BBL) ? (params.MACD_Diff > 0 ? 2:1):0'))

    start_epoch_milli = int(datetime.strptime(start_date, '%Y%m%d').timestamp()*1000)
    aggs_macd_bb = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('MACD_BB', aggs).metric('Daily', aggs_price).\
        metric('DateStr', aggs_datestr).pipeline('EMA12', aggs_ema12).pipeline('EMA26', aggs_ema26). \
        pipeline('MACD', aggs_macd).pipeline('SMA', aggs_sma).pipeline('SD', aggs_sd).\
        pipeline('BBU', aggs_bbu).pipeline('BBL', aggs_bbl).\
        pipeline('MACD_Diff', aggs_macd_diff).pipeline('MACDType', aggs_macd_type).pipeline('SMACD_BB', aggs_macd_bb)

    response = search.execute()
    return response


def iex_macd_bb(index, symbol, start_date, end_date):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(date={'gte': start_date, 'lte': end_date}), Term(symbol=symbol)]))
    aggs = A(DateHistogram(field='date', fixed_interval='1d', format='yyyy-MM-dd'))
    aggs_tp = A(ScriptedMetric(init_script='state.totals=[]',
                map_script='state.totals.add((doc.high.value+doc.low.value+doc.close.value)/3)',
                combine_script='def total=0.0; for (t in state.totals) {total += t} return total',
                reduce_script='return states[0]'))
    aggs_datestr = A(Min(field='date'))
    aggs_stp = A(BucketSelector(buckets_path={'count': '_count'}, script='params.count > 0'))
    aggs_ema12 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(12+1))', window=12,
                            buckets_path='Daily.value'))
    aggs_ema26 = A(MovingFn(script='MovingFunctions.ewma(values, 2/(26+1))', window=26,
                            buckets_path='Daily.value'))
    aggs_macd = A(BucketScript(buckets_path={'EMA12': 'EMA12', 'EMA26': 'EMA26'},
                               script='params.EMA12 - params.EMA26'))
    aggs_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=10, buckets_path='MACD'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                           window=10, buckets_path='MACD'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - params.SD'))
    aggs_macd_diff = A(Derivative(buckets_path='MACD'))
    aggs_macd_type = A(BucketScript(buckets_path={'MACD': 'MACD', 'MACD_Diff': 'MACD_Diff',
                        'BBU': 'BBU', 'BBL': 'BBL'},
                        script='(params.MACD > params.BBU) ? (params.MACD_Diff > 0 ? 3:4) : '
                               '(params.MACD < params.BBL) ? (params.MACD_Diff > 0 ? 2:1):0'))
    start_epoch_milli = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp()*1000)
    aggs_macd_bb = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))

    search.aggs.bucket('MACD_BB', aggs).metric('Daily', aggs_tp).pipeline('STP', aggs_stp).\
        metric('DateStr', aggs_datestr).pipeline('EMA12', aggs_ema12).pipeline('EMA26', aggs_ema26). \
        pipeline('MACD', aggs_macd).pipeline('SMA', aggs_sma).pipeline('SD', aggs_sd).\
        pipeline('BBU', aggs_bbu).pipeline('BBL', aggs_bbl).\
        pipeline('MACD_Diff', aggs_macd_diff).pipeline('MACDType', aggs_macd_type).pipeline('SMACD_BB', aggs_macd_bb)

    response = search.execute()
    return response


def convert_macd_response_to_df(response, date_name, start_date):

    df = pd.DataFrame()
    if len(response['aggregations']['MACD']['buckets']) > 0:
        buckets = response['aggregations']['MACD']['buckets']
        for bucket in buckets:
            if 'macd' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list[date_name] = bucket['key_as_string']
                    row_list['macd'] = bucket['macd'].value
                    row_list['signal'] = bucket['signal'].value
                    row_list['HistoDiff'] = bucket['HistoDiff'].value
                    row_list['Histo'] = bucket['Histo'].value
                    row_list['MACDType'] = bucket['MACDType'].value
                    row_list['Daily'] = bucket['Daily'].value
                    row_list['EMA12'] = bucket['EMA12'].value
                    row_list['EMA26'] = bucket['EMA26'].value
                    df = df.append(row_list, ignore_index=True)
    return df


def convert_macd_bb_response_to_df(response, date_name, start_date):

    df = pd.DataFrame()
    if len(response['aggregations']['MACD_BB']['buckets']) > 0:
        buckets = response['aggregations']['MACD_BB']['buckets']
        for bucket in buckets:
            if 'MACD' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list[date_name] = bucket['key_as_string']
                    row_list['MACD'] = bucket['MACD'].value
                    row_list['BBL'] = bucket['BBL'].value
                    row_list['BBU'] = bucket['BBU'].value
                    row_list['SMA'] = bucket['SMA'].value
                    row_list['Daily'] = bucket['Daily'].value
                    row_list['MACDType'] = bucket['MACDType'].value
                    df = df.append(row_list, ignore_index=True)
    return df

