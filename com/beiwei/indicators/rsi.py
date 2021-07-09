from com.beiwei.client.config.hlclient import HLClient
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Q, Bool, Range, Term
from elasticsearch_dsl.aggs import A, DateHistogram, ScriptedMetric, Avg, Min, MovingFn, BucketScript,  \
                                    BucketSelector, Derivative
import pandas as pd
from datetime import datetime

hl_es = HLClient.get_instance()

def rsi_bb(index, ts_code, start_date, end_date, rsi_period, sma_period):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(end_date={'gte': start_date, 'lte': end_date}), Term(ts_code=ts_code)]))
    aggs = A(DateHistogram(field='end_date', fixed_interval='1d', format='yyyyMMdd'))
    aggs_price = A(Avg(field='adj_nav'))
    aggs_datestr = A(Min(field='end_date'))
    aggs_stp = A(BucketSelector(buckets_path={'count': '_count'}, script='params.count > 0'))
    aggs_price_diff = A(Derivative(buckets_path='Daily'))
    aggs_gain = A(BucketScript(buckets_path={'Diff': 'Diff'}, script='(params.Diff > 0) ? params.Diff : 0'))
    aggs_loss = A(BucketScript(buckets_path={'Diff': 'Diff'}, script='(params.Diff < 0) ? -params.Diff : 0'))
    aggs_gain_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=rsi_period, buckets_path='Gain', shift=1))
    aggs_loss_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=rsi_period, buckets_path='Loss', shift=1))
    #aggs_gain_ewma  = A(MovingFn(script='MovingFunctions.ewma(values, 2/(14+1))', window=rsi_period-1, buckets_path='Gain'))
    #aggs_loss_ewma  = A(MovingFn(script='MovingFunctions.ewma(values, 2/(14+1))', window=rsi_period-1, buckets_path='Loss'))
    aggs_rsi = A(BucketScript(buckets_path={'GainMA': 'GainMA', 'LossMA': 'LossMA'},
                              script='100 - 100/(1+params.GainMA/params.LossMA)'))
    aggs_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=sma_period, buckets_path='Daily'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                           window=sma_period, buckets_path='Daily'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + 2*params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - 2*params.SD'))
    aggs_rsi_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=sma_period, buckets_path='RSI'))
    aggs_rsi_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                           window=sma_period, buckets_path='RSI'))
    aggs_rsi_bbu = A(BucketScript(buckets_path={'SMA': 'RSI_SMA', 'SD': 'RSI_SD'}, script='params.SMA + 2* params.SD'))
    aggs_rsi_bbl = A(BucketScript(buckets_path={'SMA': 'RSI_SMA', 'SD': 'RSI_SD'}, script='params.SMA - 2* params.SD'))
    aggs_rsi_diff = A(Derivative(buckets_path='RSI'))
    aggs_rsi_type = A(BucketScript(buckets_path={'RSI': 'RSI', 'RSI_Diff': 'RSI_Diff'},
                                   script='(params.RSI >= 70) ? (params.RSI_Diff > 0 ? 3:4) : '
                                          '(params.RSI <= 30) ? (params.RSI_Diff > 0 ? 2:1):0'))

    start_epoch_milli = int(datetime.strptime(start_date, '%Y%m%d').timestamp()*1000)
    aggs_rsi_bb = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('RSI_BB', aggs).metric('Daily', aggs_price).\
        metric('DateStr', aggs_datestr).pipeline('Diff', aggs_price_diff).\
        pipeline('Gain', aggs_gain).pipeline('Loss', aggs_loss).\
        pipeline('GainMA', aggs_gain_sma).pipeline('LossMA', aggs_loss_sma).pipeline('RSI', aggs_rsi). \
        pipeline('SMA', aggs_sma).pipeline('SD', aggs_sd). \
        pipeline('BBU', aggs_bbu).pipeline('BBL', aggs_bbl). \
        pipeline('RSI_SMA', aggs_rsi_sma).pipeline('RSI_SD', aggs_rsi_sd).\
        pipeline('RSI_BBU', aggs_rsi_bbu).pipeline('RSI_BBL', aggs_rsi_bbl).\
        pipeline('RSI_Diff', aggs_rsi_diff).pipeline('RSIType', aggs_rsi_type).pipeline('SRSI_BB', aggs_rsi_bb)

    response = search.execute()
    return response

def iex_rsi_bb(index, symbol, start_date, end_date, rsi_period, sma_period):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(date={'gte': start_date, 'lte': end_date}), Term(symbol=symbol)]))
    aggs = A(DateHistogram(field='date', fixed_interval='1d', format='yyyy-MM-dd'))
    aggs_price = A(Avg(field='close'))
    aggs_datestr = A(Min(field='date'))
    aggs_stp = A(BucketSelector(buckets_path={'count': '_count'}, script='params.count > 0'))
    aggs_price_diff = A(Derivative(buckets_path='Daily'))
    aggs_gain = A(BucketScript(buckets_path={'Diff': 'Diff'}, script='(params.Diff > 0) ? params.Diff : 0'))
    aggs_loss = A(BucketScript(buckets_path={'Diff': 'Diff'}, script='(params.Diff < 0) ? -params.Diff : 0'))
    aggs_gain_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=rsi_period, buckets_path='Gain', shift=1))
    aggs_loss_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=rsi_period, buckets_path='Loss', shift=1))
    #aggs_gain_ewma  = A(MovingFn(script='MovingFunctions.ewma(values, 2/(14+1))', window=rsi_period-1, buckets_path='Gain'))
    #aggs_loss_ewma  = A(MovingFn(script='MovingFunctions.ewma(values, 2/(14+1))', window=rsi_period-1, buckets_path='Loss'))
    aggs_rsi = A(BucketScript(buckets_path={'GainMA': 'GainMA', 'LossMA': 'LossMA'},
                              script='100 - 100/(1+params.GainMA/params.LossMA)'))
    aggs_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=sma_period, buckets_path='Daily'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                           window=sma_period, buckets_path='Daily'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + 2*params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - 2*params.SD'))
    aggs_rsi_sma = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=sma_period, buckets_path='RSI'))
    aggs_rsi_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))',
                           window=sma_period, buckets_path='RSI'))
    aggs_rsi_bbu = A(BucketScript(buckets_path={'SMA': 'RSI_SMA', 'SD': 'RSI_SD'}, script='params.SMA + 2* params.SD'))
    aggs_rsi_bbl = A(BucketScript(buckets_path={'SMA': 'RSI_SMA', 'SD': 'RSI_SD'}, script='params.SMA - 2* params.SD'))
    aggs_rsi_diff = A(Derivative(buckets_path='RSI'))
    aggs_rsi_type = A(BucketScript(buckets_path={'RSI': 'RSI', 'RSI_Diff': 'RSI_Diff'},
                                   script='(params.RSI >= 70) ? (params.RSI_Diff > 0 ? 3:4) : '
                                          '(params.RSI <= 30) ? (params.RSI_Diff > 0 ? 2:1):0'))

    start_epoch_milli = int(datetime.strptime(start_date, '%Y%m%d').timestamp()*1000)
    aggs_rsi_bb = A(BucketSelector(buckets_path={'DateStr': 'DateStr'},
                                  script='params.DateStr >= {}L'.format(start_epoch_milli)))
    search.aggs.bucket('RSI_BB', aggs).metric('Daily', aggs_price).\
        metric('DateStr', aggs_datestr).pipeline('STP', aggs_stp).pipeline('Diff', aggs_price_diff).\
        pipeline('Gain', aggs_gain).pipeline('Loss', aggs_loss).\
        pipeline('GainMA', aggs_gain_sma).pipeline('LossMA', aggs_loss_sma).pipeline('RSI', aggs_rsi). \
        pipeline('SMA', aggs_sma).pipeline('SD', aggs_sd). \
        pipeline('BBU', aggs_bbu).pipeline('BBL', aggs_bbl). \
        pipeline('RSI_SMA', aggs_rsi_sma).pipeline('RSI_SD', aggs_rsi_sd).\
        pipeline('RSI_BBU', aggs_rsi_bbu).pipeline('RSI_BBL', aggs_rsi_bbl).\
        pipeline('RSI_Diff', aggs_rsi_diff).pipeline('RSIType', aggs_rsi_type).pipeline('SRSI_BB', aggs_rsi_bb)

    response = search.execute()
    return response


def convert_rsi_bb_response_to_df(response, date_name, start_date):
    df = pd.DataFrame()
    if len(response['aggregations']['RSI_BB']['buckets']) > 0:
        buckets = response['aggregations']['RSI_BB']['buckets']
        for bucket in buckets:
            if 'RSI' in bucket:
                if bucket['key_as_string'] >= start_date:
                    row_list = dict()
                    row_list[date_name] = bucket['key_as_string']
                    row_list['BBL'] = bucket['BBL'].value
                    row_list['BBU'] = bucket['BBU'].value
                    row_list['SMA'] = bucket['SMA'].value
                    row_list['SD'] = bucket['SD'].value
                    row_list['RSI'] = bucket['RSI'].value
                    row_list['RSI_BBL'] = bucket['RSI_BBL'].value
                    row_list['RSI_BBU'] = bucket['RSI_BBU'].value
                    row_list['RSI_SMA'] = bucket['RSI_SMA'].value
                    row_list['RSI_SD'] = bucket['RSI_SD'].value
                    row_list['Daily'] = bucket['Daily'].value
                    row_list['RSIType'] = bucket['RSIType'].value
                    df = df.append(row_list, ignore_index=True)
    return df
