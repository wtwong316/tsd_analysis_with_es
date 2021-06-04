from com.beiwei.client.config.hlclient import HLClient
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Q, Bool, Range, Term
from elasticsearch_dsl.aggs import A, DateHistogram, Avg, MovingFn, BucketScript, ExtendedStatsBucket
import pandas as pd
import json

hl_es = HLClient.get_instance()


def bollinger_band(index, ts_code, field_name, date_name, start_date, end_date, window):
    search = Search(index=index, using=hl_es)[0:0]
    search.query = Q(Bool(must=[Range(end_date={'gte': start_date, 'lte': end_date}), Term(ts_code=ts_code)]))
    aggs = A(DateHistogram(field=date_name, fixed_interval='1d', format='yyyyMMdd'))
    aggs_price = A(Avg(field=field_name))
    aggs_mavg = A(MovingFn(script='MovingFunctions.unweightedAvg(values)', window=window, buckets_path='daily'))
    aggs_sd = A(MovingFn(script='MovingFunctions.stdDev(values, MovingFunctions.unweightedAvg(values))', window=window,
                         buckets_path='daily'))
    aggs_bbu = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA + 2*params.SD'))
    aggs_bbl = A(BucketScript(buckets_path={'SMA': 'SMA', 'SD': 'SD'}, script='params.SMA - 2*params.SD'))
    aggs_bbw = A(BucketScript(buckets_path={'SMA': 'SMA', 'BBU': 'BBU', 'BBL': 'BBL'},
                              script='(params.BBU - params.BBL)/params.SMA'))
    aggs_pb = A(BucketScript(buckets_path={'daily':'daily', 'BBU': 'BBU', 'BBL': 'BBL'},
                             script='(params.BBU-params.daily)/(params.BBU-params.BBL)'))
    aggs_estats = A(ExtendedStatsBucket(buckets_path='BBI>BBW'))
    search.aggs.bucket('BBI', aggs).metric('daily', aggs_price).pipeline(
        'SMA', aggs_mavg).pipeline('SD', aggs_sd).pipeline('BBU', aggs_bbu).\
        pipeline('BBL', aggs_bbl).pipeline('BBW', aggs_bbw).pipeline('PercentB', aggs_pb)
    search.aggs.bucket('EStats', aggs_estats)
    response = search.execute()
    return response


def convert_response_to_df(response, date_name):
    row_list = {}

    df = pd.DataFrame()
    if len(response['aggregations']['BBI']['buckets']) > 0:
        buckets = response['aggregations']['BBI']['buckets']
        for bucket in buckets:
            if 'BBL' in bucket:
                row_list[date_name] = bucket['key_as_string']
                row_list['BBL'] = bucket['BBL'].value
                row_list['BBU'] = bucket['BBU'].value
                row_list['BBW'] = bucket['BBW'].value
                row_list['SMA'] = bucket['SMA'].value
                row_list['PercentB'] = bucket['PercentB'].value
                row_list['daily'] = bucket['daily'].value
                df = df.append(row_list, ignore_index=True)
    if 'EStats' in response['aggregations']:
        text = json.dumps(response['aggregations']['EStats'].to_dict(), sort_keys=True, indent=4)

    return df, text


if __name__ == "__main__":
    bbi = bollinger_band('fund_nav_o', '000793.OF', 'adj_nav', 'end_date', '20141022', '20210517', '50')
    print(bbi)

