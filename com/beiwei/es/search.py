from elasticsearch_dsl import Search, A
from com.beiwei.client.config.hlclient import HLClient
import pandas as pd

hl_es = HLClient.get_instance()


def es_get_data(index, ts_code, field_name, date_name, start_date, end_date):
    search = Search(index=index, using=hl_es)[0:10000]

    if start_date is None and end_date is None:
        search = search.filter('term', **{'ts_code': ts_code}).sort({date_name: {"order": "asc"}})
    elif start_date is None:
        search = search.query("range", **{date_name: {"lte": end_date}}). \
            filter('term', **{'ts_code': ts_code}).sort({date_name: {"order": "asc"}})
    elif end_date is None:
        search = search.query("range", **{date_name: {"gte": start_date}}). \
            filter('term', **{'ts_code': ts_code}).sort({date_name: {"order": "asc"}})
    else:
        search = search.query("range", **{date_name: {"gte": start_date, "lte": end_date}}). \
            filter('term', **{'ts_code': ts_code}).sort({date_name: {"order": "asc"}})
    search = search.source([date_name, field_name])

    response = search.execute()
    df = None
    fields = {}
    if response['hits']['total']['value'] > 0:
        hits = response['hits']['hits']
        for hit in hits:
            if hit['_source'][field_name] is not None:
                fields[hit['_source'][date_name]] = hit['_source'][field_name]
    df = pd.DataFrame(list(fields.items()), columns=[date_name, field_name])
    return df
