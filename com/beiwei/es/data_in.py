import elasticsearch
from com.beiwei.client.config.hlclient import HLClient
import json
import os

hl_es = HLClient.get_instance()

def indexing_df(target_index, df):
    isExist = hl_es.indices.exists(target_index)
    response = None
    if not isExist:
        ROOT_DIR = os.path.abspath(os.curdir)
        file_path = ROOT_DIR + '/../mappings/eemd_mappings.json'
        print(file_path)
        with open(file_path) as f:
            mappings = json.load(f)
        try:
            response = hl_es.indices.create(target_index, body=mappings)
        except elasticsearch.ElasticsearchException as es1:
            print(es1.__cause__)

    try:
        response = hl_es.bulk(rec_to_actions(target_index, df))
    except elasticsearch.ElasticsearchException as es1:
            print(es1.__cause__)

    return response


def rec_to_actions(index, df):
    import json
    for record in df.to_dict(orient="records"):
        yield ('{ "index" : { "_index" : "%s", "_type" : "_doc" }}' % (index))
        yield (json.dumps(record, default=int))

