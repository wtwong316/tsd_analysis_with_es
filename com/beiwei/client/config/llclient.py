from elasticsearch.client import Elasticsearch
import threading


class LLClient:
    __es = None
    __es_lock = threading.Lock()

    @staticmethod
    def get_instance():
        if LLClient.__es is None:
            with LLClient.__es_lock:
                if LLClient.__es is None:
                    LLClient.__es = Elasticsearch(['localhost'], port=9200)
        return LLClient.__es

    def __init__(self):
        raise Exception("This class is a singleton!, use static method getInstance()")


if __name__ == "__main__":
    es = LLClient.get_instance()
    response = es.indices.get_settings(index='fund_basic')
    print("get_settings response", response)


