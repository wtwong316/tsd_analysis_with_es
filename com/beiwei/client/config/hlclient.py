from elasticsearch_dsl import connections
import threading


class HLClient:
    __conn = None
    __conn_lock = threading.Lock()

    @staticmethod
    def get_instance():
        if HLClient.__conn is None:
            with HLClient.__conn_lock:
                if HLClient.__conn is None:
                    HLClient.__conn = \
                        connections.create_connection('hlclient', hosts=['192.168.1.214'], port=19200)
        return HLClient.__conn

    def __init__(self):
        raise Exception("This class is a singleton!, use static method getInstance()")


if __name__ == "__main__":
    test3 = HLClient.get_instance()
    print("test3", hex(id(test3)))

