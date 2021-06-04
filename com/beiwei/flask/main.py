from flask import Flask
from flask_restplus import Api
from com.beiwei.flask.hl_rest_client import name_space_hl
from com.beiwei.flask.tsd_stationarity import name_space_tsd_stationarity
from com.beiwei.flask.tsd_eemd import name_space_tsd_eemd
from com.beiwei.flask.indicators import name_space_indicators
from logging.handlers import RotatingFileHandler
import logging

flask_app = Flask(__name__)
flask_app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
flask_app.config['JSON_AS_ASCII'] = False
# enable Try it Out for specific methods
flask_app.config.SWAGGER_SUPPORTED_SUBMIT_METHODS = ["get", "post"]

api = Api(flask_app)
api.add_namespace(name_space_hl)
api.add_namespace(name_space_tsd_stationarity)
api.add_namespace(name_space_tsd_eemd)
api.add_namespace(name_space_indicators)

flask_app.logger.info('Info level log')
flask_app.logger.warning('Warning level log')
flask_app.logger.error('An error occurred')

def get_api():
    return api


def get_flask_app():
    return flask_app


if __name__ == '__main__':
    formatter = logging.Formatter(
        "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    handler = RotatingFileHandler("debug.log", maxBytes=10000000, backupCount=5)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    flask_app.logger.addHandler(handler)
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)
    flask_app.run(host='192.168.1.214', port='10060')
