import connexion
import requests
from connexion import NoContent
import json
import yaml
from itertools import islice
from pykafka import KafkaClient
from pykafka.common import OffsetType
from flask_cors import CORS, cross_origin
import logging
import logging.config

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def get_pickup_event(offset):
    client = KafkaClient(hosts=app_config["kafka"]["server"] + ':' + str(app_config["kafka"]["port"]))
    topic = client.topics[app_config["kafka"]["topic"]]
    consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.EARLIEST, reset_offset_on_start=True, consumer_timeout_ms=100)
    pickup_order_count = 0
    for message in consumer:
        message_str = message.value.decode()
        message_dict = json.loads(message_str)
        if message_dict["type"] == "pickup_order":
            pickup_order_count += 1
            if pickup_order_count >= offset:
                # print(message.offset, message.value)
                logger.debug("Found nth pickup order using offset %s: %s %s" % (offset, message.offset, message.value))
                return message_dict, 200
    logger.warning("Pickup event not found using offset %s" % offset)
    return "No message found", 404


@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def get_delivery_event(offset):
    client = KafkaClient(hosts=app_config["kafka"]["server"] + ':' + str(app_config["kafka"]["port"]))
    topic = client.topics[app_config["kafka"]["topic"]]
    consumer = topic.get_simple_consumer(auto_offset_reset=OffsetType.EARLIEST, reset_offset_on_start=True, consumer_timeout_ms=100)
    for message in islice(consumer, offset - 1, None):
        message_str = message.value.decode()
        message_dict = json.loads(message_str)
        if message_dict["type"] == "delivery_order":
            logger.debug("Found first order following offset %s: %s %s" % (offset, message.offset, message.value))
            return message_dict, 200
    logger.warning("Pickup event not found using offset %s" % offset)
    return "No message found", 404


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8110)
    CORS(app.app, origins="localhost")
    app.app.config["CORS_HEADERS"] = "Content-Type"
