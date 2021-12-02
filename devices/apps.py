from django.apps import AppConfig
from kafka import KafkaConsumer
from logpipe import register_consumer, Consumer, Producer

from devices.api.serializers import TestSerializer


class DevicesConfig(AppConfig):
    name = 'devices'




    # consumer = KafkaConsumer(
    #         'people',
    #         bootstrap_servers=['192.168.1.11:9092'],
    #         group_id=None,
    #         consumer_timeout_ms=1000,
    #         auto_offset_reset='earliest',
    #         enable_auto_commit = True,
    #         auto_commit_interval_ms = 1000
    # )
    #
    # while True:
    #     for message in consumer:
    #         if message is not None:
    #             topic = message.topic
    #             request_body = message.value.decode('utf-8')
    #             print("%s:%d:%d: value=%s" % (
    #                 message.topic, message.partition,
    #                 message.offset, request_body))
    #     consumer.close()
