from typing import Dict
import pika
import json

class RabbitmqPublisher:
    def __init__(self) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "guest"
        self.__password = "guest"
        self.__exchange = "data_exchange"
        self.__channel = self.__create_channel()
    
    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )
        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.__exchange, exchange_type='direct', durable=True)
        return channel
    
    def send_message(self, routing_key: str, body: Dict):
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key=routing_key,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2 # Persistent messages
            )
        )

rabbitmq_publisher = RabbitmqPublisher()
# the routing key is useful when a exchange has to fill different queues
rabbitmq_publisher.send_message("microservice_name", {"Ola": "Mundo"})