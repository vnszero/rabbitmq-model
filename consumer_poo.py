import pika

class RabbitmqConsumer:
    def __init__(self, queue: str, callback) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "guest"
        self.__password = "guest"
        self.__queue = queue
        self.__callback = callback
        self.__channel = self.__create_channel()

    def __on_message(self, ch, method, properties, body):
        try:
            self.__callback(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing message: {e}")
            # Optionally, reject the message to requeue or dead-letter
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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
        channel.queue_declare(
            queue=self.__queue,
            # arguments={ # this is necessary if the query has special arguments
            #     "x-overflow": "reject-publish"
            # },
            durable=True
        )
        channel.basic_qos(prefetch_count=1) # Prevent one consumer from being overwhelmed
        channel.basic_consume(
            queue=self.__queue, 
            on_message_callback=self.__on_message
        )

        return channel
    
    def start(self):
        print(f"Listening on queue {self.__queue}")
        self.__channel.start_consuming()

def my_callback(body):
    print("Received message:", body.decode('utf-8'))

rabbitmq_consumer = RabbitmqConsumer("microservice_queue", my_callback)
rabbitmq_consumer.start()