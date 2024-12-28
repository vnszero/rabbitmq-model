import pika

print("Digite um numero para sua mensagem: ")
note = input()

connection_parameters = pika.ConnectionParameters(
    host="localhost",
    port=5672,
    credentials=pika.PlainCredentials(
        username="guest",
        password="guest"
    )
)

channel = pika.BlockingConnection(connection_parameters).channel()

channel.basic_publish(
    exchange="data_exchange",
    routing_key="",
    body="Minha Mensagem N#" + note,
    properties=pika.BasicProperties(
        delivery_mode=2 # persistence mode
    )
)