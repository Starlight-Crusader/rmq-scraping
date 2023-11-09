import pika


class Publisher:
    def send(queue_name, msg):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=queue_name)

        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=msg   
        )

        print(f"[>] {msg} pushed to the queue")

        connection.close()


class Consumer:
    def process(queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=queue_name)

        def callback(ch, method, properties, body):
            print(f" [x] Processing {body}")
        
        channel.basic_consume(
            queue=queue_name,
            auto_ack=True,
            on_message_callback=callback
        )

        print(' [*] Waiting for messages. To exit press CTRL+C')
    
        channel.start_consuming()