import pika
import random
import string
import os

import pika.exceptions
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

RABBITMQ_HEARTBEAT = int(os.environ.get("RABBITMQ_HEARTBEAT", "3600"))
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = int(os.environ.get("RABBITMQ_BLOCKED_CONNECTION_TIMEOUT", "3600"))

def _connection_parameters(host):
    return pika.ConnectionParameters(
        host=host,
        heartbeat=RABBITMQ_HEARTBEAT,
        blocked_connection_timeout=RABBITMQ_BLOCKED_CONNECTION_TIMEOUT,
    )

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(_connection_parameters(host))
            self.channel = self.connection.channel()
            self.queue_name = queue_name
            self.channel.queue_declare(queue=queue_name)
        except Exception:
            if self.connection.is_open:
                self.connection.close()

    
    def start_consuming(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            def ack():
                try:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    pass
            def nack():
                try:
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                except Exception:
                    pass
            on_message_callback(body, ack, nack)
        try:
            #self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback_wrapper, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()

        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
            

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        
        except pika.exceptions.ChannelWrongStateError:
            raise MessageMiddlewareDisconnectedError
        
        except Exception:
            raise MessageMiddlewareMessageError


    def close(self):
        try:
            self.connection.close()
        except:
            raise MessageMiddlewareCloseError

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys, queue_name="", durable=False, exclusive=True):
        try:
            self.connection = pika.BlockingConnection(_connection_parameters(host))
            self.channel = self.connection.channel()

            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
            self.exchange_name = exchange_name

            result = self.channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
            )
            self.queue_name = queue_name or result.method.queue

            self.routing_keys = routing_keys

            for key in routing_keys:
                self.channel.queue_bind(self.queue_name, exchange_name, key)
            self.channel.basic_qos(prefetch_count=1)
                

        except Exception:
            if self.connection.is_open:
                self.connection.close()
            raise
    
    def start_consuming(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            def ack(): ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack(): ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        
        try:
            #self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback_wrapper, auto_ack=False)
            self.channel.start_consuming()

        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.StreamLostError,
        ):
            raise MessageMiddlewareDisconnectedError
        
        except Exception:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.StreamLostError,
        ):
            raise MessageMiddlewareDisconnectedError

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_keys[0],
                body=message,
            )

        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.StreamLostError,
        ):
            raise MessageMiddlewareDisconnectedError
        
        except pika.exceptions.ChannelWrongStateError:
            raise MessageMiddlewareDisconnectedError
        
        except Exception:
            raise MessageMiddlewareMessageError

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except:
            raise MessageMiddlewareCloseError
