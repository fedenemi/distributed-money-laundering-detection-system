"""
Extensión del middleware existente para soportar sharding.
"""
import pika
import pika.exceptions
from .middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ, _connection_parameters
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError


class ShardedExchangeProducer:
    """
    Productor que publica a un exchange direct con routing key elegida
    dinámicamente (para sharding). No consume.

    Separado del Exchange consumer porque el productor no necesita
    declarar una cola propia: solo necesita el exchange.
    """

    def __init__(self, host: str, exchange_name: str, n_shards: int):
        self.exchange_name = exchange_name
        self.n_shards = n_shards
        try:
            self.connection = pika.BlockingConnection(
                _connection_parameters(host)
            )
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.channel.exchange_declare(
                exchange=exchange_name, exchange_type="direct", durable=True
            )
        except Exception:
            if hasattr(self, "connection") and self.connection.is_open:
                self.connection.close()
            raise

    def send_to_shard(self, message: bytes, shard_index: int):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=str(shard_index),
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.ChannelWrongStateError,
            pika.exceptions.StreamLostError,
        ):
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    def send_to_key(self, message: bytes, key: str):
        """Calcula el shard por hash(key) y publica."""
        shard = hash(key) % self.n_shards
        self.send_to_shard(message, shard)

    def send_eof_to_all(self, eof_body: bytes):
        for i in range(self.n_shards):
            self.send_to_shard(eof_body, i)

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            pass


class ShardedExchangeConsumer:
    """
    Consumidor de un shard específico de un exchange direct.
    Usa MessageMiddlewareExchangeRabbitMQ del proyecto con el
    routing_key = str(shard_id).
    """

    def __init__(self, host: str, exchange_name: str, shard_id: int, consumer_group: str):
        queue_name = f"{exchange_name}_{consumer_group}_shard_{shard_id}"
        self._inner = MessageMiddlewareExchangeRabbitMQ(
            host=host,
            exchange_name=exchange_name,
            routing_keys=[str(shard_id)],
            queue_name=queue_name,
            durable=False,
            exclusive=False,
        )

    def start_consuming(self, on_message_callback):
        self._inner.start_consuming(on_message_callback)

    def stop_consuming(self):
        self._inner.stop_consuming()

    def close(self):
        self._inner.close()
