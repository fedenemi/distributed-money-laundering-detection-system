"""
Extensión del middleware existente para soportar sharding.
"""
import pika
import pika.exceptions
from .middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ
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
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=exchange_name, exchange_type="direct", durable=True
            )
            for i in range(n_shards):
                q = f"{exchange_name}_shard_{i}"
                self.channel.queue_declare(queue=q, durable=True)
                self.channel.queue_bind(
                    queue=q, exchange=exchange_name, routing_key=str(i)
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
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except pika.exceptions.AMQPConnectionError:
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

    def __init__(self, host: str, exchange_name: str, shard_id: int):
        self._inner = MessageMiddlewareExchangeRabbitMQ(
            host=host,
            exchange_name=exchange_name,
            routing_keys=[str(shard_id)],
        )

    def start_consuming(self, on_message_callback):
        self._inner.start_consuming(on_message_callback)

    def stop_consuming(self):
        self._inner.stop_consuming()

    def close(self):
        self._inner.close()
