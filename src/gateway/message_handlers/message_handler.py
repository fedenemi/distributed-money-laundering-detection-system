from common import message_protocol


class MessageHandler:

    def serialize_rows_message(self, client_id, rows):
        return message_protocol.internal.serialize(
            {"client_id": client_id, "rows": rows}
        )

    def serialize_eof_message(self, client_id):
        return message_protocol.internal.serialize(
            {"type": "eof", "client_id": client_id}
        )

    def deserialize_system_message(self, message):
        return message_protocol.internal.deserialize(message)
