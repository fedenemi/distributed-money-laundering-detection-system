from asyncio import IncompleteReadError

from . import external_serializer


class MsgType:
    TRANSACTIONS_BATCH = 1
    ACCOUNTS_BATCH = 2
    END_TRANSACTIONS = 3
    END_ACCOUNTS = 4
    QUERY_RESULT_BATCH = 5
    END_QUERY = 6
    END_RESULTS = 7
    ACK = 8


# Socket helpers
def _recv_sized(socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


# Deserialize helpers
def _recv_string(socket):
    string_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return external_serializer.deserialize_string(_recv_sized(socket, string_size))


def _recv_row(socket):
    col_count = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    row = []
    for _ in range(col_count):
        row.append(_recv_string(socket))
    return row


def _recv_rows(socket):
    batch_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    rows = []
    for _ in range(batch_size):
        rows.append(_recv_row(socket))
    return rows


def _recv_client_rows(socket):
    client_id = _recv_string(socket)
    rows = _recv_rows(socket)
    return (client_id, rows)


def _recv_query_result_batch(socket):
    client_id = _recv_string(socket)
    query_id = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    rows = _recv_rows(socket)
    return (client_id, query_id, rows)


def _recv_end_query(socket):
    client_id = _recv_string(socket)
    query_id = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return (client_id, query_id)


def _recv_end_client_stream(socket):
    return _recv_string(socket)


def _recv_end_results(socket):
    return _recv_string(socket)


def _recv_ack(socket):
    return _recv_string(socket)


def _recv_empty(socket):
    return None


# Receive handlers
RECV_MSG_HANDLERS = {
    MsgType.TRANSACTIONS_BATCH: _recv_client_rows,
    MsgType.ACCOUNTS_BATCH: _recv_client_rows,
    MsgType.END_TRANSACTIONS: _recv_end_client_stream,
    MsgType.END_ACCOUNTS: _recv_end_client_stream,
    MsgType.QUERY_RESULT_BATCH: _recv_query_result_batch,
    MsgType.END_QUERY: _recv_end_query,
    MsgType.END_RESULTS: _recv_end_results,
    MsgType.ACK: _recv_ack,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


# Serialize helpers
def _serialize_string(value):
    encoded = external_serializer.serialize_string(value)
    return b"".join([
        external_serializer.serialize_uint32(len(encoded)),
        encoded,
    ])


def _serialize_row(row):
    parts = [external_serializer.serialize_uint32(len(row))]
    for item in row:
        parts.append(_serialize_string(item))
    return b"".join(parts)


def _serialize_rows(rows):
    parts = [external_serializer.serialize_uint32(len(rows))]
    for row in rows:
        parts.append(_serialize_row(row))
    return b"".join(parts)


# Send handlers
def _send_transactions_batch(socket, client_id, rows):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.TRANSACTIONS_BATCH),
        _serialize_string(client_id),
        _serialize_rows(rows),
    ]))


def _send_accounts_batch(socket, client_id, rows):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.ACCOUNTS_BATCH),
        _serialize_string(client_id),
        _serialize_rows(rows),
    ]))


def _send_end_transactions(socket, client_id):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.END_TRANSACTIONS),
        _serialize_string(client_id),
    ]))


def _send_end_accounts(socket, client_id):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.END_ACCOUNTS),
        _serialize_string(client_id),
    ]))


def _send_query_result_batch(socket, client_id, query_id, rows):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.QUERY_RESULT_BATCH),
        _serialize_string(client_id),
        external_serializer.serialize_uint32(query_id),
        _serialize_rows(rows),
    ]))


def _send_end_query(socket, client_id, query_id):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.END_QUERY),
        _serialize_string(client_id),
        external_serializer.serialize_uint32(query_id),
    ]))


def _send_end_results(socket, client_id):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.END_RESULTS),
        _serialize_string(client_id),
    ]))


def _send_ack(socket, client_id):
    socket.sendall(b"".join([
        external_serializer.serialize_uint32(MsgType.ACK),
        _serialize_string(client_id),
    ]))


SEND_MSG_HANDLERS = {
    MsgType.TRANSACTIONS_BATCH: _send_transactions_batch,
    MsgType.ACCOUNTS_BATCH: _send_accounts_batch,
    MsgType.END_TRANSACTIONS: _send_end_transactions,
    MsgType.END_ACCOUNTS: _send_end_accounts,
    MsgType.QUERY_RESULT_BATCH: _send_query_result_batch,
    MsgType.END_QUERY: _send_end_query,
    MsgType.END_RESULTS: _send_end_results,
    MsgType.ACK: _send_ack,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)
