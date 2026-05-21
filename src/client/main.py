import logging
import socket

from client import Client


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.run()
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
