import logging
import socket
import traceback

from client import Client


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    client = Client()

    try:
        logging.info("Starting client")
        client.run()
        logging.info("Client finished successfully")
        return 0

    except socket.error as e:

        logging.error(f"Socket error detected: {e}")

        logging.error(traceback.format_exc())

        if not client.closed:
            logging.error("The connection with the server was lost")

        return 1

    except Exception as e:

        logging.error(f"Unhandled client error: {e}")

        logging.error(traceback.format_exc())

        return 2
    finally:
        try:
            if not client.closed:
                client.disconnect()
        except Exception as e:
            logging.error(f"Error disconnecting client: {e}")


if __name__ == "__main__":
    exit(main())