import argparse
import pika
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision
from os import getenv
import logging
import ssl
import wagglemsg as message
from contextlib import ExitStack
from prometheus_client import start_http_server, Counter

messages_processed_total = Counter("loader_messages_processed_total", "Total messages processed by data loader.")


def assert_type(obj, t):
    if not isinstance(obj, t):
        raise TypeError(f"{obj!r} must be {t}")


def assert_maxlen(s, n):
    if len(s) > n:
        raise ValueError(f"len({s!r}) must be <= {n}")


def assert_valid_message(msg):
    assert_type(msg.name, str)
    assert_maxlen(msg.name, 64)
    assert_type(msg.timestamp, int)
    assert_type(msg.value, (int, float, str))
    assert_type(msg.meta, dict)
    for k, v in msg.meta.items():
        assert_type(k, str)
        assert_maxlen(k, 64)
        assert_type(v, str)
        assert_maxlen(v, 64)
    if "node" not in msg.meta:
        raise KeyError("message missing node meta field")


def coerce_value(x):
    if isinstance(x, int):
        return float(x)
    return x


class MessageHandler:

    def __init__(self, rabbitmq_conn: pika.BlockingConnection, influxdb_client: InfluxDBClient, influxdb_bucket: str,
            influxdb_org: str, max_flush_interval: float, max_batch_size: int):
        self.rabbitmq_conn = rabbitmq_conn
        self.influxdb_client = influxdb_client
        self.influxdb_bucket = influxdb_bucket
        self.influxdb_org = influxdb_org
        self.max_flush_interval = max_flush_interval
        self.max_batch_size = max_batch_size
        self.batch = []

    def flush(self):
        if len(self.batch) == 0:
            return

        logging.info("flushing batch with %d records", len(self.batch))
        records = []

        # create records from batch
        for ch, method, properties, body in self.batch:
            try:
                msg = message.load(body)
            except Exception:
                logging.exception("failed to parse message")
                continue

            try:
                assert_valid_message(msg)
            except Exception:
                logging.exception("dropping invalid message: %s", msg)
                continue

            # # check that meta["node"] matches user_id
            # if "node-"+msg.meta["node"] != properties.user_id:
            #     logging.info("dropping invalid message: username (%s) doesn't match node meta (%s) - ", msg.meta["node"], properties.user_id)
            #     continue

            logging.debug("creating record for msg: %s value-type: %s", msg, type(msg.value))
            records.append({
                "measurement": msg.name,
                "tags": msg.meta,
                "fields": {
                    "value": coerce_value(msg.value),
                },
                "time": msg.timestamp,
            })

        # write entire batch to influxdb
        logging.info("writing %d records to influxdb", len(records))
        with self.influxdb_client.write_api(write_options=SYNCHRONOUS) as write_api:
            try:
                write_api.write(self.influxdb_bucket, self.influxdb_org, records, write_precision=WritePrecision.NS)
            except InfluxDBError as exc:
                # TODO(sean) InfluxDB only responds with single invalid data point message.
                # Although the write goes through for the valid data points, getting this info
                # could be helpful for debugging. We may need to leverage a known schema later
                # to be more proactive about the problem.
                logging.error("error when writing batch: %s", exc.message)

        # ack entire batch
        logging.info("acking batch")
        for ch, method, properties, body in self.batch:
            ch.basic_ack(method.delivery_tag)

        messages_processed_total.inc(len(self.batch))
        self.batch.clear()
        logging.info("flushed batch")

    def handle(self, ch, method, properties, body):
        # ensure we flush new batch within max flush interval
        if len(self.batch) == 0:
            self.rabbitmq_conn.call_later(self.max_flush_interval, self.flush)

        self.batch.append((ch, method, properties, body))

        # ensure we flush when batch is large enough
        if len(self.batch) >= self.max_batch_size:
            self.flush()


def get_pika_credentials(args):
    if args.rabbitmq_username != "":
        return pika.PlainCredentials(args.rabbitmq_username, args.rabbitmq_password)
    return pika.credentials.ExternalCredentials()


def get_ssl_options(args):
    if args.rabbitmq_cacertfile == "":
        return None
    context = ssl.create_default_context(cafile=args.rabbitmq_cacertfile)
    # HACK this allows the host and baked in host to be configured independently
    context.check_hostname = False
    if args.rabbitmq_certfile != "":
        context.load_cert_chain(args.rabbitmq_certfile, args.rabbitmq_keyfile)
    return pika.SSLOptions(context, args.rabbitmq_host)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--rabbitmq_host",default=getenv("RABBITMQ_HOST", "localhost"))
    parser.add_argument("--rabbitmq_port", default=getenv("RABBITMQ_PORT", "5672"), type=int)
    parser.add_argument("--rabbitmq_username", default=getenv("RABBITMQ_USERNAME", ""))
    parser.add_argument("--rabbitmq_password", default=getenv("RABBITMQ_PASSWORD", ""))
    parser.add_argument("--rabbitmq_cacertfile", default=getenv("RABBITMQ_CACERTFILE", ""))
    parser.add_argument("--rabbitmq_certfile", default=getenv("RABBITMQ_CERTFILE", ""))
    parser.add_argument("--rabbitmq_keyfile", default=getenv("RABBITMQ_KEYFILE", ""))
    parser.add_argument("--rabbitmq_exchange", default=getenv("RABBITMQ_EXCHANGE", "waggle.msg"))
    parser.add_argument("--rabbitmq_queue", default=getenv("RABBITMQ_QUEUE", "influx-messages"))
    parser.add_argument("--influxdb_url", default=getenv("INFLUXDB_URL", "http://localhost:8086"))
    parser.add_argument("--influxdb_token", default=getenv("INFLUXDB_TOKEN"))
    parser.add_argument("--influxdb_bucket", default=getenv("INFLUXDB_BUCKET", "waggle"))
    parser.add_argument("--influxdb_org", default=getenv("INFLUXDB_ORG", "waggle"))
    parser.add_argument("--max_flush_interval", default=getenv("MAX_FLUSH_INTERVAL", "1.0"), type=float, help="max flush interval")
    parser.add_argument("--max_batch_size", default=getenv("MAX_BATCH_SIZE", "5000"), type=int, help="max batch size")
    parser.add_argument("--metrics_port", default=getenv("METRICS_PORT", "8080"), type=int, help="port to expose metrics")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S")
    # pika logging is too verbose, so we turn it down.
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    credentials = get_pika_credentials(args)
    ssl_options = get_ssl_options(args)

    params = pika.ConnectionParameters(
        host=args.rabbitmq_host,
        port=args.rabbitmq_port,
        credentials=credentials,
        ssl_options=ssl_options,
        retry_delay=60,
        socket_timeout=10.0)

    start_http_server(args.metrics_port)

    with ExitStack() as es:
        logging.info("connecting to influxdb at %s", args.influxdb_url)
        client = es.enter_context(InfluxDBClient(
            url=args.influxdb_url,
            token=args.influxdb_token,
            org=args.influxdb_org,
            enable_gzip=True,
        ))
        logging.info("connected to influxdb")

        logging.info("connecting to rabbitmq")
        conn = es.enter_context(pika.BlockingConnection(params))
        logging.info("connected to rabbitmq")

        ch = conn.channel()
        ch.queue_declare(args.rabbitmq_queue, durable=True)
        ch.queue_bind(args.rabbitmq_queue, args.rabbitmq_exchange, "#")

        handler = MessageHandler(
            rabbitmq_conn=conn,
            influxdb_client=client,
            influxdb_bucket=args.influxdb_bucket,
            influxdb_org=args.influxdb_org,
            max_flush_interval=args.max_flush_interval,
            max_batch_size=args.max_batch_size,
        )
        ch.basic_consume(args.rabbitmq_queue, handler.handle)
        ch.start_consuming()


if __name__ == "__main__":
    main()