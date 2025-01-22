import json
import re
import argparse
from typing import List
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime
import logging
import sys
from logger import setup_logging
from protodecoder import ProtoDecoder, VarintStream
from transcoder import Transcoder


def parse_credentials(credentials):
    cred_dict = {}
    for credential in credentials:
        key, value = credential.split("=", 1)
        cred_dict[key.strip()] = value.strip()
    return cred_dict


def decorate_message(msg, data: bytes, format: str) -> str:
    if format == "pretty":
        return f"{msg.topic()}:{
            msg.partition()} @ {msg.offset()} | Key: {msg.key()} | Message: {data}"
    elif format == "json":
        return json.dumps({
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp()[1],
            "key": msg.key().decode('utf-8'),
            "value": data.decode('utf-8'),
        })
    else:
        return data.decode('utf-8')


def consume_messages(brokers: str, credentials: List[str], topic: str, key: str, start_time: int, end_time: int, decorate: str, transcoder: Transcoder, writer, **kwargs):
    logger = logging.getLogger(__name__)
    consumer_config = {
        "bootstrap.servers": ",".join(brokers.split(",")),
        "group.id": "kafkacat",
        "auto.offset.reset": "earliest",
    }

    if credentials:
        consumer_config.update(parse_credentials(credentials))

    consumer = Consumer(consumer_config)
    keys = key.split(',') if key else []
    skip_time = 0
    skip_key = 0
    try:
        consumer.subscribe([topic])

        running = True
        while running:
            msg = consumer.poll(timeout=10)

            if msg is None:
                running = False
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(
                        f"Reached end of partition {msg.partition()} "
                        f"at offset {msg.offset()}."
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                timestamp = msg.timestamp()[1]
                if start_time and timestamp < start_time:
                    skip_time += 1
                    continue
                if keys and not msg.key().decode('utf-8') in keys:
                    skip_key += 1
                    continue
                key, decoded_message = transcoder.transcode(
                    msg.key(), msg.value())
                decorated_message = decorate_message(
                    msg, decoded_message, decorate
                )

                writer(decorated_message)

                if end_time and timestamp >= end_time:
                    logger.debug(
                        f"Reached end_time {msg.partition()} "
                        f"at offset {msg.offset()}."
                    )
                    running = False
        logger.debug(f"Done {skip_time} messages skipped by time,"
                     f"{skip_key} messages skipped by key")
    except Exception as e:
        logger.exception(e)
    finally:
        consumer.close()


def produce_messages(brokers: str, credentials: List[str], topic: str, key: str, transcoder: Transcoder, reader, **kwargs):
    producer_config = {
        "bootstrap.servers": ",".join(brokers.split(",")),
    }

    if credentials:
        producer_config.update(parse_credentials(credentials))

    producer = Producer(producer_config)
    while message := reader():
        _key, decoded_message = transcoder.transcode(None, message)
        _key = _key or key
        if _key:
            producer.produce(
                topic,
                key=_key,
                value=decoded_message,
            )
        else:
            producer.produce(topic, value=decoded_message)
    producer.flush()


class CustomAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        def get_action(x): return next(
            (action for action in parser._actions if action.dest == x), None)
        if values == 'producer':
            get_action('output_format').choices = [
                'json', 'protobuf_binary', 'protobuf_text']
        elif values == 'consumer':
            get_action('input_format').choices = [
                'json', 'protobuf_binary', 'protobuf_text']

        setattr(namespace, self.dest, values)


def main():
    parser = argparse.ArgumentParser(
        description="Read or write Kafka messages.")
    parser.add_argument(
        '--mode', choices=['producer', 'consumer'], required=True, action=CustomAction)
    parser.add_argument("-b", "--brokers", required=True, help="Comma-separated list of Kafka brokers"
                        )
    parser.add_argument("--credentials",
                        nargs="*",
                        default=[],
                        help="Credentials for authentication (SASL)",
                        )
    parser.add_argument("-t", "--topic", required=True, help="Topic name")
    parser.add_argument("--start-time",
                        type=lambda s: datetime.strptime(
                            s, "%Y-%m-%dT%H:%M:%S"),
                        help="Start time in ISO 8601 format (e.g., '2025-10-01T12:00:00')",
                        )
    parser.add_argument("--end-time",
                        type=lambda s: datetime.strptime(
                            s, "%Y-%m-%dT%H:%M:%S"),
                        help="End time in ISO 8601 format (e.g., '2025-10-02T13:30:00')",
                        )
    parser.add_argument("--key",
                        help="Comma separated list of keys for consumer or default key for producer (optional)")
    parser.add_argument("--input-format",
                        choices=["json", "json_key",
                                 "protobuf_binary", "protobuf_text"],
                        default="json",
                        help="Message input format. Exact set depends on mode (default: json)",
                        )
    parser.add_argument("--output-format",
                        choices=["json", "json_key", "hex",
                                 "protobuf_binary", "protobuf_text"],
                        default="json",
                        help="Output format. Exact set depends on mode  (default: json)",
                        )
    parser.add_argument("--decorate",
                        choices=["none", "json", "pretty"],
                        default="none",
                        help="Decorate format (default: none)",
                        )
    parser.add_argument("--proto-files", nargs="+", help="List of .proto files for Protobuf decoding"
                        )
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging")
    parser.add_argument("--log-format",
                        choices=["plain", "json"],
                        default="plain",
                        help="Log format (default: plain)",
                        )
    args = parser.parse_args()

    setup_logging(args.verbose, args.log_format)
    logger = logging.getLogger(__name__)

    if args.proto_files:
        proto_decoder = ProtoDecoder(args.proto_files)
    else:
        proto_decoder = None

    transcoder = Transcoder(
        **vars(args), pretty=args.decorate == "pretty", proto_decoder=proto_decoder, logger=logger)

    if args.mode == 'producer':
        if not args.key and not args.input_format == "json_key" and (args.output_format != "json"):
            raise ValueError("Can not guess how to decode without a key")
        logger.info("Stream To kafka")
        if "protobuf" in args.input_format:
            stream = VarintStream(sys.stdin)
            read = stream.read
        else:
            def read():
                return sys.stdin.readline().encode()
        produce_messages(**vars(args), transcoder=transcoder, reader=read)
    else:
        logger.info("Stream From kafka")
        if "protobuf" in args.output_format:
            def write(data):
                VarintStream(sys.stdout).write(data)
        else:
            def write(data):
                print(data)
        consume_messages(**vars(args), transcoder=transcoder, writer=write)


if __name__ == "__main__":
    main()
