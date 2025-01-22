from argparse import Namespace
import codecs
import io
import time
import json
import os
import tempfile
import unittest
from unittest.mock import patch
from parameterized import parameterized
from testcontainers.kafka import KafkaContainer

from kafkacat import ProtoDecoder, consume_messages, produce_messages
from protodecoder import VarintStream
from transcoder import Transcoder

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "testdata")


def load_test_data(filename):
    with open(os.path.join(TEST_DATA_DIR, filename), "r") as f:
        return json.load(f)


class KafkaLoaderTestCase(unittest.TestCase):
    USERNAME = "test"
    PASSWORD = "pass"

    @classmethod
    def setUpClass(cls):

        cls.kafka_container = KafkaContainer(
            image="confluentinc/cp-kafka:7.5.0",
        )
        jaas_config = (
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='admin' password='admin' "
            f"user_{cls.USERNAME}='{cls.PASSWORD}';"
        )
        cls.kafka_container.security_protocol_map = (
            "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT"
        )
        cls.kafka_container.with_kraft()
        cls.kafka_container.with_cluster_id("6PMpHYL9QkeyXRj9Nrp4KA")
        cls.kafka_container.env.update(
            {
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT",
                "KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS": "PLAIN",
                "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG": jaas_config,
            }
        )

        cls.kafka_container.start()
        cls.kafka_brokers = cls.kafka_container.get_bootstrap_server()

    @classmethod
    def tearDownClass(cls):
        cls.kafka_container.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_file = os.path.join(self.temp_dir.name, "output.txt")

    def tearDown(self):
        self.temp_dir.cleanup()

    @parameterized.expand(
        [
            ("json.json",),
            ("protobuf1.json",),
        ]
    )
    def test_consume_messages(self, test_data_filename):
        test_data = load_test_data(test_data_filename)
        expected_output = codecs.decode(
            test_data["expected_output"], "unicode_escape").encode()
        proto_files = test_data.get("proto_files", [])
        if proto_files:
            proto_decoder = ProtoDecoder(proto_files)
        else:
            proto_decoder = None
        base = io.BytesIO()
        stream = VarintStream(base)
        stream.write(expected_output)
        base.seek(0)
        transcoder_produce = Transcoder(
            input_format=test_data["output_format"], output_format=test_data["input_format"], pretty=False, proto_decoder=proto_decoder, key="test.Main")
        transcoder_consume = Transcoder(
            input_format=test_data["input_format"], output_format=test_data["output_format"], pretty=False, proto_decoder=proto_decoder, key="test.Main")

        produce_messages(
            brokers=self.kafka_brokers,
            credentials=[
                "security.protocol=SASL_PLAINTEXT",
                "sasl.mechanisms=PLAIN",
                f"sasl.username={self.USERNAME}",
                f"sasl.password={self.PASSWORD}",
            ],
            key=None,
            topic="test-topic",
            transcoder=transcoder_produce,
            reader=stream.read
        )

        with open(self.output_file, "w") as output_file:
            consume_messages(
                brokers=self.kafka_brokers,
                credentials=[
                    "security.protocol=SASL_PLAINTEXT",
                    "sasl.mechanisms=PLAIN",
                    f"sasl.username={self.USERNAME}",
                    f"sasl.password={self.PASSWORD}",
                ],
                topic="test-topic",
                start_time=time.time(),
                end_time=time.time(),
                key="",
                input_format=test_data["input_format"],
                output_format=test_data["output_format"],
                decorate="none",
                transcoder=transcoder_consume,
                writer=output_file.write
            )

        with open(self.output_file, "rb") as output_file:
            actual_output = output_file.read()

        self.assertEqual(expected_output, actual_output)


if __name__ == "__main__":
    unittest.main()
