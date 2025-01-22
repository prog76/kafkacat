from argparse import Namespace
import codecs
import time
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from parameterized import parameterized
from confluent_kafka import Message
from google.protobuf.json_format import ParseDict


from kafkacat import (
    ProtoDecoder,
    consume_messages,
)
from transcoder import Transcoder

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "testdata")


def load_test_data(filename):
    with open(os.path.join(TEST_DATA_DIR, filename), "rt") as f:
        return json.load(f)


class KafkaLoaderTestCase(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_file = os.path.join(self.temp_dir.name, "output.txt")

    def tearDown(self):
        self.temp_dir.cleanup()

    @parameterized.expand(
        [
            ("json.json",),
            ("hex.json",),
            ("protobuf1.json",),
            ("protobuf2.json",),
        ]
    )
    def test_decode_message(self, test_data_filename):
        test_data = load_test_data(test_data_filename)
        input_data = codecs.decode(test_data["input"], "unicode_escape")
        expected_output = codecs.decode(
            test_data["expected_output"], "unicode_escape").encode()
        proto_files = test_data.get("proto_files", [])
        if proto_files:
            proto_decoder = ProtoDecoder(proto_files)
        else:
            proto_decoder = None
        transcoder = Transcoder(
            input_format=test_data["input_format"], output_format=test_data["output_format"], pretty=False, proto_decoder=proto_decoder, key="test.Main")
        key, actual_output = transcoder.transcode(None, input_data.encode())
        self.assertEqual(actual_output, expected_output)

    @parameterized.expand(
        [
            ("json.json",),
            ("hex.json",),
            ("protobuf1.json",),
            ("protobuf2.json",),
        ]
    )
    @patch("kafkacat.Consumer")
    def test_consume_messages(self, test_data_filename, mock_consumer):
        test_data = load_test_data(test_data_filename)
        input_data = codecs.decode(test_data["input"], "unicode_escape")
        expected_output = codecs.decode(
            test_data["expected_output"], "unicode_escape").encode()
        proto_files = test_data.get("proto_files", [])

        if proto_files:
            proto_decoder = ProtoDecoder(proto_files)
        else:
            proto_decoder = None
        transcoder = Transcoder(
            input_format=test_data["input_format"], output_format=test_data["output_format"], pretty=False, proto_decoder=proto_decoder, key="test.Main")

        # Create a mock Message object
        mock_msg = MagicMock(spec=Message)
        mock_msg.topic.return_value = "test-topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.key.return_value = "test.Main".encode()
        mock_msg.value.return_value = input_data.encode()
        mock_msg.headers.return_value = None
        mock_msg.timestamp.return_value = (0, time.time() + 10)
        mock_msg.error.return_value = None

        def mock_consumer_init(*args, **kwargs):
            consumer_mock = MagicMock(name="Consumer")
            consumer_mock.poll.side_effect = lambda *args, **kwargs: mock_msg
            return consumer_mock

        mock_consumer.side_effect = mock_consumer_init

        with open(self.output_file, "w") as output_file:
            consume_messages(
                brokers="localhost,localhost",
                credentials=[],
                topic="test-topic",
                start_time=time.time(),
                end_time=time.time(),
                key="",
                input_format=test_data["input_format"],
                output_format=test_data["output_format"],
                decorate="none",
                transcoder=transcoder,
                writer=output_file.write,
            )

        with open(self.output_file, "rb") as output_file:
            actual_output = output_file.read()

        self.assertEqual(expected_output, actual_output)


if __name__ == "__main__":
    unittest.main()
