import logging
from typing import Any, Tuple
import json

from protodecoder import ProtoDecoder
from google.protobuf.json_format import MessageToJson, ParseDict
from google.protobuf.text_format import Parse, MessageToString


class Transcoder:
    def __init__(self, input_format: str, output_format: str, key: str, pretty: bool, proto_decoder: ProtoDecoder, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.input_format = input_format
        self.output_format = output_format
        self.pretty = pretty
        self.pipeline = []
        self.keys = [item.encode() for item in key.split(',')] if key else []
        formats = set([output_format.split('_')[0],
                      input_format.split('_')[0]])
        if input_format == "json_key":
            self.pipeline.append(self._decode_json_key)
        if "protobuf" in formats and len(formats) == 2:
            if not proto_decoder:
                raise ValueError("Proto files required for transcoding")
            self.proto_decoder = proto_decoder
            if "protobuf" in input_format:
                self.pipeline.append(self._decode_proto)
            if "protobuf" in output_format:
                self.pipeline.append(self._encode_proto)
        if output_format == "json_key":
            self.pipeline.append(self._encode_json_key)
        elif output_format == "hex":
            self.pipeline.append(self._encode_hex)

    def transcode(self, key: bytes, data: bytes) -> Tuple[str, bytes]:
        for step in self.pipeline:
            key, data = step(key, data)
        return key, data

    def _decode_proto(self, key: bytes, data: bytes) -> Tuple[bytes, bytes]:
        keys = [key] if key else self.keys
        if not keys:
            raise ValueError(
                f"Could not decode protobuf for message without key,  '{
                    data}'."
            )
        for key in keys:
            _key = key.decode("utf-8")
            message_class = self.proto_decoder.get_message_class(_key)
            if message_class is None:
                raise ValueError(
                    f"Could not find message class for type '{_key}'.")
            try:
                deserialized_message = message_class()
                if self.input_format == "protobuf_binary":
                    deserialized_message.ParseFromString(data)
                else:
                    Parse(data, deserialized_message)

                json_str = MessageToJson(
                    deserialized_message,
                    preserving_proto_field_name=True,
                    indent=2 if self.pretty else None,
                )
                decoded_message = json_str
                return key, decoded_message.encode()
            except Exception as e:
                raise ValueError(
                    f"Error deserializing or converting to JSON :{e}")

    def _decode_json_key(self, key: bytes, data: bytes) -> Tuple[bytes, bytes]:
        parsed_data = json.loads(data.decode('utf-8'))
        key = parsed_data['key']
        json_message = parsed_data['msg']
        return key.encode(), json_message.encode()

    def _encode_proto(self, key: bytes, data: bytes) -> Tuple[bytes, bytes]:
        keys = [key] if key else self.keys
        if not keys:
            raise ValueError(
                f"Could not encode protobuf for message without key,  '{
                    data}'."
            )
        for key in keys:
            _key = key.decode("utf-8")
            message_class = self.proto_decoder.get_message_class(_key)
            if message_class is None:
                raise ValueError(
                    f"Could not find message class for type '{_key}'.")
            try:
                msg = ParseDict(json.loads(data), message_class())
                if self.output_format == "protobuf_binary":
                    return key, msg.SerializeToString()
                else:
                    return key, MessageToString(msg, as_one_line=not self.pretty).encode()
            except Exception as e:
                raise ValueError(
                    f"Error serializing or converting from JSON :{e}")

    def _encode_json_key(self, key: bytes, data: bytes) -> Tuple[bytes, bytes]:
        return key, json.dumps({
            'key': key.decode('utf-8'),
            'msg': data.decode('utf-8')
        }, indent=2 if self.pretty else None).encode()

    def _encode_hex(self, key: bytes, data: bytes) -> Tuple[bytes, bytes]:
        return key, ':'.join(f'{byte:02X}' for byte in data).encode()
