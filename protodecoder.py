import os
import tempfile

import grpc_tools.protoc
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.message_factory import GetMessages
from google.protobuf.internal import encoder
from google.protobuf.internal import decoder


class VarintStream:
    def __init__(self, stream):
        self.stream = stream

    def __iter__(self):
        return self

    def __next__(self):
        if message := self.read():
            return message
        else:
            raise StopIteration

    def read(self):
        size = decoder._DecodeVarint(self.stream)
        if size is None:
            return None

        message = self.stream.read(size)
        if len(message) != size:
            raise EOFError("Unexpected end of stream while reading message")
        return message

    def write(self, message):
        size = len(message)
        encoder._EncodeVarint(self.stream.write, size)
        self.stream.write(message)


class ProtoDecoder:
    def __init__(self, proto_files: list[str]):
        self.message_classes: dict[str, type] = {}

        abs_paths = [os.path.abspath(proto_file) for proto_file in proto_files]

        for proto_file in abs_paths:
            if not os.path.exists(proto_file):
                raise FileNotFoundError(f"Proto file not found: {proto_file}")
        try:
            with tempfile.NamedTemporaryFile(delete_on_close=False) as fp:
                fp.close()
                self._compile_protos(abs_paths, fp.name)
                self._load_descriptors_and_messages(fp.name)
        except Exception as e:
            raise RuntimeError(f"Error during proto processing: {e}")

    def _compile_protos(self, abs_paths: list[str], desc_file: str):
        try:
            dir = os.getcwd()
            os.chdir(os.path.dirname(abs_paths[0]))
            command = [
                "grpc_tools.protoc",
                "--include_imports",
                f"--descriptor_set_out={desc_file}",
                "-I",
                os.path.dirname(abs_paths[0]),
            ] + abs_paths
            grpc_tools.protoc.main(command)
        finally:
            os.chdir(dir)

    def _load_descriptors_and_messages(self, desc_file: str):
        with open(desc_file, "rb") as desc_f:
            file_descriptor_set = FileDescriptorSet.FromString(desc_f.read())

        self.message_classes.update(
            GetMessages(list(file_descriptor_set.file)))

    def get_message_class(self, message_name: str) -> type:
        return self.message_classes.get(message_name)
