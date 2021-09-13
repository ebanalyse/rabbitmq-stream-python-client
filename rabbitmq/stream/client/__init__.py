import struct

from abc import ABC
from dataclasses import dataclass, field, fields
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    List,
    Literal,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    SupportsBytes,
    Union,
)

T = TypeVar("T", bound=SupportsBytes)


class BytesReader(Protocol):
    from_bytes: Callable[[bytes], Tuple[Any, int]]


@dataclass
class BaseInt(SupportsBytes, BytesReader):
    value: int
    format: ClassVar[str]
    byte_size: ClassVar[int]

    def __int__(self):
        return self.value

    def __bytes__(self) -> bytes:
        return struct.pack(f">{self.format}", self.value)

    @classmethod
    def from_bytes(cls, data: bytes):
        offset = cls.byte_size
        data = data[:offset]
        return cls(*struct.unpack(f">{cls.format}", data)), len(data)


@dataclass
class Int8(BaseInt):
    format = "b"
    byte_size = 1


@dataclass
class Int16(BaseInt):
    format = "h"
    byte_size = 2


@dataclass
class Int32(BaseInt):
    format = "i"
    byte_size = 4


@dataclass
class Int64(BaseInt):
    format = "q"
    byte_size = 8


@dataclass
class UInt8(BaseInt):
    format = "B"
    byte_size = 1


@dataclass
class UInt16(BaseInt):
    format = "H"
    byte_size = 2


@dataclass
class UInt32(BaseInt):
    format = "I"
    byte_size = 4


@dataclass
class UInt64(BaseInt):
    format = "Q"
    byte_size = 8


@dataclass
class Bytes(SupportsBytes):
    value: bytes

    def __bytes__(self) -> bytes:
        b = bytes(Int32(len(self.value)))
        b += self.value
        return b


@dataclass
class String(SupportsBytes):
    value: Union[str, None]

    @classmethod
    def from_bytes(cls, data: bytes):
        length, offset = Int16.from_bytes(data[:2])
        value = b"".join(
            struct.unpack("c" * int(length), data[offset : offset + int(length)])
        )
        offset += len(value)
        value = value.decode()
        return cls(value), offset

    def __bytes__(self) -> bytes:
        if self.value is None:
            return bytes(Int16(-1))
        e = self.value.encode()
        b = bytes(Int16(len(e)))
        b += self.value.encode()
        return b

    def __len__(self):
        if self.value is None:
            return -1
        return len(self.value.encode())

    def __str__(self):
        return self.value


@dataclass
class Array(Generic[T]):
    items: List[T]

    def __bytes__(self) -> bytes:
        b = bytes(Int32(len(self.items)))
        for item in self.items:
            b += bytes(item)
        return b

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index: int):
        return self.items[index]

    def __len__(self):
        return len(self.items)


@dataclass
class ArrayBytesReader:
    type: Type[BytesReader]

    def from_bytes(self, data: bytes):
        length, offset = Int32.from_bytes(data[:4])
        items = []
        for _ in range(int(length)):
            item, item_offset = self.type.from_bytes(data[offset:])
            items.append(item)
            offset += item_offset
        return Array(items), offset


class AutoBytes:
    def __bytes__(self) -> bytes:
        data: bytes = bytes()

        for field in self.get_instance_fields():
            data += bytes(getattr(self, field.name))

        return data

    @classmethod
    def from_bytes(
        cls, data: bytes
    ) -> Tuple[Union[SupportsBytes, BytesReader, "AutoBytes"], int]:
        offset: int = 0
        items = []
        for field in cls.get_class_fields():
            if field.metadata.get("reader") is not None:
                reader = field.metadata["reader"]
            else:
                reader = field.type
            item, additional_offset = reader.from_bytes(data[offset:])
            items.append(item)
            offset += additional_offset
        instance = cls(*items)  # type: ignore
        return instance, offset

    def get_instance_fields(self):
        return fields(self)

    @classmethod
    def get_class_fields(cls):
        return fields(cls)


@dataclass
class DeclarePublisherRequest(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    publisher_id: UInt8
    publisher_references: Array[String] = field(
        metadata={"reader": ArrayBytesReader(String)}
    )
    stream: String


@dataclass
class DeclarePublisherResponse(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    response_code: UInt16
    publisher_id: UInt8


@dataclass
class Publish(AutoBytes):
    @dataclass
    class PublishedMessage(AutoBytes):
        publishing_id: UInt64
        message: Bytes

    key: UInt16
    version: UInt16
    publisher_id: UInt8
    published_messages: Array[PublishedMessage] = field(
        metadata={"reader": ArrayBytesReader(PublishedMessage)}
    )


@dataclass
class PublishConfirm(AutoBytes):
    key: UInt16
    version: UInt16
    publisher_id: UInt8
    publishing_ids: Array[UInt64] = field(metadata={"reader": ArrayBytesReader(UInt64)})


@dataclass
class PublishError(AutoBytes):
    @dataclass
    class PublishingError(AutoBytes):
        publishing_id: UInt64
        code: UInt16

    key: UInt16
    version: UInt16
    publisher_id: UInt8
    publishing_error: PublishingError


@dataclass
class QueryPublisherRequest(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    publisher_reference: String
    stream: String


@dataclass
class QueryPublisherResponse(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    response_code: UInt16
    sequence: UInt64


@dataclass
class DeletePublisherRequest(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    publisher_id: UInt8


@dataclass
class DeletePublisherResponse(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    response_code: UInt16


@dataclass
class Subscribe(AutoBytes):
    @dataclass
    class OffsetSpecification(AutoBytes):
        offset_type: UInt16
        offset: UInt64

    @dataclass
    class Property(AutoBytes):
        key: String
        value: String

    key: UInt16
    version: UInt16
    correlation_id: UInt32
    subscription_id: UInt8
    stream: String
    offset_specification: OffsetSpecification
    credit: UInt16
    properties: Array[Property] = field(metadata={"reader": ArrayBytesReader(Property)})
