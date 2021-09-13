import struct

from dataclasses import dataclass, fields
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
    _GenericAlias,
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
        for i in range(int(length)):
            item, item_offset = self.type.from_bytes(data[offset:])
            items.append(item)
            offset += item_offset
        return Array(items), offset


class AutoBytes:
    def __bytes__(self) -> bytes:
        data: bytes = bytes()

        for field in fields(self):
            data += bytes(getattr(self, field.name))

        return data

    @classmethod
    def from_bytes(cls, data: bytes):
        offset = 0
        items = []
        for field in fields(cls):
            typ = field.type
            if isinstance(typ, _GenericAlias) and typ.__origin__ is Array:
                typ = ArrayBytesReader(typ.__args__[0])
            item, additional_offset = typ.from_bytes(data[offset:])
            items.append(item)
            offset += additional_offset
        return cls(*items), offset  # type: ignore


@dataclass
class DeclarePublisherRequest(AutoBytes):
    key: UInt16
    version: UInt16
    correlation_id: UInt32
    publisher_id: UInt8
    publisher_references: Array[String]
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
    published_messages: Array[PublishedMessage]


@dataclass
class PublishConfirm(AutoBytes):
    key: UInt16
    version: UInt16
    publisher_id: UInt8
    publishing_ids: Array[UInt64]


command = PublishConfirm.from_bytes(
    bytes(PublishConfirm(UInt16(1), UInt16(1), UInt8(1), Array([UInt64(1)])))
)
