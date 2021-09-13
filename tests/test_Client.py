import socket as sockets

from rabbitmq.stream.client import (
    Frame,
    Array,
    PeerPropertiesRequest,
    PeerPropertiesResponse,
    PeerProperty,
    String,
    UInt16,
    UInt32,
)


def test_Client():
    socket = sockets.socket(sockets.AF_INET, sockets.SOCK_STREAM)
    socket.connect(("localhost", 5552))
    peer_properties = PeerPropertiesRequest(
        key=UInt16(17),
        version=UInt16(1),
        correlation_id=UInt32(100),
        peer_properties=Array(
            [
                PeerProperty(String("product"), String("RabbitMQ Stream")),
                PeerProperty(String("version"), String("0.1.0")),
                PeerProperty(String("platform"), String("Python")),
            ]
        ),
    )
    frame: Frame[PeerPropertiesRequest] = Frame(peer_properties)
    socket.send(bytes(frame))
    data = socket.recv(UInt32.byte_size)
    size, _ = UInt32.from_bytes(data)
    data = socket.recv(UInt16.byte_size)
    key, _ = UInt16.from_bytes(data)
    key = int(key) & 0b0111111111111111
    data = socket.recv(int(size) - UInt16.byte_size)
    response, offset = PeerPropertiesResponse.from_bytes(bytes(UInt16(key)) + data)
    print(response)
    socket.close()
