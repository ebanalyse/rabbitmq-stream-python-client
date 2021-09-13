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
    data = socket.recv(4)
    size, offset = UInt32.from_bytes(data)
    print(size)
    data = socket.recv(int(size))
    response, offset = PeerPropertiesResponse.from_bytes(data)
    print(response)
    socket.close()
