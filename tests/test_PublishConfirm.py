from rabbitmq.stream.client import Array, PublishConfirm, UInt16, UInt64, UInt8


def test_PublishConfirm():
    command_x = PublishConfirm(
        key=UInt16(1),
        version=UInt16(1),
        publisher_id=UInt8(1),
        publishing_ids=Array([UInt64(1)]),
    )
    data = bytes(command_x)
    command_y, offset = PublishConfirm.from_bytes(data)
    assert offset == len(data)
    assert command_y == command_x
