# zigpb

This is a simple Protobuf encoding and decoding library written in Zig. It aims to support Protobuf 3, but also supports custom defaults, meaning any Protobuf 2
message which does not use the deprecated groups feature can also be used.

Details of the API are currently subject to change. Some planned changes are as follows:

- Add a comptime parser for `.proto` files
