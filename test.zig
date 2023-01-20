const std = @import("std");
const pb = @import("protobuf.zig");

const Example = struct {
    single1: u32,
    single2: u32,
    opt: ?u64,
    rep: []i32,
    map: pb.Map([]const u8, f32),
    options: ?union(enum) {
        foo: u32,
        bar: []const u8,

        pub const pb_desc = .{
            .foo = pb.fd(5, .varint),
            .bar = pb.fd(10, .bytes),
        };
    },

    pub const pb_desc = .{
        .single1 = pb.fd(1, .varint),
        .single2 = pb.fd(42, .fixed),
        .opt = pb.fd(2, .varint),
        .rep = pb.fd(3, .{ .repeat_pack = &pb.fe(.zigzag) }),
        .map = pb.fd(4, .{ .map = &[2]pb.FieldEncoding{ .string, .default } }),
    };
};

test {
    var rep = [3]i32{ 69, 0, 42 };

    var map = pb.Map([]const u8, f32).init(std.testing.allocator);
    defer map.deinit();

    try map.put("hello", 1);
    try map.put("", 2);
    try map.put("this has\x00embedded nuls", 3);

    const ex: Example = .{
        .single1 = 256,
        .single2 = 0,
        .opt = 0,
        .rep = &rep,
        .map = map,
        .options = .{ .bar = "ziggify all the kingdoms" },
    };

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try pb.encodeMessage(buf.writer(), std.testing.allocator, ex);

    var fbs = std.io.fixedBufferStream(buf.items);
    const decoded = try pb.decodeMessage(Example, fbs.reader(), std.testing.allocator);
    defer decoded.deinit();

    try std.testing.expectEqual(ex.single1, decoded.msg.single1);
    try std.testing.expectEqual(ex.single2, decoded.msg.single2);
    try std.testing.expectEqual(ex.opt, decoded.msg.opt);
    try std.testing.expectEqualSlices(i32, ex.rep, decoded.msg.rep);
    try std.testing.expectEqual(ex.map.count(), decoded.msg.map.count());

    var it = ex.map.iterator();
    while (it.next()) |pair| {
        const other = decoded.msg.map.get(pair.key_ptr.*) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(pair.value_ptr.*, other);
    }

    try std.testing.expect(ex.options != null);
    try std.testing.expectEqual(std.meta.activeTag(ex.options.?), std.meta.activeTag(decoded.msg.options.?));
    try std.testing.expectEqualSlices(u8, ex.options.?.bar, decoded.msg.options.?.bar);
}
