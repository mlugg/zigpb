const std = @import("std");
const pb = @import("protobuf.zig");

fn expectEqualMessages(comptime T: type, expected: T, actual: T) !void {
    if (@typeInfo(T) == .Optional) {
        try std.testing.expectEqual(expected == null, actual == null);
        return expectEqualMessages(std.meta.Child(T), expected.?, actual.?);
    }

    if (@typeInfo(T) == .Union) {
        try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(actual));
        switch (expected) {
            inline else => |val, tag| {
                return expectEqualMessages(@TypeOf(val), val, @field(actual, @tagName(tag)));
            },
        }
    }

    if (@typeInfo(T) == .Struct) {
        if (@hasDecl(T, "pb_desc")) {
            inline for (comptime std.meta.fields(T)) |field| {
                try expectEqualMessages(field.type, @field(expected, field.name), @field(actual, field.name));
            }
        } else if (@hasDecl(T, "GetOrPutResult")) {
            try std.testing.expectEqual(expected.count(), actual.count());
            var it = expected.iterator();
            while (it.next()) |pair| {
                const val = actual.get(pair.key_ptr.*) orelse return error.TestExpectedEqual;
                try std.testing.expectEqual(pair.value_ptr.*, val);
            }
        } else if (@hasDecl(T, "Slice")) {
            try std.testing.expectEqualSlices(std.meta.Child(T.Slice), expected.items, actual.items);
        } else {
            @compileError("Cannot test equality of type '" ++ @typeName(T) ++ "'");
        }
        return;
    }

    if (T == []const u8 or T == []u8) {
        return std.testing.expectEqualSlices(u8, expected, actual);
    }

    switch (@typeInfo(T)) {
        .Int, .Float, .Enum => try std.testing.expectEqual(expected, actual),
        else => @compileError("Cannot test equality of type '" ++ @typeName(T) ++ "'"),
    }
}

fn initMessage(comptime T: type, comptime val: anytype, arena: std.mem.Allocator) !T {
    if (@typeInfo(T) == .Optional) {
        if (@typeInfo(@TypeOf(val)) == .Optional) {
            return if (val) |x| try initMessage(std.meta.Child(T), x, arena) else null;
        } else {
            return try initMessage(std.meta.Child(T), val, arena);
        }
    }

    if (@typeInfo(T) == .Union) {
        if (@typeInfo(@TypeOf(val)) != .Struct) @compileError("Expected struct literal to initialize union");
        const fields = @typeInfo(@TypeOf(val)).Struct.fields;
        if (fields.len != 1) @compileError("Expected single-element struct to initialize union");
        return @unionInit(T, fields[0].name, try initMessage(
            std.meta.TagPayload(T, @field(std.meta.Tag(T), fields[0].name)),
            @field(val, fields[0].name),
            arena,
        ));
    }

    if (@typeInfo(T) == .Struct) {
        if (@hasDecl(T, "pb_desc")) {
            var result: T = undefined;
            inline for (comptime std.meta.fields(T)) |field| {
                @field(result, field.name) = try initMessage(field.type, @field(val, field.name), arena);
            }
            return result;
        } else if (@hasDecl(T, "GetOrPutResult")) {
            var result: T = .{};
            inline for (val) |pair| {
                try result.put(arena, pair[0], pair[1]);
            }
            return result;
        } else if (@hasDecl(T, "Slice")) {
            var result: T = .{};
            try result.appendSlice(arena, &val);
            return result;
        } else {
            @compileError("Cannot initalize type '" ++ @typeName(T) ++ "'");
        }
        return;
    }

    if (T == []u8 and @TypeOf(val) == []const u8) {
        return arena.dupe(val);
    } else {
        return val;
    }
}

fn testEncodeDecode(comptime Msg: type, comptime val: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const msg = try initMessage(Msg, val, arena.allocator());

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try pb.encodeMessage(buf.writer(), std.testing.allocator, msg);

    var fbs = std.io.fixedBufferStream(buf.items);
    const decoded = try pb.decodeMessage(Msg, fbs.reader(), std.testing.allocator);
    defer decoded.deinit();

    try expectEqualMessages(Msg, msg, decoded.msg);
}

test {
    try testEncodeDecode(struct {
        single1: u32,
        single2: u32,
        opt: ?u64,
        rep: std.ArrayListUnmanaged(i32),
        map: pb.Map([]const u8, f32),
        options: ?union(enum) {
            foo: u32,
            bar: []const u8,
            pub const pb_desc = .{
                .foo = .{ 5, .varint },
                .bar = .{ 10, .bytes },
            };
        },
        embedded: struct {
            x: u32,
            y: i64,
            pub const pb_desc = .{
                .x = .{ 1, .varint },
                .y = .{ 2, .zigzag },
            };
        },
        en: enum {
            val1,
            val2,
            val3,
        },

        pub const pb_desc = .{
            .single1 = .{ 1, .varint },
            .single2 = .{ 42, .fixed },
            .opt = .{ 2, .varint },
            .rep = .{ 3, .{ .repeat_pack = .zigzag } },
            .map = .{ 4, .{ .map = .{ .string, .default } } },
            .embedded = .{ 6, .default },
            .en = .{ 7, .default },
        };
    }, .{
        .single1 = 256,
        .single2 = 0,
        .opt = 0,
        .rep = .{ 69, 0, 42 },
        .map = .{
            .{ "hello", 1 },
            .{ "", 2 },
            .{ "this has\x00embedded nuls", 3 },
        },
        .options = .{
            .bar = "ziggify all the kingdoms",
        },
        .embedded = .{
            .x = 1 << 20,
            .y = -2048,
        },
        .en = .val2,
    });
}
