const std = @import("std");

/// Convenience wrapper for constructing FieldDescriptors.
/// TODO I hate this
pub fn fd(field_num: u29, encoding: FieldEncoding) FieldDescriptor {
    return .{
        .field_num = field_num,
        .encoding = encoding,
    };
}

/// Convenience wrapper for making sure we get FieldEncoding and not the corresponding enum tag.
/// TODO I hate this
pub fn fe(encoding: FieldEncoding) FieldEncoding {
    return encoding;
}

/// Describes how a single struct field is encoded. For 'repeated' fields, this specifies whether
/// the values are packed; otherwise it just tells us the representation of the actual type (e.g.
/// to differentiate between u32 and fixed32).
pub const FieldEncoding = union(enum) {
    default, // bool; float/double (f32/f64); submessage (struct with pb_desc)
    fixed, // [s]fixed[32/64]
    varint, // [u]int[32/64]
    zigzag, // sint[32/64]
    string, // string
    repeat_pack: *const FieldEncoding, // repeated (child encoding)
    repeat: *const FieldEncoding, // repeated (child encoding)
    bytes, // bytes
    map: [*]const FieldEncoding, //*const [2]FieldEncoding, // map (k/v encodings)
};

/// A descriptor for a single field, giving its field number and encoding. These should be stored in
/// a 'pb_desc' decl on the message struct. 'oneof' values, represented as optional tagged unions,
/// are the only field type which should not have a corresponding descriptor index, but they must
/// contain their own 'pb_desc' decl describing the fields within them.
pub const FieldDescriptor = struct {
    field_num: u29,
    encoding: FieldEncoding,
};

/// Convenience wrapper for constructing protobuf maps.
pub fn Map(comptime K: type, comptime V: type) type {
    return std.HashMapUnmanaged(K, V, struct {
        pub fn hash(_: @This(), key: K) u64 {
            var hasher = std.hash.Wyhash.init(0);
            std.hash.autoHashStrat(&hasher, key, .Deep);
            return hasher.final();
        }
        pub fn eql(_: @This(), a: K, b: K) bool {
            return if (comptime std.meta.trait.isSlice(K))
                std.mem.eql(std.meta.Child(K), a, b)
            else
                a == b;
        }
    }, std.hash_map.default_max_load_percentage);
}

/// A Protobuf wire type - all data is encoded as one of these.
const WireType = enum(u3) {
    varint,
    i64,
    len,
    sgroup, // DEPRECATED
    egroup, // DEPRECATED
    i32,
};

/// Encode 'val' into the given writer as LEB128.
fn encodeVarInt(w: anytype, val: u64) !void {
    if (val == 0) {
        try w.writeByte(0);
        return;
    }

    var x = val;
    while (x != 0) {
        const part: u8 = @truncate(u7, x);
        x >>= 7;
        const next: u8 = @boolToInt(x != 0);
        try w.writeByte(next << 7 | part);
    }
}

/// Encode a field tag, composed of a field number and associated wire type.
fn encodeTag(w: anytype, field_num: u29, wire_type: WireType) !void {
    const wire = @enumToInt(wire_type);
    const val = @as(u32, wire) | @as(u32, field_num) << 3;
    return encodeVarInt(w, val);
}

/// Encode 'val' of scalar type (integer, float, bool, string, or bytes) with field descriptor
/// 'desc' into the given writer. If 'encode_default' is false, the field will be omitted if it
/// corresponds to its type's default value. If 'include_tag' is false, the field's tag is not
/// included in the output.
fn encodeSingleScalar(w: anytype, val: anytype, comptime desc: FieldDescriptor, comptime encode_default: bool, comptime override_default: ?@TypeOf(val), comptime include_tag: bool) !void {
    const T = @TypeOf(val);

    switch (T) {
        bool => {
            if (desc.encoding != .default) @compileError("Boolean types must use FieldEncoding.default");
            if (!encode_default and val == (override_default orelse false)) return;
            if (include_tag) try encodeTag(w, desc.field_num, .varint);
            try w.writeByte(@boolToInt(val));
        },

        u32, u64, i32, i64 => {
            if (!encode_default and val == (override_default orelse 0)) return;
            switch (desc.encoding) {
                .fixed => {
                    if (include_tag) try encodeTag(w, desc.field_num, switch (T) {
                        u32, i32 => .i32,
                        u64, i64 => .i64,
                        else => unreachable,
                    });
                    try w.writeIntLittle(T, val);
                },
                .varint => {
                    if (include_tag) try encodeTag(w, desc.field_num, .varint);
                    const val64: u64 = switch (T) {
                        u32, u64 => val,
                        i32 => @bitCast(u64, @as(i64, val)), // sign-extend
                        i64 => @bitCast(u64, val),
                        else => unreachable,
                    };
                    try encodeVarInt(w, val64);
                },
                .zigzag => {
                    if (@typeInfo(T).Int.signedness != .signed) @compileError("Only signed integral types can use FieldEncoding.zigzag");
                    if (include_tag) try encodeTag(w, desc.field_num, .varint);
                    if (val >= 0) {
                        try encodeVarInt(w, @intCast(u64, val) * 2);
                    } else {
                        try encodeVarInt(w, @intCast(u64, -val - 1) * 2 + 1);
                    }
                },
                else => @compileError("Integral types must use FieldEncoding.fixed, FieldEncoding.varint, or FieldEncoding.zigzag"),
            }
            return;
        },

        f32, f64 => {
            if (desc.encoding != .default) @compileError("Floating types must use FieldEncoding.default");
            if (!encode_default and val == (override_default orelse 0)) return;
            if (T == f32) {
                if (include_tag) try encodeTag(w, desc.field_num, .i32);
                try w.writeIntLittle(u32, @bitCast(u32, val));
            } else {
                if (include_tag) try encodeTag(w, desc.field_num, .i64);
                try w.writeIntLittle(u64, @bitCast(u64, val));
            }
        },

        []u8, []const u8 => {
            if (override_default != null) @compileError("Cannot override default for []u8");
            if (!encode_default and val.len == 0) return;
            switch (desc.encoding) {
                .string, .bytes => {
                    if (include_tag) try encodeTag(w, desc.field_num, .len);
                    try encodeVarInt(w, val.len);
                    try w.writeAll(val);
                },
                else => @compileError("[]u8 must use FieldEncoding.string or FieldEncoding.bytes"),
            }
        },

        else => @compileError("Type '" ++ @typeName(T) ++ "' cannot be encoded as a primitive"),
    }
}

/// Encode a single value of scalar or submessage type. 'map's are not included here since
/// they're sugar for a 'repeated' submessage (and cannot themselves be repeated), meaning they are
/// really multiple values.
fn encodeSingleValue(w: anytype, ally: std.mem.Allocator, val: anytype, comptime desc: FieldDescriptor, comptime encode_default: bool, comptime override_default: ?@TypeOf(val)) !void {
    const T = @TypeOf(val);

    if (@typeInfo(T) == .Struct) {
        if (desc.encoding != .default) @compileError("Sub-messages must use FieldEncoding.default");

        var buf = std.ArrayList(u8).init(ally);
        defer buf.deinit();

        try encodeMessage(buf.writer(), ally, val);

        try encodeTag(w, desc.field_num, .len);
        try encodeVarInt(w, buf.items.len);
        try w.writeAll(buf.items);
    } else {
        try encodeSingleScalar(w, val, desc, encode_default, override_default, true);
    }
}

/// Encode the field 'val' with 'desc_opt' as its descriptor (null if none exists) into the given
/// writer. 'field_name' is used only for error messages.
fn encodeAnyField(
    w: anytype,
    ally: std.mem.Allocator,
    val: anytype,
    comptime desc_opt: ?FieldDescriptor,
    comptime field_name: []const u8,
    comptime field_default: ?@TypeOf(val),
) !void {
    const T = @TypeOf(val);

    // Nicer error message if you forgot to make your union optional
    if (@typeInfo(T) == .Union) {
        @compileError("Only optional unions can be encoded");
    }

    if (@typeInfo(T) == .Optional and
        @typeInfo(std.meta.Child(T)) == .Union)
    {
        // oneof
        const U = std.meta.Child(T);
        if (desc_opt != null) @compileError("Union '" ++ field_name ++ "' must not have a field descriptor");
        if (val) |un| {
            if (!@hasDecl(U, "pb_desc")) @compileError("Union 'must have a pb_desc decl");
            switch (un) {
                inline else => |payload, tag| {
                    if (!@hasField(@TypeOf(U.pb_desc), @tagName(tag))) {
                        @compileError("Mising descriptor for field '" ++ @typeName(U) ++ "." ++ @tagName(tag) ++ "'");
                    }
                    const sub_desc = @field(U.pb_desc, @tagName(tag));
                    try encodeSingleValue(w, ally, payload, sub_desc, true, null);
                },
            }
        }

        return;
    }

    const desc = desc_opt orelse @compileError("Missing descriptor for field '" ++ field_name ++ "'");

    if (desc.encoding == .repeat) {
        for (val) |x| {
            try encodeSingleValue(w, ally, x, .{
                .field_num = desc.field_num,
                .encoding = desc.encoding.repeat.*,
            }, true, null);
        }
    } else if (desc.encoding == .repeat_pack) {
        var buf = std.ArrayList(u8).init(ally);
        defer buf.deinit();

        for (val.items) |x| {
            try encodeSingleScalar(buf.writer(), x, .{
                .field_num = desc.field_num,
                .encoding = desc.encoding.repeat_pack.*,
            }, true, null, false);
        }

        try encodeTag(w, desc.field_num, .len);
        try encodeVarInt(w, buf.items.len);
        try w.writeAll(buf.items);
    } else if (desc.encoding == .map) {
        var it = val.iterator();
        while (it.next()) |pair| {
            try encodeSingleValue(w, ally, struct {
                k: std.meta.FieldType(T.KV, .key),
                v: std.meta.FieldType(T.KV, .value),
                const pb_desc = .{
                    .k = fd(1, desc.encoding.map[0]),
                    .v = fd(2, desc.encoding.map[1]),
                };
            }{ .k = pair.key_ptr.*, .v = pair.value_ptr.* }, .{
                .field_num = desc.field_num,
                .encoding = .default,
            }, true, null);
        }
    } else if (@typeInfo(T) == .Optional) {
        if (val) |x| {
            try encodeSingleValue(w, ally, x, desc, true, null);
        }
    } else {
        try encodeSingleValue(w, ally, val, desc, false, field_default);
    }
}

/// Encode an entire Protobuf message 'msg' into the given writer. Only temporary allocations are
/// performed, all of which are cleaned up before this function returns.
pub fn encodeMessage(w: anytype, ally: std.mem.Allocator, msg: anytype) !void {
    const Msg = @TypeOf(msg);
    if (!@hasDecl(Msg, "pb_desc")) @compileError("Message type '" ++ @typeName(Msg) ++ "' must have a pb_desc decl");

    validateDescriptors(Msg);

    inline for (@typeInfo(Msg).Struct.fields) |field| {
        const desc: ?FieldDescriptor = if (@hasField(@TypeOf(Msg.pb_desc), field.name))
            @field(Msg.pb_desc, field.name)
        else
            null;

        const default: ?field.type = if (field.default_value) |ptr|
            @ptrCast(*const field.type, ptr).*
        else
            null;

        try encodeAnyField(w, ally, @field(msg, field.name), desc, @typeName(Msg) ++ "." ++ field.name, default);
    }
}

/// Perform some basic checks on the field descriptors in the message type 'Msg', ensuring every
/// descriptor corresponds to a field and that field numbers appear at most once.
fn validateDescriptors(comptime Msg: type) void {
    comptime {
        var seen_field_nums: []const u29 = &.{};
        validateDescriptorsInner(Msg, &seen_field_nums);
        for (seen_field_nums) |x, i| {
            for (seen_field_nums[i + 1 ..]) |y| {
                if (x == y) {
                    @compileError(std.fmt.comptimePrint("Duplicate field number {} in type '{s}'", .{ x, @typeName(Msg) }));
                }
            }
        }
    }
}

fn validateDescriptorsInner(comptime Msg: type, comptime seen_field_nums: *[]const u29) void {
    for (std.meta.fieldNames(@TypeOf(Msg.pb_desc))) |name| {
        if (!@hasField(Msg, name)) {
            @compileError("Descriptor '" ++ name ++ "' does not correspond to any field in type '" + @typeName(Msg));
        }
    }

    for (std.meta.fields(Msg)) |field| {
        if (@hasField(@TypeOf(Msg.pb_desc), field.name)) {
            seen_field_nums.* = seen_field_nums.* ++ &[1]u29{@field(Msg.pb_desc, field.name).field_num};
        }

        if (@typeInfo(field.type) == .Struct and @hasDecl(field.type, "pb_desc")) {
            validateDescriptors(field.type);
        } else if (@typeInfo(field.type) == .Optional and
            @typeInfo(std.meta.Child(field.type)) == .Union and
            @hasDecl(std.meta.Child(field.type), "pb_desc"))
        {
            validateDescriptorsInner(std.meta.Child(field.type), seen_field_nums);
        }
    }
}

/// A small wrapper around a decoded message. You must call 'deinit' once you're done with the
/// message to free all its allocated memory.
pub fn Decoded(comptime Msg: type) type {
    return struct {
        msg: Msg,
        arena: std.heap.ArenaAllocator,

        const Self = @This();

        pub fn deinit(self: Self) void {
            self.arena.deinit();
        }
    };
}

fn initDefault(comptime Msg: type, arena: std.mem.Allocator) Msg {
    var result: Msg = undefined;

    inline for (comptime std.meta.fields(Msg)) |field| {
        if (comptime std.meta.trait.isSlice(field.type)) {
            @field(result, field.name) = &.{};
            continue;
        }

        const default: ?field.type = if (field.default_value) |ptr|
            @ptrCast(*const field.type, ptr).*
        else
            null;

        @field(result, field.name) = switch (@typeInfo(field.type)) {
            .Optional => default orelse null,
            .Int, .Float => default orelse 0,
            .Bool => default orelse false,
            .Struct => if (@hasDecl(field.type, "pb_desc"))
                initDefault(field.type, arena)
            else
                field.type{},
            else => @compileError("Type '" ++ @typeName(field.type) ++ "' cannot be deserialized"),
        };
    }

    return result;
}

fn decodeVarInt(r: anytype) !u64 {
    var shift: u6 = 0;
    var x: u64 = 0;
    while (true) {
        const b = try r.readByte();
        x |= @as(u64, @truncate(u7, b)) << shift;
        if (b >> 7 == 0) break;
        shift += 7;
    }
    return x;
}

fn skipField(r: anytype, wire_type: WireType, field_num: u29) !void {
    switch (wire_type) {
        .varint => _ = try decodeVarInt(r),
        .i64 => _ = try r.readIntLittle(u64),
        .len => {
            const len = try decodeVarInt(r);
            try r.skipBytes(len, .{});
        },
        .sgroup => {
            while (true) {
                const tag = try decodeVarInt(r);
                const sub_wire = std.meta.intToEnum(WireType, @truncate(u3, tag)) catch return error.MalformedInput;
                const sub_num = std.math.cast(u29, tag >> 3) orelse return error.MalformedInput;
                if (sub_wire == .egroup and sub_num == field_num) {
                    break;
                }
                try skipField(r, sub_wire, sub_num);
            }
        },
        .egroup => return error.MalformedInput,
        .i32 => _ = try r.readIntLittle(u32),
    }
}

fn decodeSingleScalar(comptime T: type, comptime encoding: FieldEncoding, r: anytype, arena: std.mem.Allocator, wire_type: WireType) !T {
    switch (T) {
        bool => {
            if (encoding != .default) @compileError("Boolean types must use FieldEncoding.default");
            if (wire_type != .varint) return error.MalformedInput;
            const x = try decodeVarInt(r);
            return @truncate(u32, x) != 0;
        },

        u32, u64, i32, i64 => {
            switch (encoding) {
                .fixed => {
                    switch (T) {
                        u32, i32 => if (wire_type != .i32) return error.MalformedInput,
                        u64, i64 => if (wire_type != .i64) return error.MalformedInput,
                        else => unreachable,
                    }
                    return r.readIntLittle(T);
                },
                .varint => {
                    if (wire_type != .varint) return error.MalformedInput;
                    const Unsigned = switch (T) {
                        u32, i32 => u32,
                        u64, i64 => u64,
                        else => unreachable,
                    };
                    return @bitCast(T, @truncate(Unsigned, try decodeVarInt(r)));
                },
                .zigzag => {
                    if (@typeInfo(T).Int.signedness != .signed) @compileError("Only signed integral types can use FieldEncoding.zigzag");
                    if (wire_type != .varint) return error.MalformedInput;
                    const raw = try decodeVarInt(r);
                    const val = if (raw % 2 == 1)
                        -@intCast(i64, raw / 2) - 1
                    else
                        @intCast(i64, raw / 2);
                    return @truncate(T, val);
                },
                else => @compileError("Integral types must use FieldEncoding.fixed, FieldEncoding.varint, or FieldEncoding.zigzag"),
            }
        },

        f32, f64 => {
            if (encoding != .default) @compileError("Floating types must use FieldEncoding.default");
            if (T == f32) {
                if (wire_type != .i32) return error.MalformedInput;
                return @bitCast(f32, try r.readIntLittle(u32));
            } else {
                if (wire_type != .i64) return error.MalformedInput;
                return @bitCast(f64, try r.readIntLittle(u64));
            }
        },

        []u8, []const u8 => {
            if (encoding != .string and encoding != .bytes) @compileError("[]u8 must use FieldEncoding.string or FieldEncoding.bytes");
            if (wire_type != .len) return error.MalformedInput;
            const len = try decodeVarInt(r);
            const buf = try arena.alloc(u8, len);
            try r.readNoEof(buf);
            return buf;
        },

        else => @compileError("Type '" ++ @typeName(T) ++ "' cannot be decoded as a primitive"),
    }
}

/// Decodes a value of scalar or submessage type, returning the result.
fn decodeSingleValue(comptime T: type, comptime encoding: FieldEncoding, r: anytype, arena: std.mem.Allocator, wire_type: WireType) !T {
    if (@typeInfo(T) == .Struct) {
        if (encoding != .default) @compileError("Sub-messages must use FieldEncoding.default");
        if (wire_type != .len) return error.MalformedInput;
        const len = try decodeVarInt(r);
        var lr = std.io.limitedReader(r, len);
        return decodeMessageInner(T, lr.reader(), arena);
    } else {
        return decodeSingleScalar(T, encoding, r, arena, wire_type);
    }
}

/// Attempts to decode a field of any type, modifying the result location as necessary (either
/// overwriting the value or appending data). Returns true if this message corresponded to the given
/// field (and was decoded).
fn maybeDecodeAnyField(comptime T: type, comptime desc_opt: ?FieldDescriptor, comptime field_name: []const u8, r: anytype, arena: std.mem.Allocator, wire_type: WireType, field_num: u29, result: *T) !bool {
    // Nicer error message if you forgot to make your union optional
    if (@typeInfo(T) == .Union) {
        @compileError("Only optional unions can be decoded");
    }

    if (@typeInfo(T) == .Optional and @typeInfo(std.meta.Child(T)) == .Union) {
        if (desc_opt != null) @compileError("Union must not have a field descriptor");
        if (try maybeDecodeOneOf(std.meta.Child(T), r, arena, wire_type, field_num)) |val| {
            result.* = val;
            return true;
        } else {
            return false;
        }
    }

    const desc = desc_opt orelse @compileError("Missing descriptor for field '" ++ field_name ++ "'");

    if (field_num != desc.field_num) return false;

    if (desc.encoding == .repeat or desc.encoding == .repeat_pack) {
        const Elem = std.meta.Child(T.Slice);
        const scalar_elem = switch (@typeInfo(Elem)) {
            .Int, .Bool, .Float => true,
            else => Elem == []u8 or Elem == []const u8,
        };
        if (desc.encoding == .repeat_pack and !scalar_elem) {
            @compileError("Packed repeated fields must be slices of scalar types");
        }
        const child_enc = switch (desc.encoding) {
            .repeat, .repeat_pack => |e| e.*,
            else => unreachable,
        };
        // By spec, decoders should be able to decode non-packed repeated fields as packed and vice
        // versa, so that the protocol can be changed whilst preserving forwards and backwards
        // compatibility.
        if (scalar_elem) {
            if (wire_type == .len) {
                const len = try decodeVarInt(r);
                var lr = std.io.limitedReader(r, len);
                const expect_wire: WireType = switch (child_enc) {
                    .fixed => switch (Elem) {
                        u32, i32, f32 => .i32,
                        u64, i64, f64 => .i64,
                        else => undefined, // not unreachable to defer to nice error handling in decodeSingleScalar
                    },
                    .varint, .zigzag => .varint,
                    .string, .bytes => .len,
                    .default => switch (Elem) {
                        bool => .varint,
                        f32 => .i32,
                        f64 => .i64,
                        else => undefined, // not unreachable to defer to nice error handling in decodeSingleScalar
                    },
                    else => undefined,
                };

                while (decodeSingleScalar(Elem, child_enc, lr.reader(), arena, expect_wire)) |elem| {
                    try result.*.append(arena, elem);
                } else |err| switch (err) {
                    error.EndOfStream => {},
                    else => |e| return e,
                }

                return true;
            }
        }

        const elem = try decodeSingleScalar(Elem, child_enc, r, arena, wire_type);
        try result.*.append(arena, elem);
    } else if (desc.encoding == .map) {
        const val = try decodeSingleValue(struct {
            k: std.meta.FieldType(T.KV, .key),
            v: std.meta.FieldType(T.KV, .value),
            const pb_desc = .{
                .k = fd(1, desc.encoding.map[0]),
                .v = fd(2, desc.encoding.map[1]),
            };
        }, .default, r, arena, wire_type);
        try result.put(arena, val.k, val.v);
    } else if (@typeInfo(T) == .Optional) {
        result.* = try decodeSingleValue(std.meta.Child(T), desc.encoding, r, arena, wire_type);
    } else {
        result.* = try decodeSingleValue(T, desc.encoding, r, arena, wire_type);
    }

    return true;
}

fn maybeDecodeOneOf(comptime U: type, r: anytype, arena: std.mem.Allocator, wire_type: WireType, field_num: u29) !?U {
    if (!@hasDecl(U, "pb_desc")) @compileError("Union must have a pb_desc decl");

    inline for (std.meta.fields(U)) |field| {
        if (!@hasField(@TypeOf(U.pb_desc), field.name)) {
            @compileError("Missing descriptor for field '" ++ @typeName(U) ++ "." ++ field.name ++ "'");
        }
        const desc = @field(U.pb_desc, field.name);

        if (desc.field_num == field_num) {
            const payload = try decodeSingleValue(field.type, desc.encoding, r, arena, wire_type);
            return @unionInit(U, field.name, payload);
        }
    }

    return null;
}

fn decodeMessageInner(comptime Msg: type, r: anytype, arena: std.mem.Allocator) !Msg {
    if (!@hasDecl(Msg, "pb_desc")) @compileError("Message type '" ++ @typeName(Msg) ++ "' must have a pb_desc decl");
    validateDescriptors(Msg);

    var result = initDefault(Msg, arena);

    while (decodeVarInt(r)) |tag| {
        const wire_type = std.meta.intToEnum(WireType, @truncate(u3, tag)) catch return error.MalformedInput;
        const field_num = std.math.cast(u29, tag >> 3) orelse return error.MalformedInput;

        inline for (std.meta.fields(Msg)) |field| {
            const desc_opt: ?FieldDescriptor = if (@hasField(@TypeOf(Msg.pb_desc), field.name))
                @field(Msg.pb_desc, field.name)
            else
                null;

            if (try maybeDecodeAnyField(field.type, desc_opt, @typeName(Msg) ++ "." ++ field.name, r, arena, wire_type, field_num, &@field(result, field.name))) {
                break;
            }
        } else {
            try skipField(r, wire_type, field_num);
        }
    } else |err| switch (err) {
        error.EndOfStream => {},
        else => |e| return e,
    }

    return result;
}

pub fn decodeMessage(comptime Msg: type, r: anytype, ally: std.mem.Allocator) !Decoded(Msg) {
    var arena = std.heap.ArenaAllocator.init(ally);
    errdefer arena.deinit();

    return .{
        .msg = try decodeMessageInner(Msg, r, arena.allocator()),
        .arena = arena,
    };
}
