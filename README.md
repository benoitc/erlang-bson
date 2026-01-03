# erlang_bson

[![CI](https://github.com/benoitc/erlang-bson/actions/workflows/ci.yml/badge.svg)](https://github.com/benoitc/erlang-bson/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/erlang_bson.svg)](https://hex.pm/packages/erlang_bson)

High-performance BSON encoder/decoder for Erlang.

## Features

- **Zero-copy traversal** - Navigate BSON documents without decoding values
- **Efficient skipping** - Skip entire subtrees using BSON length prefixes
- **Memory safe** - All decoded values are copied to avoid retaining source binaries
- **Full type support** - All common BSON types including decimal128

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {erlang_bson, "0.1.0"}
]}.
```

## Quick Start

### Encode a map to BSON

```erlang
Map = #{
    <<"_id">> => {objectid, <<1,2,3,4,5,6,7,8,9,10,11,12>>},
    <<"name">> => <<"Alice">>,
    <<"age">> => 30,
    <<"tags">> => [<<"developer">>, <<"erlang">>]
},
{ok, Bson} = bson_codec:encode_map(Map).
```

### Decode BSON to a map

```erlang
{ok, Map} = bson_codec:decode_map(Bson).
```

### Zero-copy traversal

```erlang
%% Create iterator
{ok, Iter} = bson_iter:new(Bson),

%% Iterate elements
{ok, Key, Type, ValueRef, Iter2} = bson_iter:next(Iter),

%% Decode only when needed
{ok, Value} = bson_iter:decode_value(Type, ValueRef).
```

### Direct field lookup

```erlang
%% Find a top-level key
{ok, Type, ValueRef} = bson_iter:peek(Bson, <<"name">>),
{ok, <<"Alice">>} = bson_iter:decode_value(Type, ValueRef).

%% Navigate nested paths
{ok, Type, ValueRef} = bson_iter:find_path(Bson, [<<"address">>, <<"city">>]).
```

## Modules

### bson_iter

Zero-copy BSON binary iterator for hot paths. Use this when you need to:
- Filter documents without full decode
- Access specific fields efficiently
- Skip large nested structures

### bson_codec

Convenience encode/decode for Erlang maps. Use this when you need to:
- Fully decode documents for processing
- Encode Erlang maps for storage
- Work with documents in admin tools or tests

## Type Mappings

| Erlang Value | BSON Type |
|--------------|-----------|
| `integer` (32-bit range) | int32 |
| `integer` (64-bit range) | int64 |
| `float` | double |
| `binary` | string (UTF-8) |
| `true` / `false` | boolean |
| `null` | null |
| `map` | document |
| `list` | array |
| `{objectid, <<12 bytes>>}` | objectid |
| `{datetime_ms, Integer}` | datetime |
| `{binary, Subtype, Data}` | binary |
| `{timestamp, Increment, Time}` | timestamp |
| `{decimal128, Coeff, Exp}` | decimal128 |
| `{regex, Pattern, Options}` | regex |
| `minkey` / `maxkey` | minkey / maxkey |

## Memory Safety

ValueRefs from `bson_iter` point into the original binary without copying. This is efficient but means the source binary stays in memory.

To release the source binary:
- Call `bson_iter:decode_value/2` which uses `binary:copy/1`
- Use `bson_codec:decode_map/1` for full document decode

## License

Apache-2.0
