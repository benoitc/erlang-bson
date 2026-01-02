%% @doc BSON type tag constants and shared record definitions.
%% BSON Specification v1.1: https://bsonspec.org/spec.html

-ifndef(BSON_TYPES_HRL).
-define(BSON_TYPES_HRL, true).

%% =============================================================================
%% BSON Type Tags (single byte)
%% =============================================================================

-define(BSON_TYPE_DOUBLE,      16#01).  % 64-bit IEEE 754 floating point
-define(BSON_TYPE_STRING,      16#02).  % UTF-8 string (int32 len + data + \x00)
-define(BSON_TYPE_DOCUMENT,    16#03).  % Embedded document
-define(BSON_TYPE_ARRAY,       16#04).  % Array (document with "0","1",... keys)
-define(BSON_TYPE_BINARY,      16#05).  % Binary data (int32 len + subtype + data)
-define(BSON_TYPE_UNDEFINED,   16#06).  % Deprecated - undefined
-define(BSON_TYPE_OBJECTID,    16#07).  % 12-byte ObjectId
-define(BSON_TYPE_BOOLEAN,     16#08).  % Boolean (1 byte: 0x00 or 0x01)
-define(BSON_TYPE_DATETIME,    16#09).  % UTC datetime (int64 ms since epoch)
-define(BSON_TYPE_NULL,        16#0A).  % Null value (0 bytes)
-define(BSON_TYPE_REGEX,       16#0B).  % Regular expression (cstring + cstring)
-define(BSON_TYPE_DBPOINTER,   16#0C).  % Deprecated - DBPointer
-define(BSON_TYPE_JAVASCRIPT,  16#0D).  % JavaScript code
-define(BSON_TYPE_SYMBOL,      16#0E).  % Deprecated - Symbol
-define(BSON_TYPE_JAVASCRIPT_SCOPE, 16#0F). % JavaScript with scope
-define(BSON_TYPE_INT32,       16#10).  % 32-bit signed integer
-define(BSON_TYPE_TIMESTAMP,   16#11).  % MongoDB internal timestamp
-define(BSON_TYPE_INT64,       16#12).  % 64-bit signed integer
-define(BSON_TYPE_DECIMAL128,  16#13).  % 128-bit decimal floating point
-define(BSON_TYPE_MINKEY,      16#FF).  % Min key (internal)
-define(BSON_TYPE_MAXKEY,      16#7F).  % Max key (internal)

%% =============================================================================
%% Binary Subtypes
%% =============================================================================

-define(BSON_SUBTYPE_GENERIC,    16#00).  % Generic binary
-define(BSON_SUBTYPE_FUNCTION,   16#01).  % Function
-define(BSON_SUBTYPE_BINARY_OLD, 16#02).  % Binary (old, deprecated)
-define(BSON_SUBTYPE_UUID_OLD,   16#03).  % UUID (old, deprecated)
-define(BSON_SUBTYPE_UUID,       16#04).  % UUID (RFC 4122)
-define(BSON_SUBTYPE_MD5,        16#05).  % MD5 hash
-define(BSON_SUBTYPE_ENCRYPTED,  16#06).  % Encrypted BSON value
-define(BSON_SUBTYPE_USER,       16#80).  % User-defined (0x80-0xFF)

%% =============================================================================
%% Iterator Record
%% =============================================================================
%%
%% The iterator holds:
%%   - bin: the original BSON document binary (kept as reference, not copied)
%%   - pos: current parsing position (byte offset from start)
%%   - doc_end: position of the terminating null byte (bin size - 1 for top-level)
%%
%% This design allows sub-binary slicing without copying the underlying data.
%% CAUTION: The original binary is retained in memory as long as any iterator
%% or ValueRef derived from it exists.

-record(bson_iter, {
    bin     :: binary(),     % Original document binary
    pos     :: non_neg_integer(), % Current offset
    doc_end :: non_neg_integer()  % Document end offset (before terminator)
}).

%% =============================================================================
%% Value Reference
%% =============================================================================
%%
%% ValueRef is a map pointing into the original binary without copying.
%% Used by traversal to defer decoding until explicitly requested.
%%
%% Fields:
%%   - bin: reference to the original binary
%%   - off: byte offset where the value data starts
%%   - len: byte length of the value data (or 'unknown' for cstrings/variable)
%%
%% Example: For a double at offset 15, ValueRef = #{bin => Bin, off => 15, len => 8}

-type value_ref() :: #{
    bin := binary(),
    off := non_neg_integer(),
    len := non_neg_integer() | unknown
}.

-type bson_type() ::
    double | string | document | array | binary | objectid |
    boolean | datetime | null | int32 | int64 | timestamp |
    decimal128 | regex | javascript | minkey | maxkey |
    {unsupported, byte()}.

-export_type([value_ref/0, bson_type/0]).

-endif. % BSON_TYPES_HRL
