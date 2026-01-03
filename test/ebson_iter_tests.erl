-module(ebson_iter_tests).

-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Test Fixtures - Hand-crafted BSON binaries
%% =============================================================================

%% Empty document: {"$": 5 bytes total}
%% Layout: length(4) + terminator(1) = 5 bytes
empty_doc() ->
    <<5:32/little, 0>>.

%% Simple document: {"a": 1} where 1 is int32
%% Layout: length(4) + type(1) + key"a"(2) + int32(4) + terminator(1) = 12 bytes
simple_int32_doc() ->
    <<12:32/little,          %% Document length
      16#10,                 %% Type: int32
      "a", 0,                %% Key: "a\0"
      1:32/little-signed,    %% Value: 1
      0>>.                   %% Terminator

%% Document with multiple fields: {"a": 1, "b": 2.5}
%% Layout: length(4) + [type(1) + key(2) + int32(4)] + [type(1) + key(2) + double(8)] + term(1) = 23
multi_field_doc() ->
    <<23:32/little,          %% Document length
      16#10,                 %% Type: int32
      "a", 0,                %% Key: "a\0"
      1:32/little-signed,    %% Value: 1
      16#01,                 %% Type: double
      "b", 0,                %% Key: "b\0"
      2.5:64/little-float,   %% Value: 2.5
      0>>.                   %% Terminator

%% Document with string: {"name": "test"}
%% String format: int32_length + data + null (length includes null)
string_doc() ->
    <<20:32/little,          %% Document length
      16#02,                 %% Type: string
      "name", 0,             %% Key: "name\0"
      5:32/little,           %% String length (4 chars + null)
      "test", 0,             %% String value with null
      0>>.                   %% Terminator

%% Document with nested doc: {"outer": {"inner": 42}}
%% Inner: length(4) + type(1) + "inner\0"(6) + int32(4) + term(1) = 16 bytes
nested_doc() ->
    InnerDoc = <<16:32/little, 16#10, "inner", 0, 42:32/little-signed, 0>>,
    InnerLen = byte_size(InnerDoc),  %% 16 bytes
    OuterLen = 4 + 1 + 6 + InnerLen + 1, %% length + type + "outer\0" + inner + term = 28
    <<OuterLen:32/little,
      16#03,                 %% Type: document
      "outer", 0,            %% Key: "outer\0"
      InnerDoc/binary,       %% Nested document
      0>>.                   %% Terminator

%% Document with boolean: {"flag": true}
%% Layout: length(4) + type(1) + "flag\0"(5) + boolean(1) + term(1) = 12
boolean_doc() ->
    <<12:32/little,          %% Document length
      16#08,                 %% Type: boolean
      "flag", 0,             %% Key: "flag\0"
      1,                     %% Value: true (0x01)
      0>>.                   %% Terminator

%% Document with null: {"nothing": null}
%% Layout: length(4) + type(1) + "nothing\0"(8) + null(0) + term(1) = 14
null_doc() ->
    <<14:32/little,          %% Document length
      16#0A,                 %% Type: null
      "nothing", 0,          %% Key: "nothing\0"
      0>>.                   %% Terminator (null has no value bytes)

%% Document with objectid: {"_id": ObjectId(12 bytes)}
objectid_doc() ->
    <<22:32/little,          %% Document length
      16#07,                 %% Type: objectid
      "_id", 0,              %% Key: "_id\0"
      1,2,3,4,5,6,7,8,9,10,11,12, %% 12 byte ObjectId
      0>>.                   %% Terminator

%% Document with datetime: {"ts": datetime}
%% Layout: length(4) + type(1) + "ts\0"(3) + int64(8) + term(1) = 17
datetime_doc() ->
    <<17:32/little,          %% Document length
      16#09,                 %% Type: datetime
      "ts", 0,               %% Key: "ts\0"
      1704067200000:64/little-signed, %% Unix timestamp ms (2024-01-01)
      0>>.                   %% Terminator

%% Document with int64: {"big": 9223372036854775807}
%% Layout: length(4) + type(1) + "big\0"(4) + int64(8) + term(1) = 18
int64_doc() ->
    <<18:32/little,          %% Document length
      16#12,                 %% Type: int64
      "big", 0,              %% Key: "big\0"
      9223372036854775807:64/little-signed, %% Max int64
      0>>.                   %% Terminator

%% Document with binary data: {"data": Binary(generic, <<1,2,3>>)}
%% Layout: length(4) + type(1) + "data\0"(5) + binlen(4) + subtype(1) + data(3) + term(1) = 19
binary_doc() ->
    <<19:32/little,          %% Document length
      16#05,                 %% Type: binary
      "data", 0,             %% Key: "data\0"
      3:32/little,           %% Binary length
      0,                     %% Subtype: generic
      1, 2, 3,               %% Binary data
      0>>.                   %% Terminator

%% Document with array: {"arr": [1, 2, 3]}
%% Arrays are documents with "0", "1", "2" as keys
array_doc() ->
    ArrayContent = <<
        16#10, "0", 0, 1:32/little-signed,
        16#10, "1", 0, 2:32/little-signed,
        16#10, "2", 0, 3:32/little-signed
    >>,
    ArrayLen = 4 + byte_size(ArrayContent) + 1,
    DocLen = 4 + 1 + 4 + ArrayLen + 1,
    <<DocLen:32/little,
      16#04,                 %% Type: array
      "arr", 0,              %% Key: "arr\0"
      ArrayLen:32/little,    %% Array document length
      ArrayContent/binary,
      0,                     %% Array terminator
      0>>.                   %% Document terminator

%% =============================================================================
%% new/1 Tests
%% =============================================================================

new_empty_doc_test() ->
    {ok, _Iter} = ebson_iter:new(empty_doc()).

new_simple_doc_test() ->
    {ok, _Iter} = ebson_iter:new(simple_int32_doc()).

new_invalid_too_small_test() ->
    {error, {invalid_length, 5, 4}} = ebson_iter:new(<<5:32/little>>).

new_invalid_length_mismatch_test() ->
    %% Declared length 10, actual size 5
    {error, {invalid_length, 10, 5}} = ebson_iter:new(<<10:32/little, 0>>).

new_missing_terminator_test() ->
    %% Valid length but no null terminator
    {error, {missing_terminator, 4}} = ebson_iter:new(<<5:32/little, 1>>).

new_not_binary_test() ->
    {error, not_a_binary} = ebson_iter:new("not a binary").

%% =============================================================================
%% next/1 Tests - Basic Iteration
%% =============================================================================

next_empty_doc_test() ->
    {ok, Iter} = ebson_iter:new(empty_doc()),
    done = ebson_iter:next(Iter).

next_single_int32_test() ->
    {ok, Iter} = ebson_iter:new(simple_int32_doc()),
    {ok, <<"a">>, int32, #{off := Off, len := 4}, Iter2} = ebson_iter:next(Iter),
    ?assert(Off > 0),
    done = ebson_iter:next(Iter2).

next_multiple_fields_test() ->
    {ok, Iter} = ebson_iter:new(multi_field_doc()),
    {ok, <<"a">>, int32, _, Iter2} = ebson_iter:next(Iter),
    {ok, <<"b">>, double, #{len := 8}, Iter3} = ebson_iter:next(Iter2),
    done = ebson_iter:next(Iter3).

next_string_test() ->
    {ok, Iter} = ebson_iter:new(string_doc()),
    {ok, <<"name">>, string, #{len := 9}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_nested_doc_test() ->
    {ok, Iter} = ebson_iter:new(nested_doc()),
    {ok, <<"outer">>, document, #{len := 16}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_boolean_test() ->
    {ok, Iter} = ebson_iter:new(boolean_doc()),
    {ok, <<"flag">>, boolean, #{len := 1}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_null_test() ->
    {ok, Iter} = ebson_iter:new(null_doc()),
    {ok, <<"nothing">>, null, #{len := 0}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_objectid_test() ->
    {ok, Iter} = ebson_iter:new(objectid_doc()),
    {ok, <<"_id">>, objectid, #{len := 12}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_datetime_test() ->
    {ok, Iter} = ebson_iter:new(datetime_doc()),
    {ok, <<"ts">>, datetime, #{len := 8}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_int64_test() ->
    {ok, Iter} = ebson_iter:new(int64_doc()),
    {ok, <<"big">>, int64, #{len := 8}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_binary_test() ->
    {ok, Iter} = ebson_iter:new(binary_doc()),
    {ok, <<"data">>, binary, #{len := 8}, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

next_array_test() ->
    {ok, Iter} = ebson_iter:new(array_doc()),
    {ok, <<"arr">>, array, _, Iter2} = ebson_iter:next(Iter),
    done = ebson_iter:next(Iter2).

%% =============================================================================
%% Iteration - Complete Traversal
%% =============================================================================

iterate_all_test() ->
    {ok, Iter} = ebson_iter:new(multi_field_doc()),
    Keys = iterate_keys(Iter, []),
    ?assertEqual([<<"a">>, <<"b">>], Keys).

iterate_keys(Iter, Acc) ->
    case ebson_iter:next(Iter) of
        {ok, Key, _Type, _Ref, NextIter} ->
            iterate_keys(NextIter, Acc ++ [Key]);
        done ->
            Acc
    end.

%% =============================================================================
%% Error Cases
%% =============================================================================

truncated_string_test() ->
    %% String claims length 100 but document is too short
    %% length(4) + type(1) + "x\0"(2) + str_len(4) + "short"(5) + term(1) = 17
    Bin = <<17:32/little, 16#02, "x", 0, 100:32/little, "short", 0>>,
    {ok, Iter} = ebson_iter:new(Bin),
    {error, {truncated_value, string, _}} = ebson_iter:next(Iter).

invalid_string_length_test() ->
    %% String with length 0 (invalid, must be at least 1 for null)
    %% length(4) + type(1) + "x\0"(2) + str_len(4) + term(1) = 12
    Bin = <<12:32/little, 16#02, "x", 0, 0:32/little, 0>>,
    {ok, Iter} = ebson_iter:new(Bin),
    {error, {invalid_string_length, 0, _}} = ebson_iter:next(Iter).

unsupported_type_test() ->
    %% Type 0x06 (undefined) is deprecated/unsupported
    Bin = <<8:32/little, 16#06, "x", 0, 0>>,
    {ok, Iter} = ebson_iter:new(Bin),
    {error, {unsupported_type, 16#06}} = ebson_iter:next(Iter).

%% =============================================================================
%% peek/2 Tests
%% =============================================================================

peek_existing_key_test() ->
    Bin = multi_field_doc(),
    {ok, int32, #{len := 4}} = ebson_iter:peek(Bin, <<"a">>),
    {ok, double, #{len := 8}} = ebson_iter:peek(Bin, <<"b">>).

peek_missing_key_test() ->
    Bin = simple_int32_doc(),
    not_found = ebson_iter:peek(Bin, <<"nonexistent">>).

peek_empty_doc_test() ->
    Bin = empty_doc(),
    not_found = ebson_iter:peek(Bin, <<"anything">>).

peek_invalid_doc_test() ->
    {error, {invalid_length, _, _}} = ebson_iter:peek(<<1,2,3>>, <<"key">>).

%% =============================================================================
%% find_path/2 Tests
%% =============================================================================

find_path_single_level_test() ->
    Bin = simple_int32_doc(),
    {ok, int32, #{len := 4}} = ebson_iter:find_path(Bin, [<<"a">>]).

find_path_nested_test() ->
    Bin = nested_doc(),
    %% {"outer": {"inner": 42}}
    {ok, int32, #{len := 4}} = ebson_iter:find_path(Bin, [<<"outer">>, <<"inner">>]).

find_path_not_found_at_first_level_test() ->
    Bin = nested_doc(),
    not_found = ebson_iter:find_path(Bin, [<<"missing">>]).

find_path_not_found_at_second_level_test() ->
    Bin = nested_doc(),
    not_found = ebson_iter:find_path(Bin, [<<"outer">>, <<"missing">>]).

find_path_into_non_document_test() ->
    %% Trying to navigate into an int32
    Bin = simple_int32_doc(),
    not_found = ebson_iter:find_path(Bin, [<<"a">>, <<"deeper">>]).

find_path_empty_path_test() ->
    Bin = simple_int32_doc(),
    {ok, document, #{off := 0}} = ebson_iter:find_path(Bin, []).

find_path_into_array_test() ->
    Bin = array_doc(),
    %% {"arr": [1, 2, 3]} - array elements have keys "0", "1", "2"
    {ok, int32, _} = ebson_iter:find_path(Bin, [<<"arr">>, <<"1">>]).

find_path_deeply_nested_test() ->
    %% Create a 3-level nested document: {"a": {"b": {"c": 99}}}
    InnerMost = <<12:32/little, 16#10, "c", 0, 99:32/little-signed, 0>>,
    MiddleContent = <<16#03, "b", 0, InnerMost/binary>>,
    MiddleLen = 4 + byte_size(MiddleContent) + 1,
    Middle = <<MiddleLen:32/little, MiddleContent/binary, 0>>,
    OuterContent = <<16#03, "a", 0, Middle/binary>>,
    OuterLen = 4 + byte_size(OuterContent) + 1,
    Outer = <<OuterLen:32/little, OuterContent/binary, 0>>,

    {ok, int32, _} = ebson_iter:find_path(Outer, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, document, _} = ebson_iter:find_path(Outer, [<<"a">>, <<"b">>]),
    {ok, document, _} = ebson_iter:find_path(Outer, [<<"a">>]),
    not_found = ebson_iter:find_path(Outer, [<<"a">>, <<"b">>, <<"d">>]).

%% =============================================================================
%% decode_value/2 Tests
%% =============================================================================

decode_double_test() ->
    Bin = multi_field_doc(),
    {ok, double, ValueRef} = ebson_iter:peek(Bin, <<"b">>),
    {ok, 2.5} = ebson_iter:decode_value(double, ValueRef).

decode_int32_test() ->
    Bin = simple_int32_doc(),
    {ok, int32, ValueRef} = ebson_iter:peek(Bin, <<"a">>),
    {ok, 1} = ebson_iter:decode_value(int32, ValueRef).

decode_string_test() ->
    Bin = string_doc(),
    {ok, string, ValueRef} = ebson_iter:peek(Bin, <<"name">>),
    {ok, <<"test">>} = ebson_iter:decode_value(string, ValueRef).

decode_boolean_true_test() ->
    Bin = boolean_doc(),
    {ok, boolean, ValueRef} = ebson_iter:peek(Bin, <<"flag">>),
    {ok, true} = ebson_iter:decode_value(boolean, ValueRef).

decode_boolean_false_test() ->
    %% Create a doc with false: {"flag": false}
    Bin = <<12:32/little, 16#08, "flag", 0, 0, 0>>,
    {ok, boolean, ValueRef} = ebson_iter:peek(Bin, <<"flag">>),
    {ok, false} = ebson_iter:decode_value(boolean, ValueRef).

decode_null_test() ->
    Bin = null_doc(),
    {ok, null, ValueRef} = ebson_iter:peek(Bin, <<"nothing">>),
    {ok, null} = ebson_iter:decode_value(null, ValueRef).

decode_objectid_test() ->
    Bin = objectid_doc(),
    {ok, objectid, ValueRef} = ebson_iter:peek(Bin, <<"_id">>),
    {ok, {objectid, OidBin}} = ebson_iter:decode_value(objectid, ValueRef),
    ?assertEqual(<<1,2,3,4,5,6,7,8,9,10,11,12>>, OidBin).

decode_datetime_test() ->
    Bin = datetime_doc(),
    {ok, datetime, ValueRef} = ebson_iter:peek(Bin, <<"ts">>),
    {ok, {datetime_ms, Ms}} = ebson_iter:decode_value(datetime, ValueRef),
    ?assertEqual(1704067200000, Ms).

decode_int64_test() ->
    Bin = int64_doc(),
    {ok, int64, ValueRef} = ebson_iter:peek(Bin, <<"big">>),
    {ok, 9223372036854775807} = ebson_iter:decode_value(int64, ValueRef).

decode_binary_test() ->
    Bin = binary_doc(),
    {ok, binary, ValueRef} = ebson_iter:peek(Bin, <<"data">>),
    {ok, {binary, 0, <<1,2,3>>}} = ebson_iter:decode_value(binary, ValueRef).

decode_document_test() ->
    Bin = nested_doc(),
    {ok, document, ValueRef} = ebson_iter:peek(Bin, <<"outer">>),
    {ok, {document, DocBin}} = ebson_iter:decode_value(document, ValueRef),
    %% Verify the extracted document is valid
    {ok, _} = ebson_iter:new(DocBin).

decode_array_test() ->
    Bin = array_doc(),
    {ok, array, ValueRef} = ebson_iter:peek(Bin, <<"arr">>),
    {ok, {array, ArrBin}} = ebson_iter:decode_value(array, ValueRef),
    %% Verify the extracted array is a valid document
    {ok, _} = ebson_iter:new(ArrBin).

decode_timestamp_test() ->
    %% Create a timestamp doc: {"ts": timestamp(100, 1234567890)}
    %% length(4) + type(1) + "ts\0"(3) + timestamp(8) + term(1) = 17 bytes
    Bin = <<17:32/little, 16#11, "ts", 0, 100:32/little-unsigned, 1234567890:32/little-unsigned, 0>>,
    {ok, timestamp, ValueRef} = ebson_iter:peek(Bin, <<"ts">>),
    {ok, {timestamp, 100, 1234567890}} = ebson_iter:decode_value(timestamp, ValueRef).

%% Test that decoded binaries are independent copies
decode_binary_copy_test() ->
    Bin = string_doc(),
    {ok, string, ValueRef} = ebson_iter:peek(Bin, <<"name">>),
    {ok, StrBin} = ebson_iter:decode_value(string, ValueRef),
    %% Verify string is correct
    ?assertEqual(<<"test">>, StrBin),
    %% Check that it's a separate binary (by checking reference)
    %% binary:referenced_byte_size returns size of underlying binary
    %% For a copy, this should equal the actual size
    RefSize = binary:referenced_byte_size(StrBin),
    ?assertEqual(4, RefSize).

decode_minkey_test() ->
    %% Create a minkey doc: {"k": minkey}
    Bin = <<8:32/little, 16#FF, "k", 0, 0>>,
    {ok, minkey, ValueRef} = ebson_iter:peek(Bin, <<"k">>),
    {ok, minkey} = ebson_iter:decode_value(minkey, ValueRef).

decode_maxkey_test() ->
    %% Create a maxkey doc: {"k": maxkey}
    Bin = <<8:32/little, 16#7F, "k", 0, 0>>,
    {ok, maxkey, ValueRef} = ebson_iter:peek(Bin, <<"k">>),
    {ok, maxkey} = ebson_iter:decode_value(maxkey, ValueRef).
