-module(ebson_tests).

-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% encode_map/1 Tests - Basic Types
%% =============================================================================

encode_empty_map_test() ->
    {ok, Bin} = ebson:encode_map(#{}),
    %% Empty doc: length(4) + terminator(1) = 5 bytes
    ?assertEqual(5, byte_size(Bin)),
    {ok, _} = ebson_iter:new(Bin).

encode_int32_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"a">> => 42}),
    {ok, int32, ValueRef} = ebson_iter:peek(Bin, <<"a">>),
    {ok, 42} = ebson_iter:decode_value(int32, ValueRef).

encode_int32_negative_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"n">> => -100}),
    {ok, int32, ValueRef} = ebson_iter:peek(Bin, <<"n">>),
    {ok, -100} = ebson_iter:decode_value(int32, ValueRef).

encode_int64_test() ->
    BigInt = 9223372036854775807,  %% Max int64
    {ok, Bin} = ebson:encode_map(#{<<"big">> => BigInt}),
    {ok, int64, ValueRef} = ebson_iter:peek(Bin, <<"big">>),
    {ok, BigInt} = ebson_iter:decode_value(int64, ValueRef).

encode_int64_negative_test() ->
    BigNeg = -9223372036854775808,  %% Min int64
    {ok, Bin} = ebson:encode_map(#{<<"neg">> => BigNeg}),
    {ok, int64, ValueRef} = ebson_iter:peek(Bin, <<"neg">>),
    {ok, BigNeg} = ebson_iter:decode_value(int64, ValueRef).

encode_double_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"pi">> => 3.14159}),
    {ok, double, ValueRef} = ebson_iter:peek(Bin, <<"pi">>),
    {ok, Value} = ebson_iter:decode_value(double, ValueRef),
    ?assert(abs(Value - 3.14159) < 0.00001).

encode_string_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"name">> => <<"hello">>}),
    {ok, string, ValueRef} = ebson_iter:peek(Bin, <<"name">>),
    {ok, <<"hello">>} = ebson_iter:decode_value(string, ValueRef).

encode_boolean_true_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"flag">> => true}),
    {ok, boolean, ValueRef} = ebson_iter:peek(Bin, <<"flag">>),
    {ok, true} = ebson_iter:decode_value(boolean, ValueRef).

encode_boolean_false_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"flag">> => false}),
    {ok, boolean, ValueRef} = ebson_iter:peek(Bin, <<"flag">>),
    {ok, false} = ebson_iter:decode_value(boolean, ValueRef).

encode_null_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"nothing">> => null}),
    {ok, null, ValueRef} = ebson_iter:peek(Bin, <<"nothing">>),
    {ok, null} = ebson_iter:decode_value(null, ValueRef).

%% =============================================================================
%% encode_map/1 Tests - Special Types
%% =============================================================================

encode_objectid_test() ->
    Oid = <<1,2,3,4,5,6,7,8,9,10,11,12>>,
    {ok, Bin} = ebson:encode_map(#{<<"_id">> => {objectid, Oid}}),
    {ok, objectid, ValueRef} = ebson_iter:peek(Bin, <<"_id">>),
    {ok, {objectid, Oid}} = ebson_iter:decode_value(objectid, ValueRef).

encode_datetime_test() ->
    Ms = 1704067200000,  %% 2024-01-01
    {ok, Bin} = ebson:encode_map(#{<<"ts">> => {datetime_ms, Ms}}),
    {ok, datetime, ValueRef} = ebson_iter:peek(Bin, <<"ts">>),
    {ok, {datetime_ms, Ms}} = ebson_iter:decode_value(datetime, ValueRef).

encode_binary_test() ->
    Data = <<1, 2, 3, 4, 5>>,
    {ok, Bin} = ebson:encode_map(#{<<"data">> => {binary, 0, Data}}),
    {ok, binary, ValueRef} = ebson_iter:peek(Bin, <<"data">>),
    {ok, {binary, 0, Data}} = ebson_iter:decode_value(binary, ValueRef).

encode_timestamp_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"ts">> => {timestamp, 100, 1234567890}}),
    {ok, timestamp, ValueRef} = ebson_iter:peek(Bin, <<"ts">>),
    {ok, {timestamp, 100, 1234567890}} = ebson_iter:decode_value(timestamp, ValueRef).

encode_regex_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"r">> => {regex, <<"^test.*">>, <<"im">>}}),
    {ok, regex, ValueRef} = ebson_iter:peek(Bin, <<"r">>),
    {ok, {regex, <<"^test.*">>, <<"im">>}} = ebson_iter:decode_value(regex, ValueRef).

encode_minkey_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"k">> => minkey}),
    {ok, minkey, ValueRef} = ebson_iter:peek(Bin, <<"k">>),
    {ok, minkey} = ebson_iter:decode_value(minkey, ValueRef).

encode_maxkey_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"k">> => maxkey}),
    {ok, maxkey, ValueRef} = ebson_iter:peek(Bin, <<"k">>),
    {ok, maxkey} = ebson_iter:decode_value(maxkey, ValueRef).

%% =============================================================================
%% encode_map/1 Tests - Nested Structures
%% =============================================================================

encode_nested_map_test() ->
    Map = #{<<"outer">> => #{<<"inner">> => 42}},
    {ok, Bin} = ebson:encode_map(Map),
    {ok, int32, ValueRef} = ebson_iter:find_path(Bin, [<<"outer">>, <<"inner">>]),
    {ok, 42} = ebson_iter:decode_value(int32, ValueRef).

encode_array_test() ->
    Map = #{<<"arr">> => [1, 2, 3]},
    {ok, Bin} = ebson:encode_map(Map),
    {ok, array, _} = ebson_iter:peek(Bin, <<"arr">>),
    %% Verify array elements
    {ok, int32, V0} = ebson_iter:find_path(Bin, [<<"arr">>, <<"0">>]),
    {ok, 1} = ebson_iter:decode_value(int32, V0),
    {ok, int32, V1} = ebson_iter:find_path(Bin, [<<"arr">>, <<"1">>]),
    {ok, 2} = ebson_iter:decode_value(int32, V1),
    {ok, int32, V2} = ebson_iter:find_path(Bin, [<<"arr">>, <<"2">>]),
    {ok, 3} = ebson_iter:decode_value(int32, V2).

encode_mixed_array_test() ->
    Map = #{<<"arr">> => [1, <<"hello">>, true, null]},
    {ok, Bin} = ebson:encode_map(Map),
    {ok, int32, V0} = ebson_iter:find_path(Bin, [<<"arr">>, <<"0">>]),
    {ok, 1} = ebson_iter:decode_value(int32, V0),
    {ok, string, V1} = ebson_iter:find_path(Bin, [<<"arr">>, <<"1">>]),
    {ok, <<"hello">>} = ebson_iter:decode_value(string, V1),
    {ok, boolean, V2} = ebson_iter:find_path(Bin, [<<"arr">>, <<"2">>]),
    {ok, true} = ebson_iter:decode_value(boolean, V2),
    {ok, null, V3} = ebson_iter:find_path(Bin, [<<"arr">>, <<"3">>]),
    {ok, null} = ebson_iter:decode_value(null, V3).

encode_nested_array_test() ->
    Map = #{<<"arr">> => [[1, 2], [3, 4]]},
    {ok, Bin} = ebson:encode_map(Map),
    {ok, int32, V} = ebson_iter:find_path(Bin, [<<"arr">>, <<"0">>, <<"1">>]),
    {ok, 2} = ebson_iter:decode_value(int32, V).

encode_deeply_nested_test() ->
    Map = #{<<"a">> => #{<<"b">> => #{<<"c">> => #{<<"d">> => 99}}}},
    {ok, Bin} = ebson:encode_map(Map),
    {ok, int32, V} = ebson_iter:find_path(Bin, [<<"a">>, <<"b">>, <<"c">>, <<"d">>]),
    {ok, 99} = ebson_iter:decode_value(int32, V).

%% =============================================================================
%% encode_map/1 Tests - Multiple Fields
%% =============================================================================

encode_multiple_fields_test() ->
    Map = #{
        <<"a">> => 1,
        <<"b">> => <<"hello">>,
        <<"c">> => true
    },
    {ok, Bin} = ebson:encode_map(Map),
    {ok, int32, _} = ebson_iter:peek(Bin, <<"a">>),
    {ok, string, _} = ebson_iter:peek(Bin, <<"b">>),
    {ok, boolean, _} = ebson_iter:peek(Bin, <<"c">>).

%% =============================================================================
%% encode_map/1 Tests - Error Cases
%% =============================================================================

encode_non_binary_key_test() ->
    {error, {invalid_key, _, expected_binary}} = ebson:encode_map(#{foo => 1}).

encode_non_map_test() ->
    {error, not_a_map} = ebson:encode_map([1, 2, 3]).

encode_unsupported_value_test() ->
    {error, {unsupported_value, _}} = ebson:encode_map(#{<<"x">> => {unsupported}}).

encode_integer_out_of_range_test() ->
    TooBig = 9223372036854775808,  %% Max int64 + 1
    {error, {integer_out_of_range, TooBig}} = ebson:encode_map(#{<<"n">> => TooBig}).

%% =============================================================================
%% encode_map/1 Tests - Decimal128
%% =============================================================================

encode_decimal128_zero_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"d">> => {decimal128, 0, 0}}),
    {ok, decimal128, ValueRef} = ebson_iter:peek(Bin, <<"d">>),
    {ok, {decimal128, 0, 0}} = ebson_iter:decode_value(decimal128, ValueRef).

encode_decimal128_positive_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"d">> => {decimal128, 12345, -2}}),
    {ok, decimal128, ValueRef} = ebson_iter:peek(Bin, <<"d">>),
    {ok, {decimal128, 12345, -2}} = ebson_iter:decode_value(decimal128, ValueRef).

encode_decimal128_negative_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"d">> => {decimal128, -12345, 0}}),
    {ok, decimal128, ValueRef} = ebson_iter:peek(Bin, <<"d">>),
    {ok, {decimal128, -12345, 0}} = ebson_iter:decode_value(decimal128, ValueRef).

encode_decimal128_infinity_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"d">> => {decimal128, infinity, 0}}),
    {ok, decimal128, ValueRef} = ebson_iter:peek(Bin, <<"d">>),
    {ok, {decimal128, infinity, 0}} = ebson_iter:decode_value(decimal128, ValueRef).

encode_decimal128_neg_infinity_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"d">> => {decimal128, neg_infinity, 0}}),
    {ok, decimal128, ValueRef} = ebson_iter:peek(Bin, <<"d">>),
    {ok, {decimal128, neg_infinity, 0}} = ebson_iter:decode_value(decimal128, ValueRef).

encode_decimal128_nan_test() ->
    {ok, Bin} = ebson:encode_map(#{<<"d">> => {decimal128, nan, 0}}),
    {ok, decimal128, ValueRef} = ebson_iter:peek(Bin, <<"d">>),
    {ok, {decimal128, nan, 0}} = ebson_iter:decode_value(decimal128, ValueRef).

%% =============================================================================
%% decode_map/1 Tests - Roundtrip
%% =============================================================================

decode_empty_map_test() ->
    {ok, Bin} = ebson:encode_map(#{}),
    {ok, #{}} = ebson:decode_map(Bin).

decode_simple_int_test() ->
    Original = #{<<"a">> => 42},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_multiple_fields_test() ->
    Original = #{
        <<"a">> => 1,
        <<"b">> => <<"hello">>,
        <<"c">> => true
    },
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_nested_map_test() ->
    Original = #{<<"outer">> => #{<<"inner">> => 42}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_array_test() ->
    Original = #{<<"arr">> => [1, 2, 3]},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_mixed_array_test() ->
    Original = #{<<"arr">> => [1, <<"hello">>, true, null]},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_nested_array_test() ->
    Original = #{<<"arr">> => [[1, 2], [3, 4]]},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_deeply_nested_test() ->
    Original = #{<<"a">> => #{<<"b">> => #{<<"c">> => 99}}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

%% =============================================================================
%% decode_map/1 Tests - Special Types
%% =============================================================================

decode_objectid_test() ->
    Oid = <<1,2,3,4,5,6,7,8,9,10,11,12>>,
    Original = #{<<"_id">> => {objectid, Oid}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_datetime_test() ->
    Original = #{<<"ts">> => {datetime_ms, 1704067200000}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_binary_data_test() ->
    Original = #{<<"data">> => {binary, 0, <<1,2,3>>}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_timestamp_test() ->
    Original = #{<<"ts">> => {timestamp, 100, 1234567890}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_regex_test() ->
    Original = #{<<"r">> => {regex, <<"^test.*">>, <<"im">>}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_minmax_key_test() ->
    Original = #{<<"min">> => minkey, <<"max">> => maxkey},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

decode_decimal128_test() ->
    Original = #{<<"d">> => {decimal128, 12345, -2}},
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Original} = ebson:decode_map(Bin).

%% =============================================================================
%% decode_map/1 Tests - Error Cases
%% =============================================================================

decode_non_binary_test() ->
    {error, not_a_binary} = ebson:decode_map([1, 2, 3]).

decode_malformed_test() ->
    %% Invalid length
    {error, {invalid_length, _, _}} = ebson:decode_map(<<1, 2, 3>>).

%% =============================================================================
%% Complex Roundtrip Tests
%% =============================================================================

complex_document_roundtrip_test() ->
    Original = #{
        <<"_id">> => {objectid, <<1,2,3,4,5,6,7,8,9,10,11,12>>},
        <<"name">> => <<"Test Document">>,
        <<"count">> => 42,
        <<"active">> => true,
        <<"data">> => null,
        <<"tags">> => [<<"a">>, <<"b">>, <<"c">>],
        <<"nested">> => #{
            <<"x">> => 1,
            <<"y">> => 2,
            <<"z">> => 3
        },
        <<"created">> => {datetime_ms, 1704067200000},
        <<"blob">> => {binary, 0, <<255, 0, 128>>}
    },
    {ok, Bin} = ebson:encode_map(Original),
    {ok, Decoded} = ebson:decode_map(Bin),
    ?assertEqual(Original, Decoded).
