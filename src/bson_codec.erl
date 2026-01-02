%% @doc BSON map encoder/decoder.
%%
%% This module provides convenience functions for encoding Erlang maps to BSON
%% and decoding BSON to Erlang maps. It is built on top of bson_iter for parsing.
%%
%% This is NOT the hot path module - it fully decodes/encodes documents and is
%% suitable for ingest, admin tools, and testing. For query hot paths, use
%% `bson_iter' directly for zero-copy traversal.
%%
%% == Type Mappings ==
%%
%% ```
%% Erlang Value                    | BSON Type
%% --------------------------------|------------------
%% integer (fits in 32 bits)       | int32
%% integer (requires 64 bits)      | int64
%% float                           | double
%% binary                          | string (UTF-8)
%% true | false                    | boolean
%% null                            | null
%% map                             | document
%% list                            | array
%% {objectid, <<12 bytes>>}        | objectid
%% {datetime_ms, MillisInt}        | datetime
%% {binary, Subtype, Data}         | binary
%% {timestamp, Increment, Time}    | timestamp
%% {decimal128, Coeff, Exp}        | decimal128
%% {decimal128, infinity, _}       | decimal128 +Inf
%% {decimal128, neg_infinity, _}   | decimal128 -Inf
%% {decimal128, nan, _}            | decimal128 NaN
%% {regex, Pattern, Options}       | regex
%% minkey                          | minkey
%% maxkey                          | maxkey
%% '''
%%
%% == Requirements ==
%%
%% <ul>
%%   <li>Map keys must be binaries</li>
%%   <li>Integers must fit in 64-bit signed range</li>
%%   <li>ObjectId must be exactly 12 bytes</li>
%% </ul>
%%
%% == Memory Safety ==
%%
%% All decoded values are copied using `binary:copy/1' to break references
%% to the source binary. This ensures decoded maps don't retain large
%% source documents in memory.
%%
%% == Example Usage ==
%%
%% ```
%% %% Encode a map to BSON
%% Map = #{
%%     <<"_id">> => {objectid, <<1,2,3,4,5,6,7,8,9,10,11,12>>},
%%     <<"name">> => <<"Test">>,
%%     <<"count">> => 42,
%%     <<"tags">> => [<<"a">>, <<"b">>]
%% },
%% {ok, BsonBin} = bson_codec:encode_map(Map).
%%
%% %% Decode BSON to a map
%% {ok, DecodedMap} = bson_codec:decode_map(BsonBin).
%% '''
%%
%% @end
-module(bson_codec).

-include("bson_types.hrl").

%% API exports
-export([
    encode_map/1,
    decode_map/1
]).

%% =============================================================================
%% API Functions
%% =============================================================================

%% @doc Encode an Erlang map to a BSON binary document.
-spec encode_map(map()) -> {ok, binary()} | {error, term()}.
encode_map(Map) when is_map(Map) ->
    try
        Elements = encode_elements(maps:to_list(Map), []),
        ElementsBin = iolist_to_binary(Elements),
        DocLen = byte_size(ElementsBin) + 5,  %% 4 for length + 1 for terminator
        {ok, <<DocLen:32/little, ElementsBin/binary, 0>>}
    catch
        throw:{encode_error, Reason} ->
            {error, Reason}
    end;
encode_map(_) ->
    {error, not_a_map}.

%% @doc Decode a BSON binary document to an Erlang map.
%% All values are fully decoded and binaries are copied to break reference chains.
-spec decode_map(binary()) -> {ok, map()} | {error, term()}.
decode_map(Bin) when is_binary(Bin) ->
    case bson_iter:new(Bin) of
        {ok, Iter} ->
            decode_document(Iter);
        {error, _} = Err ->
            Err
    end;
decode_map(_) ->
    {error, not_a_binary}.

%% =============================================================================
%% Internal Functions - Decoding
%% =============================================================================

decode_document(Iter) ->
    decode_elements(Iter, #{}).

decode_elements(Iter, Acc) ->
    case bson_iter:next(Iter) of
        {ok, Key, Type, ValueRef, NextIter} ->
            case decode_field_value(Type, ValueRef) of
                {ok, Value} ->
                    decode_elements(NextIter, Acc#{Key => Value});
                {error, _} = Err ->
                    Err
            end;
        done ->
            {ok, Acc};
        {error, _} = Err ->
            Err
    end.

decode_field_value(document, #{bin := Bin, off := Off, len := Len}) ->
    DocBin = binary:part(Bin, Off, Len),
    decode_map(DocBin);
decode_field_value(array, #{bin := Bin, off := Off, len := Len}) ->
    ArrayBin = binary:part(Bin, Off, Len),
    decode_array(ArrayBin);
decode_field_value(Type, ValueRef) ->
    bson_iter:decode_value(Type, ValueRef).

decode_array(Bin) ->
    case bson_iter:new(Bin) of
        {ok, Iter} ->
            decode_array_elements(Iter, []);
        {error, _} = Err ->
            Err
    end.

decode_array_elements(Iter, Acc) ->
    case bson_iter:next(Iter) of
        {ok, _Key, Type, ValueRef, NextIter} ->
            case decode_field_value(Type, ValueRef) of
                {ok, Value} ->
                    decode_array_elements(NextIter, [Value | Acc]);
                {error, _} = Err ->
                    Err
            end;
        done ->
            {ok, lists:reverse(Acc)};
        {error, _} = Err ->
            Err
    end.

%% =============================================================================
%% Internal Functions - Encoding
%% =============================================================================

encode_elements([], Acc) ->
    lists:reverse(Acc);
encode_elements([{Key, Value} | Rest], Acc) when is_binary(Key) ->
    Element = encode_element(Key, Value),
    encode_elements(Rest, [Element | Acc]);
encode_elements([{Key, _} | _], _) ->
    throw({encode_error, {invalid_key, Key, expected_binary}}).

encode_element(Key, Value) ->
    {TypeByte, ValueBin} = encode_value(Value),
    [TypeByte, Key, 0, ValueBin].

encode_value(Value) when is_integer(Value) ->
    encode_integer(Value);
encode_value(Value) when is_float(Value) ->
    {?BSON_TYPE_DOUBLE, <<Value:64/little-float>>};
encode_value(Value) when is_binary(Value) ->
    %% Encode as UTF-8 string
    StrLen = byte_size(Value) + 1,  %% +1 for null terminator
    {?BSON_TYPE_STRING, <<StrLen:32/little, Value/binary, 0>>};
encode_value(true) ->
    {?BSON_TYPE_BOOLEAN, <<1>>};
encode_value(false) ->
    {?BSON_TYPE_BOOLEAN, <<0>>};
encode_value(null) ->
    {?BSON_TYPE_NULL, <<>>};
encode_value(Map) when is_map(Map) ->
    case encode_map(Map) of
        {ok, DocBin} ->
            {?BSON_TYPE_DOCUMENT, DocBin};
        {error, Reason} ->
            throw({encode_error, Reason})
    end;
encode_value(List) when is_list(List) ->
    encode_array(List);
encode_value({objectid, OidBin}) when is_binary(OidBin), byte_size(OidBin) =:= 12 ->
    {?BSON_TYPE_OBJECTID, OidBin};
encode_value({datetime_ms, Ms}) when is_integer(Ms) ->
    {?BSON_TYPE_DATETIME, <<Ms:64/little-signed>>};
encode_value({binary, Subtype, Data}) when is_integer(Subtype), is_binary(Data) ->
    DataLen = byte_size(Data),
    {?BSON_TYPE_BINARY, <<DataLen:32/little, Subtype:8, Data/binary>>};
encode_value({timestamp, Increment, Timestamp})
  when is_integer(Increment), is_integer(Timestamp) ->
    {?BSON_TYPE_TIMESTAMP, <<Increment:32/little-unsigned, Timestamp:32/little-unsigned>>};
encode_value({decimal128, Coeff, Exp}) when is_integer(Coeff), is_integer(Exp) ->
    encode_decimal128(Coeff, Exp);
encode_value({decimal128, infinity, _}) ->
    encode_decimal128_special(infinity);
encode_value({decimal128, neg_infinity, _}) ->
    encode_decimal128_special(neg_infinity);
encode_value({decimal128, nan, _}) ->
    encode_decimal128_special(nan);
encode_value({regex, Pattern, Options}) when is_binary(Pattern), is_binary(Options) ->
    {?BSON_TYPE_REGEX, <<Pattern/binary, 0, Options/binary, 0>>};
encode_value(minkey) ->
    {?BSON_TYPE_MINKEY, <<>>};
encode_value(maxkey) ->
    {?BSON_TYPE_MAXKEY, <<>>};
encode_value(Value) ->
    throw({encode_error, {unsupported_value, Value}}).

encode_integer(Value) when Value >= -2147483648, Value =< 2147483647 ->
    {?BSON_TYPE_INT32, <<Value:32/little-signed>>};
encode_integer(Value) when Value >= -9223372036854775808, Value =< 9223372036854775807 ->
    {?BSON_TYPE_INT64, <<Value:64/little-signed>>};
encode_integer(Value) ->
    throw({encode_error, {integer_out_of_range, Value}}).

encode_array(List) ->
    %% Arrays are documents with "0", "1", "2", ... as keys
    IndexedElements = index_list(List, 0, []),
    Elements = encode_elements(IndexedElements, []),
    ElementsBin = iolist_to_binary(Elements),
    ArrayLen = byte_size(ElementsBin) + 5,
    {?BSON_TYPE_ARRAY, <<ArrayLen:32/little, ElementsBin/binary, 0>>}.

index_list([], _, Acc) ->
    lists:reverse(Acc);
index_list([Value | Rest], Index, Acc) ->
    Key = integer_to_binary(Index),
    index_list(Rest, Index + 1, [{Key, Value} | Acc]).

%% Encode decimal128 from coefficient and exponent
encode_decimal128(Coeff, Exp) when Coeff >= 0 ->
    encode_decimal128_unsigned(Coeff, Exp, 0);
encode_decimal128(Coeff, Exp) when Coeff < 0 ->
    encode_decimal128_unsigned(-Coeff, Exp, 1).

encode_decimal128_unsigned(Coeff, Exp, Sign) ->
    %% Bias the exponent
    BiasedExp = Exp + 6176,

    %% Check if coefficient fits in normal format (< 2^113)
    MaxNormalCoeff = 1 bsl 113,
    if
        BiasedExp < 0 orelse BiasedExp > 16383 ->
            throw({encode_error, {decimal128_exponent_out_of_range, Exp}});
        Coeff >= MaxNormalCoeff ->
            throw({encode_error, {decimal128_coefficient_too_large, Coeff}});
        true ->
            %% Normal format
            Low = Coeff band 16#FFFFFFFFFFFFFFFF,
            CoeffHigh = Coeff bsr 64,
            High = (Sign bsl 63) bor (BiasedExp bsl 49) bor CoeffHigh,
            {?BSON_TYPE_DECIMAL128, <<Low:64/little-unsigned, High:64/little-unsigned>>}
    end.

encode_decimal128_special(infinity) ->
    %% Positive infinity: sign=0, combination=11110
    High = 16#7800000000000000,
    {?BSON_TYPE_DECIMAL128, <<0:64/little, High:64/little>>};
encode_decimal128_special(neg_infinity) ->
    %% Negative infinity: sign=1, combination=11110
    High = 16#F800000000000000,
    {?BSON_TYPE_DECIMAL128, <<0:64/little, High:64/little>>};
encode_decimal128_special(nan) ->
    %% NaN: combination=11111
    High = 16#7C00000000000000,
    {?BSON_TYPE_DECIMAL128, <<0:64/little, High:64/little>>}.
