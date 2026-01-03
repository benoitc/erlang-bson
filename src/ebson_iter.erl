%% @doc BSON binary iterator for zero-copy traversal.
%%
%% This module provides efficient traversal over BSON documents without
%% decoding values eagerly. It uses offset-based ValueRefs to defer
%% decoding until explicitly requested.
%%
%% == Design ==
%%
%% The iterator operates directly on the raw BSON binary using offsets,
%% allowing traversal without memory allocation for values. This is
%% ideal for hot paths like query filtering where only specific fields
%% need to be accessed.
%%
%% == API Overview ==
%%
%% <ul>
%%   <li>`new/1' - Create an iterator from a BSON binary</li>
%%   <li>`next/1' - Get next element (key, type, value ref)</li>
%%   <li>`peek/2' - Find a key at top level without iteration state</li>
%%   <li>`find_path/2' - Navigate nested documents via path</li>
%%   <li>`decode_value/2' - Decode a value ref to Erlang term</li>
%% </ul>
%%
%% == ValueRef and Memory Safety ==
%%
%% ValueRefs are maps containing `#{bin, off, len}' that point into the
%% original binary without copying data. This is efficient but means
%% the original binary is retained in memory as long as any ValueRef
%% derived from it exists.
%%
%% To break this reference chain, call `decode_value/2' which uses
%% `binary:copy/1' internally. For full document decoding, use
%% `ebson:decode_map/1' which ensures all data is copied.
%%
%% == Example Usage ==
%%
%% ```
%% %% Traverse and find a field
%% {ok, Iter} = ebson_iter:new(BsonBin),
%% case ebson_iter:next(Iter) of
%%     {ok, Key, Type, ValueRef, Iter2} ->
%%         {ok, Value} = ebson_iter:decode_value(Type, ValueRef),
%%         ...;
%%     done -> ...
%% end.
%%
%% %% Quick lookup without full iteration
%% {ok, Type, ValueRef} = ebson_iter:peek(BsonBin, <<"fieldname">>),
%% {ok, Value} = ebson_iter:decode_value(Type, ValueRef).
%%
%% %% Nested path lookup
%% {ok, Type, ValueRef} = ebson_iter:find_path(BsonBin, [<<"a">>, <<"b">>, <<"c">>]).
%% '''
%%
%% @end
-module(ebson_iter).

-include("bson_types.hrl").

%% API exports
-export([
    new/1,
    next/1,
    peek/2,
    find_path/2,
    decode_value/2
]).

%% Types
-export_type([iter/0, bson_type/0, value_ref/0]).

-opaque iter() :: #bson_iter{}.

-type bson_type() :: double | string | document | array | binary | objectid |
                     boolean | datetime | null | int32 | int64 | timestamp |
                     decimal128 | regex | javascript | minkey | maxkey.

-type value_ref() :: #{bin := binary(), off := non_neg_integer(), len := non_neg_integer()}.

%% Minimum BSON document size: 4 (length) + 1 (terminator) = 5 bytes
-define(MIN_DOC_SIZE, 5).

%% =============================================================================
%% API Functions
%% =============================================================================

%% @doc Create a new iterator from a BSON binary.
%% Validates document structure: length prefix and terminator.
-spec new(binary()) -> {ok, iter()} | {error, term()}.
new(Bin) when is_binary(Bin) ->
    Size = byte_size(Bin),
    if
        Size < ?MIN_DOC_SIZE ->
            {error, {invalid_length, ?MIN_DOC_SIZE, Size}};
        true ->
            <<DeclaredLen:32/little-signed, _/binary>> = Bin,
            validate_document(Bin, DeclaredLen, Size)
    end;
new(_) ->
    {error, not_a_binary}.

%% @doc Advance to the next element in the document.
%% Returns {ok, Key, Type, ValueRef, NewIter} for each element,
%% 'done' when iteration is complete, or {error, Reason} on malformed input.
-spec next(iter()) -> {ok, binary(), bson_type(), value_ref(), iter()} | done | {error, term()}.
next(#bson_iter{bin = Bin, pos = Pos, doc_end = DocEnd} = Iter) ->
    if
        Pos >= DocEnd ->
            done;
        true ->
            case Bin of
                <<_:Pos/binary, 0, _/binary>> ->
                    %% Null terminator - end of document
                    done;
                <<_:Pos/binary, TypeByte, Rest/binary>> ->
                    parse_element(Iter, TypeByte, Pos + 1, Rest);
                _ ->
                    {error, {truncated_document, Pos}}
            end
    end.

%% @doc Look up a key at the top level of a BSON document without decoding.
%% Returns {ok, Type, ValueRef} if found, not_found if key doesn't exist,
%% or {error, Reason} on malformed input.
-spec peek(binary(), binary()) -> {ok, bson_type(), value_ref()} | not_found | {error, term()}.
peek(Bin, KeyBin) when is_binary(Bin), is_binary(KeyBin) ->
    case new(Bin) of
        {ok, Iter} ->
            peek_loop(Iter, KeyBin);
        {error, _} = Err ->
            Err
    end.

%% @doc Find a value by navigating a path through nested documents.
%% Path is a list of binary keys. Skips entire subdocuments efficiently
%% using length prefixes.
-spec find_path(binary(), [binary()]) -> {ok, bson_type(), value_ref()} | not_found | {error, term()}.
find_path(Bin, []) when is_binary(Bin) ->
    %% Empty path - return the document itself as a value ref
    case new(Bin) of
        {ok, _} ->
            {ok, document, #{bin => Bin, off => 0, len => byte_size(Bin)}};
        {error, _} = Err ->
            Err
    end;
find_path(Bin, [Key | RestPath]) when is_binary(Bin), is_binary(Key) ->
    case peek(Bin, Key) of
        {ok, Type, ValueRef} when RestPath =:= [] ->
            {ok, Type, ValueRef};
        {ok, document, #{bin := DocBin, off := Off, len := Len}} ->
            %% Extract nested document and continue
            NestedDoc = binary:part(DocBin, Off, Len),
            find_path(NestedDoc, RestPath);
        {ok, array, #{bin := DocBin, off := Off, len := Len}} ->
            %% Arrays are documents, can navigate into them
            NestedDoc = binary:part(DocBin, Off, Len),
            find_path(NestedDoc, RestPath);
        {ok, _OtherType, _} ->
            %% Trying to navigate into a non-document type
            not_found;
        not_found ->
            not_found;
        {error, _} = Err ->
            Err
    end;
find_path(_, _) ->
    {error, invalid_arguments}.

%% @doc Decode a value from a ValueRef into an Erlang term.
%% All binary data is copied using binary:copy/1 to break reference chains.
%% Embedded documents/arrays are NOT recursively decoded - use decode_map/1 for that.
-spec decode_value(bson_type(), value_ref()) -> {ok, term()} | {error, term()}.
decode_value(double, #{bin := Bin, off := Off, len := 8}) ->
    <<_:Off/binary, Value:64/little-float, _/binary>> = Bin,
    {ok, Value};

decode_value(string, #{bin := Bin, off := Off, len := Len}) when Len >= 5 ->
    %% String format: int32 length + data + null
    <<_:Off/binary, StrLen:32/little, _/binary>> = Bin,
    DataStart = Off + 4,
    DataLen = StrLen - 1,  %% Exclude null terminator
    if
        DataLen >= 0, DataLen + 5 =< Len ->
            StrBin = binary:copy(binary:part(Bin, DataStart, DataLen)),
            {ok, StrBin};
        true ->
            {error, {invalid_string, Off}}
    end;

decode_value(document, #{bin := Bin, off := Off, len := Len}) ->
    %% Return the raw document binary (copied)
    DocBin = binary:copy(binary:part(Bin, Off, Len)),
    {ok, {document, DocBin}};

decode_value(array, #{bin := Bin, off := Off, len := Len}) ->
    %% Return the raw array binary (copied) - caller can iterate if needed
    ArrayBin = binary:copy(binary:part(Bin, Off, Len)),
    {ok, {array, ArrayBin}};

decode_value(binary, #{bin := Bin, off := Off, len := Len}) when Len >= 5 ->
    %% Binary format: int32 length + subtype byte + data
    <<_:Off/binary, DataLen:32/little, Subtype:8, _/binary>> = Bin,
    DataStart = Off + 5,
    if
        DataLen >= 0, DataLen + 5 =:= Len ->
            Data = binary:copy(binary:part(Bin, DataStart, DataLen)),
            {ok, {binary, Subtype, Data}};
        true ->
            {error, {invalid_binary, Off}}
    end;

decode_value(objectid, #{bin := Bin, off := Off, len := 12}) ->
    OidBin = binary:copy(binary:part(Bin, Off, 12)),
    {ok, {objectid, OidBin}};

decode_value(boolean, #{bin := Bin, off := Off, len := 1}) ->
    <<_:Off/binary, Value:8, _/binary>> = Bin,
    case Value of
        0 -> {ok, false};
        1 -> {ok, true};
        _ -> {error, {invalid_boolean, Value, Off}}
    end;

decode_value(datetime, #{bin := Bin, off := Off, len := 8}) ->
    <<_:Off/binary, Value:64/little-signed, _/binary>> = Bin,
    {ok, {datetime_ms, Value}};

decode_value(null, #{len := 0}) ->
    {ok, null};

decode_value(int32, #{bin := Bin, off := Off, len := 4}) ->
    <<_:Off/binary, Value:32/little-signed, _/binary>> = Bin,
    {ok, Value};

decode_value(int64, #{bin := Bin, off := Off, len := 8}) ->
    <<_:Off/binary, Value:64/little-signed, _/binary>> = Bin,
    {ok, Value};

decode_value(timestamp, #{bin := Bin, off := Off, len := 8}) ->
    %% MongoDB timestamp: uint32 increment + uint32 timestamp
    <<_:Off/binary, Increment:32/little-unsigned, Timestamp:32/little-unsigned, _/binary>> = Bin,
    {ok, {timestamp, Increment, Timestamp}};

decode_value(decimal128, #{bin := Bin, off := Off, len := 16}) ->
    %% Extract raw 16 bytes, decode later
    RawBin = binary:part(Bin, Off, 16),
    decode_decimal128(RawBin);

decode_value(minkey, #{len := 0}) ->
    {ok, minkey};

decode_value(maxkey, #{len := 0}) ->
    {ok, maxkey};

decode_value(regex, #{bin := Bin, off := Off, len := Len}) ->
    %% Regex: cstring pattern + cstring options
    SearchBin = binary:part(Bin, Off, Len),
    case find_null(SearchBin, 0) of
        {ok, PatternLen} ->
            Pattern = binary:copy(binary:part(Bin, Off, PatternLen)),
            OptionsStart = Off + PatternLen + 1,
            OptionsLen = Len - PatternLen - 2,  %% -2 for both nulls
            Options = binary:copy(binary:part(Bin, OptionsStart, OptionsLen)),
            {ok, {regex, Pattern, Options}};
        not_found ->
            {error, {invalid_regex, Off}}
    end;

decode_value(javascript, #{bin := Bin, off := Off, len := Len}) when Len >= 5 ->
    %% JavaScript: same format as string
    <<_:Off/binary, StrLen:32/little, _/binary>> = Bin,
    DataStart = Off + 4,
    DataLen = StrLen - 1,
    if
        DataLen >= 0, DataLen + 5 =< Len ->
            JsBin = binary:copy(binary:part(Bin, DataStart, DataLen)),
            {ok, {javascript, JsBin}};
        true ->
            {error, {invalid_javascript, Off}}
    end;

decode_value(Type, _ValueRef) ->
    {error, {unsupported_decode, Type}}.

%% =============================================================================
%% Internal Functions
%% =============================================================================

%% Decode IEEE 754-2008 decimal128 to {decimal128, Coefficient, Exponent}
decode_decimal128(<<Low:64/little-unsigned, High:64/little-unsigned>>) ->
    %% Extract sign bit (bit 63 of high word)
    Sign = (High bsr 63) band 1,

    %% Check combination field (bits 58-62 of high word)
    %% G0-G1 are bits 62-61
    CombHigh2 = (High bsr 61) band 16#3,

    case CombHigh2 of
        2#11 ->
            %% Special value or large coefficient
            %% Check G2 (bit 60)
            G2 = (High bsr 60) band 1,
            case G2 of
                0 ->
                    %% Large coefficient format (G0-G1-G2 = 110)
                    Exponent = ((High bsr 47) band 16#3FFF) - 6176,
                    %% Implicit leading bits 100 for coefficient
                    CoeffHigh = (8 bor ((High bsr 46) band 1)) bsl 110,
                    CoeffMid = (High band 16#3FFFFFFFFFFF) bsl 64,
                    Coefficient = CoeffHigh bor CoeffMid bor Low,
                    SignedCoeff = apply_sign(Sign, Coefficient),
                    {ok, {decimal128, SignedCoeff, Exponent}};
                1 ->
                    %% G0-G1-G2 = 111, check G3-G4 for infinity/NaN
                    %% G3 is bit 59, G4 is bit 58
                    G4 = (High bsr 58) band 1,
                    case G4 of
                        0 ->
                            %% Infinity (G3-G4 pattern with G4=0)
                            case Sign of
                                0 -> {ok, {decimal128, infinity, 0}};
                                1 -> {ok, {decimal128, neg_infinity, 0}}
                            end;
                        1 ->
                            %% NaN (G4=1)
                            {ok, {decimal128, nan, 0}}
                    end
            end;
        _ ->
            %% Normal format
            Exponent = ((High bsr 49) band 16#3FFF) - 6176,
            CoeffHigh = (High band 16#1FFFFFFFFFFFF) bsl 64,
            Coefficient = CoeffHigh bor Low,
            SignedCoeff = apply_sign(Sign, Coefficient),
            {ok, {decimal128, SignedCoeff, Exponent}}
    end.

apply_sign(0, Coeff) -> Coeff;
apply_sign(1, Coeff) -> -Coeff.

peek_loop(Iter, KeyBin) ->
    case next(Iter) of
        {ok, Key, Type, ValueRef, NextIter} ->
            if
                Key =:= KeyBin ->
                    {ok, Type, ValueRef};
                true ->
                    peek_loop(NextIter, KeyBin)
            end;
        done ->
            not_found;
        {error, _} = Err ->
            Err
    end.

validate_document(Bin, DeclaredLen, ActualSize) when DeclaredLen =:= ActualSize ->
    %% Check terminator byte
    TerminatorPos = DeclaredLen - 1,
    case Bin of
        <<_:TerminatorPos/binary, 0>> ->
            {ok, #bson_iter{
                bin = Bin,
                pos = 4,  %% Start after length prefix
                doc_end = TerminatorPos
            }};
        _ ->
            {error, {missing_terminator, TerminatorPos}}
    end;
validate_document(_Bin, DeclaredLen, ActualSize) ->
    {error, {invalid_length, DeclaredLen, ActualSize}}.

parse_element(Iter, TypeByte, KeyStart, RestAfterType) ->
    #bson_iter{bin = Bin, doc_end = DocEnd} = Iter,
    %% Find null terminator for cstring key
    case find_null(RestAfterType, 0) of
        {ok, KeyLen} ->
            Key = binary:part(Bin, KeyStart, KeyLen),
            ValueStart = KeyStart + KeyLen + 1, %% +1 for null terminator
            parse_value(Iter, TypeByte, Key, ValueStart, DocEnd);
        not_found ->
            {error, {invalid_key, KeyStart}}
    end.

find_null(<<0, _/binary>>, Offset) ->
    {ok, Offset};
find_null(<<_, Rest/binary>>, Offset) ->
    find_null(Rest, Offset + 1);
find_null(<<>>, _Offset) ->
    not_found.

parse_value(Iter, TypeByte, Key, ValueStart, DocEnd) ->
    #bson_iter{bin = Bin} = Iter,
    case value_size(TypeByte, Bin, ValueStart, DocEnd) of
        {ok, Type, ValueLen} ->
            ValueRef = #{bin => Bin, off => ValueStart, len => ValueLen},
            NextPos = ValueStart + ValueLen,
            NewIter = Iter#bson_iter{pos = NextPos},
            {ok, Key, Type, ValueRef, NewIter};
        {error, _} = Err ->
            Err
    end.

%% Calculate value size based on BSON type.
%% Returns {ok, TypeAtom, ByteLength} or {error, Reason}.
value_size(?BSON_TYPE_DOUBLE, _Bin, _Off, _DocEnd) ->
    {ok, double, 8};

value_size(?BSON_TYPE_STRING, Bin, Off, DocEnd) ->
    %% String: int32 length + data + null terminator
    %% Length includes the null terminator
    string_size(Bin, Off, DocEnd);

value_size(?BSON_TYPE_DOCUMENT, Bin, Off, DocEnd) ->
    %% Embedded document: int32 length (includes self)
    embedded_doc_size(Bin, Off, DocEnd, document);

value_size(?BSON_TYPE_ARRAY, Bin, Off, DocEnd) ->
    %% Array: same format as document
    embedded_doc_size(Bin, Off, DocEnd, array);

value_size(?BSON_TYPE_BINARY, Bin, Off, DocEnd) ->
    %% Binary: int32 length + subtype byte + data
    binary_size(Bin, Off, DocEnd);

value_size(?BSON_TYPE_OBJECTID, _Bin, _Off, _DocEnd) ->
    {ok, objectid, 12};

value_size(?BSON_TYPE_BOOLEAN, _Bin, _Off, _DocEnd) ->
    {ok, boolean, 1};

value_size(?BSON_TYPE_DATETIME, _Bin, _Off, _DocEnd) ->
    {ok, datetime, 8};

value_size(?BSON_TYPE_NULL, _Bin, _Off, _DocEnd) ->
    {ok, null, 0};

value_size(?BSON_TYPE_INT32, _Bin, _Off, _DocEnd) ->
    {ok, int32, 4};

value_size(?BSON_TYPE_TIMESTAMP, _Bin, _Off, _DocEnd) ->
    {ok, timestamp, 8};

value_size(?BSON_TYPE_INT64, _Bin, _Off, _DocEnd) ->
    {ok, int64, 8};

value_size(?BSON_TYPE_DECIMAL128, _Bin, _Off, _DocEnd) ->
    {ok, decimal128, 16};

value_size(?BSON_TYPE_MINKEY, _Bin, _Off, _DocEnd) ->
    {ok, minkey, 0};

value_size(?BSON_TYPE_MAXKEY, _Bin, _Off, _DocEnd) ->
    {ok, maxkey, 0};

value_size(?BSON_TYPE_REGEX, Bin, Off, DocEnd) ->
    %% Regex: cstring pattern + cstring options
    regex_size(Bin, Off, DocEnd);

value_size(?BSON_TYPE_JAVASCRIPT, Bin, Off, DocEnd) ->
    %% JavaScript: same format as string
    case string_size(Bin, Off, DocEnd) of
        {ok, _, Len} -> {ok, javascript, Len};
        Err -> Err
    end;

value_size(TypeByte, _Bin, _Off, _DocEnd) ->
    {error, {unsupported_type, TypeByte}}.

string_size(Bin, Off, DocEnd) ->
    if
        Off + 4 > DocEnd ->
            {error, {truncated_value, string, Off}};
        true ->
            <<_:Off/binary, StrLen:32/little-signed, _/binary>> = Bin,
            if
                StrLen < 1 ->
                    {error, {invalid_string_length, StrLen, Off}};
                Off + 4 + StrLen > DocEnd + 1 ->
                    {error, {truncated_value, string, Off}};
                true ->
                    {ok, string, 4 + StrLen}
            end
    end.

embedded_doc_size(Bin, Off, DocEnd, Type) ->
    if
        Off + 4 > DocEnd ->
            {error, {truncated_value, Type, Off}};
        true ->
            <<_:Off/binary, DocLen:32/little-signed, _/binary>> = Bin,
            if
                DocLen < ?MIN_DOC_SIZE ->
                    {error, {invalid_document_length, DocLen, Off}};
                Off + DocLen > DocEnd + 1 ->
                    {error, {truncated_value, Type, Off}};
                true ->
                    {ok, Type, DocLen}
            end
    end.

binary_size(Bin, Off, DocEnd) ->
    if
        Off + 5 > DocEnd ->
            {error, {truncated_value, binary, Off}};
        true ->
            <<_:Off/binary, DataLen:32/little-signed, _/binary>> = Bin,
            if
                DataLen < 0 ->
                    {error, {invalid_binary_length, DataLen, Off}};
                Off + 5 + DataLen > DocEnd + 1 ->
                    {error, {truncated_value, binary, Off}};
                true ->
                    %% 4 (length) + 1 (subtype) + data
                    {ok, binary, 5 + DataLen}
            end
    end.

regex_size(Bin, Off, DocEnd) ->
    %% Find end of pattern cstring
    MaxLen = DocEnd - Off + 1,
    SearchBin = binary:part(Bin, Off, MaxLen),
    case find_null(SearchBin, 0) of
        {ok, PatternLen} ->
            %% Find end of options cstring
            OptionsStart = Off + PatternLen + 1,
            OptionsSearchBin = binary:part(Bin, OptionsStart, DocEnd - OptionsStart + 1),
            case find_null(OptionsSearchBin, 0) of
                {ok, OptionsLen} ->
                    %% Total: pattern + null + options + null
                    {ok, regex, PatternLen + 1 + OptionsLen + 1};
                not_found ->
                    {error, {invalid_regex, Off}}
            end;
        not_found ->
            {error, {invalid_regex, Off}}
    end.
