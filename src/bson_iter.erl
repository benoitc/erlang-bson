%% @doc BSON binary iterator for zero-copy traversal.
%%
%% This module provides efficient traversal over BSON documents without
%% decoding values eagerly. It uses offset-based ValueRefs to defer
%% decoding until explicitly requested.
%%
%% WARNING: ValueRefs retain a reference to the original binary.
%% Call decode_value/2 to get a copied value that doesn't retain the source.
-module(bson_iter).

-include("bson_types.hrl").

%% API exports
-export([
    new/1,
    next/1,
    peek/2,
    find_path/2
]).

%% Types
-export_type([iter/0]).

-opaque iter() :: #bson_iter{}.

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
-spec next(iter()) -> {ok, binary(), atom(), map(), iter()} | done | {error, term()}.
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
-spec peek(binary(), binary()) -> {ok, atom(), map()} | not_found | {error, term()}.
peek(Bin, KeyBin) when is_binary(Bin), is_binary(KeyBin) ->
    case new(Bin) of
        {ok, Iter} ->
            peek_loop(Iter, KeyBin);
        {error, _} = Err ->
            Err
    end.

%% @doc Find a value by navigating a path through nested documents.
%% Path is a list of binary keys, e.g., [<<"outer">>, <<"inner">>, <<"value">>].
%% Skips entire subdocuments efficiently using length prefixes.
-spec find_path(binary(), [binary()]) -> {ok, atom(), map()} | not_found | {error, term()}.
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

%% =============================================================================
%% Internal Functions
%% =============================================================================

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
