%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2012 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_leofs).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").
-include_lib("leo_s3_libs/include/leo_s3_auth.hrl").

-record(url, {abspath, host, port, username, password, path, protocol, host_type}).

-record(state, { base_urls,          % Tuple of #url -- one for each IP
                 base_urls_index,    % #url to use for next request
                 path_params,        % Params to append on the path
                 id,                 % Job ID passed through new/1 minus 1 -> 0... concurrent
                 concurrent,         % Number of Job Processed
                 value_source,       % source for a value generator
                 value_size_groups,  % size groups for a value generator
                 aws_chunk_size,     % aws-chunked Chunk Size
                 aws_chunk_nohash,   % aws-chunked skip content SHA-256
                 retry_on_overload,  % whether retry when LeoFS is overloaded (503)
                 mp_part_size,       % Part Size for Multipart Upload
                 mp_part_bin,        % Pre-gen Binary for Multipart Upload
                 check_integrity}).  % Params to test data integrity

-define(S3_ACC_KEY,      "05236").
-define(S3_SEC_KEY,      <<"802562235">>).
-define(S3_CONTENT_TYPE, "application/octet-stream").
-define(ETS_BODY_MD5, ets_body_md5).
-define(OL_SLEEP_INTERNVAL, 500).

%% ====================================================================
%% API
%% ====================================================================
new(Id) ->
    %% Guaranteed One shot
    case Id of
        1 -> ets:new(?ETS_BODY_MD5, [public, named_table, {read_concurrency, true}]);
        _ -> void
    end,
    %% Make sure ibrowse is available
    case code:which(ibrowse) of
        non_existing ->
            ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,

    application:start(ibrowse),

    %% The IPs, port and path we'll be testing
    Ips  = basho_bench_config:get(http_raw_ips,      ["localhost"]),
    Port = basho_bench_config:get(http_raw_port,     8080),
    Path = basho_bench_config:get(http_raw_path,     "/test_bucket/_test"),
    Params = basho_bench_config:get(http_raw_params, ""),
    Disconnect = basho_bench_config:get(http_raw_disconnect_frequency, infinity),
    
    PartSize = basho_bench_config:get(mp_part_size, 0),
    PartBin = crypto:rand_bytes(PartSize),

    case Disconnect of
        infinity -> ok;
        Seconds when is_integer(Seconds) -> ok;
        {ops, Ops} when is_integer(Ops) -> ok;
        _ -> ?FAIL_MSG("Invalid configuration for http_raw_disconnect_frequency: ~p~n", [Disconnect])
    end,

    %% Uses pdict to avoid threading state record through lots of functions
    erlang:put(disconnect_freq, Disconnect),

    %% If there are multiple URLs, convert the list to a tuple so we can efficiently
    %% round-robin through them.
    case length(Ips) of
        1 ->
            [Ip] = Ips,
            BaseUrls = #url { host = Ip, port = Port, path = Path },
            BaseUrlsIndex = 1;
        _ ->
            BaseUrls = list_to_tuple([ #url { host = Ip, port = Port, path = Path }
                                       || Ip <- Ips]),
            BaseUrlsIndex = random:uniform(tuple_size(BaseUrls))
    end,
    CI = basho_bench_config:get(
           check_integrity, false), %% should be false when doing benchmark
    VSG = basho_bench_config:get(
           value_size_groups, [{1, 4096, 8192},{1, 16384, 32768}]),
    Concurrent = basho_bench_config:get(concurrent, 0),
    AWSChunkSize = basho_bench_config:get(aws_chunk_size, 131072),
    AWSChunkNoHash = basho_bench_config:get(aws_chunk_nohash, true),
    RetryOnOL = basho_bench_config:get(retry_on_overload, false),
    {ok, #state { base_urls = BaseUrls,
                  base_urls_index = BaseUrlsIndex,
                  path_params = Params,
                  value_source = init_source(),
                  value_size_groups = size_group_load_config(VSG, []),
                  id = Id - 1,
                  concurrent = Concurrent,
                  aws_chunk_size = AWSChunkSize,
                  aws_chunk_nohash = AWSChunkNoHash,
                  retry_on_overload = RetryOnOL,
                  mp_part_size = PartSize,
                  mp_part_bin = PartBin,
                  check_integrity = CI }}.

keygen_global_uniq(false, _Id, _Concurrent, KeyGen) ->
    KeyGen();
keygen_global_uniq(true, Id, Concurrent, KeyGen) ->
    Base = KeyGen(),
    Rem = Base rem Concurrent,
    Diff = Rem - Id,
    Base - Diff.

run(get, KeyGen, _ValueGen, #state{check_integrity = CI, 
                                   retry_on_overload = RetryOnOL,
                                   id = Id, concurrent = Concurrent} = State) ->
    Key = keygen_global_uniq(CI, Id, Concurrent, KeyGen),
    {NextUrl, S2} = next_url(State),
    case do_get(url(NextUrl, Key, State#state.path_params), RetryOnOL) of
        {not_found, _Url} ->
            {ok, S2};
        {ok, _Url, _Headers, Body} ->
            case CI of
                true ->
                    case ets:lookup(?ETS_BODY_MD5, Key) of
                        [{_Key, LocalMD5}|_] ->
                            RemoteMD5 = erlang:md5(Body),
                            case RemoteMD5 =:= LocalMD5 of
                                true -> {ok, S2};
                                false -> {error, checksum_error, S2}
                            end;
                        _ -> {ok, S2}
                    end;
                false -> {ok, S2}
            end;
        {error, Reason} ->
            io:format("~p~n",[Reason]),
            {error, Reason, S2}
    end;

run(getv4, KeyGen, _ValueGen, #state{check_integrity = CI, 
                                     retry_on_overload = RetryOnOL,
                                     id = Id, concurrent = Concurrent} = State) ->
    Key = keygen_global_uniq(CI, Id, Concurrent, KeyGen),
    {NextUrl, S2} = next_url(State),
    case do_get_v4(url(NextUrl, Key, State#state.path_params), RetryOnOL) of
        {not_found, _Url} ->
            {ok, S2};
        {ok, _Url, _Headers, Body} ->
            case CI of
                true ->
                    case ets:lookup(?ETS_BODY_MD5, Key) of
                        [{_Key, LocalMD5}|_] ->
                            RemoteMD5 = erlang:md5(Body),
                            case RemoteMD5 =:= LocalMD5 of
                                true -> {ok, S2};
                                false -> {error, checksum_error, S2}
                            end;
                        _ -> {ok, S2}
                    end;
                false -> {ok, S2}
            end;
        {error, Reason} ->
            io:format("~p~n",[Reason]),
            {error, Reason, S2}
    end;

run(test, _KeyGen, _ValueGen, #state{value_source = VS,
                                     value_size_groups = VSG} = State) ->
    {Group, _Val} = value_gen_with_size_groups(VS, VSG),
    C = case erlang:get({size_groups_counter, Group}) of
        undefined -> 0;
        Else -> Else
    end,
    erlang:put({size_groups_counter, Group}, C + 1),
    case C rem 10000 of
        0 -> io:format(user, "[debug] group:~p counter:~p~n", [Group, C]);
        _ -> void
    end,
    {ok, State};

run(put, KeyGen, _ValueGen, #state{check_integrity = CI,
                                   retry_on_overload = RetryOnOL,
                                   value_source = VS,
                                   value_size_groups = VSG, 
                                   id = Id,
                                   concurrent = Concurrent} = State) ->
    Key = keygen_global_uniq(CI, Id, Concurrent, KeyGen),
    {_, Val} = value_gen_with_size_groups(VS, VSG),
    {NextUrl, S2} = next_url(State),
    Url = url(NextUrl, Key, State#state.path_params),
    case do_put(Url, [], Val, RetryOnOL) of
        ok ->
            case CI of
                true ->
                    LocalMD5 = erlang:md5(Val),
                    ets:insert(?ETS_BODY_MD5, {Key, LocalMD5});
                false -> void
            end,
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(putv4, KeyGen, _ValueGen, #state{check_integrity = CI,
                                     retry_on_overload = RetryOnOL,
                                     value_source = VS,
                                     value_size_groups = VSG,
                                     id = Id,
                                     concurrent = Concurrent} = State) ->
    Key = keygen_global_uniq(CI, Id, Concurrent, KeyGen),
    {_, Val} = value_gen_with_size_groups(VS, VSG),
    {NextUrl, S2} = next_url(State),
    Url = url(NextUrl, Key, State#state.path_params),
    case do_put_v4(Url, [], Val, <<"dummy">>, RetryOnOL) of
        ok ->
            case CI of
                true ->
                    LocalMD5 = erlang:md5(Val),
                    ets:insert(?ETS_BODY_MD5, {Key, LocalMD5});
                false -> void
            end,
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(putv4chunk, KeyGen, _ValueGen, #state{check_integrity = CI,
                                          retry_on_overload = RetryOnOL,
                                          value_source = VS,
                                          value_size_groups = VSG,
                                          id = Id,
                                          aws_chunk_size = AWSChunkSize,
                                          aws_chunk_nohash = AWSChunkNoHash,
                                          concurrent = Concurrent} = State) ->
    Key = keygen_global_uniq(CI, Id, Concurrent, KeyGen),
    {_, Val} = value_gen_with_size_groups(VS, VSG),
    {NextUrl, S2} = next_url(State),
    Url = url(NextUrl, Key, State#state.path_params),
    case do_put_v4_chunk(Url, [], Val, AWSChunkSize, AWSChunkNoHash, RetryOnOL) of
        ok ->
            case CI of
                true ->
                    LocalMD5 = erlang:md5(Val),
                    ets:insert(?ETS_BODY_MD5, {Key, LocalMD5});
                false -> void
            end,
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(mp_put, KeyGen, _ValueGen, #state{retry_on_overload = RetryOnOL,
                                      mp_part_size = PartSize,
                                      mp_part_bin = PartBin,
                                      value_size_groups = VSG} = State) ->
    Key = KeyGen(),
    {NextUrl, S2} = next_url(State),
    Url = url(NextUrl, Key, State#state.path_params),
    
    Len = length(VSG),
    Nth = random:uniform(Len),
    {Min, Max} = lists:nth(Nth, VSG),
    Size = case Max > Min of
        true -> random:uniform(Max - Min) + Min - 1;
        false -> Max
    end,
    case do_mp(Url, [], Size, PartSize, PartBin, RetryOnOL) of
        ok ->
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end;

run(delete, KeyGen, _ValueGen, #state{check_integrity = CI, 
                                      retry_on_overload = RetryOnOL,
                                      id = Id, concurrent = Concurrent} = State) ->
    Key = keygen_global_uniq(CI, Id, Concurrent, KeyGen),
    {NextUrl, S2} = next_url(State),
    case do_delete(url(NextUrl, Key, State#state.path_params), RetryOnOL) of
        {not_found, _Url} ->
            {ok, S2};
        {ok, _Url, _Headers} ->
            case CI of
                true ->
                    ets:delete(?ETS_BODY_MD5, Key);
                false -> void
            end,
            {ok, S2};
        {error, Reason} ->
            {error, Reason, S2}
    end.

%% original value generator which can specify percentage per a size group
size_group_load_config([], Acc) ->
    lists:flatten(Acc);
size_group_load_config([{Weight, Min, Max}|Rest], Acc) ->
    List = lists:duplicate(Weight, {Min, Max}),
    size_group_load_config(Rest, [List|Acc]).

init_source() ->
    SourceSz = basho_bench_config:get(?VAL_GEN_SRC_SIZE, 1048576),
    {?VAL_GEN_SRC_SIZE, SourceSz, crypto:rand_bytes(SourceSz)}.

data_block({SourceCfg, SourceSz, Source}, BlockSize) ->
    case SourceSz - BlockSize > 0 of
        true ->
            Offset = random:uniform(SourceSz - BlockSize),
            <<_:Offset/bytes, Slice:BlockSize/bytes, _Rest/binary>> = Source,
            Slice;
        false ->
            ?WARN("~p is too small ~p < ~p\n",
                  [SourceCfg, SourceSz, BlockSize]),
            Source
    end.

value_gen_with_size_groups(ValueSource, SizeGroups) ->
    Len = length(SizeGroups),
    Nth = random:uniform(Len),
    {Min, Max} = lists:nth(Nth, SizeGroups),
    Size = case Max > Min of
        true -> random:uniform(Max - Min) + Min - 1;
        false -> Max
    end,
    {Nth, data_block(ValueSource, Size)}.
   
%% ====================================================================
%% Internal functions
%% ====================================================================

next_url(State) when is_record(State#state.base_urls, url) ->
    {State#state.base_urls, State};

next_url(State) when State#state.base_urls_index > tuple_size(State#state.base_urls) ->
    { element(1, State#state.base_urls),
      State#state { base_urls_index = 1 } };

next_url(State) ->
    { element(State#state.base_urls_index, State#state.base_urls),
      State#state { base_urls_index = State#state.base_urls_index + 1 }}.

%% url(BaseUrl, Params) ->
%%     BaseUrl#url { path = lists:concat([BaseUrl#url.path, Params]) }.
url(BaseUrl, Key, Params) ->
    BaseUrl#url { path = lists:concat([BaseUrl#url.path, '/', Key, Params]) }.

do_get_v4(Url, RetryOnOL) ->
    TS = leo_date:now(),
    ValSHA256 = leo_hex:binary_to_hexbin(crypto:hash(sha256, <<>>)),
    {_, _, _, Auth} = gen_sig_v4("GET", Url, ValSHA256, TS),
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"x-amz-content-sha256", ValSHA256},
                 {"x-amz-date", iso8601_date_format(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", Auth}
                ],
    case send_request(Url, Headers_2, get, [], [{response_format, binary}]) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "200", Headers, Body} ->
            {ok, Url, Headers, Body};
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> do_get_v4(Url, RetryOnOL) end, RetryOnOL);
        {ok, Code, _Headers, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_get(Url, RetryOnOL) ->
    TS = leo_date:now(),
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", gen_sig("GET", Url, TS)}
                ],
    case send_request(Url, Headers_2, get, [], [{response_format, binary}]) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "200", Headers, Body} ->
            {ok, Url, Headers, Body};
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> do_get(Url, RetryOnOL) end, RetryOnOL);
        {ok, Code, _Headers, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

iso8601_date_format(GregorianSeconds) ->
    {{Year, Month, Date},{Hour,Min,Sec}} =
        calendar:universal_time_to_local_time(
          calendar:gregorian_seconds_to_datetime(GregorianSeconds)),
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0wT~2..0w~2..0w~2..0wZ",
                  [Year, Month, Date, Hour, Min, Sec])).

gen_sig(HTTPMethod, Url, TS) ->
    [Bucket|_] = string:tokens(Url#url.path, "/"),
    SignParams = #sign_params{http_verb    = list_to_binary(HTTPMethod),
                              content_type = list_to_binary(?S3_CONTENT_TYPE),
                              date         = list_to_binary(leo_http:rfc1123_date(TS)),
                              bucket       = list_to_binary(Bucket),
                              raw_uri      = list_to_binary(Url#url.path),
                              requested_uri = list_to_binary(Url#url.path),
                              sign_ver     = ?AWS_SIGN_VER_2
                             },
    {Sig, _, _} = leo_s3_auth:get_signature(?S3_SEC_KEY, SignParams, #sign_v4_params{}),
    io_lib:format("AWS ~s:~s", [?S3_ACC_KEY, Sig]).

gen_sig_v4(HTTPMethod, Url, ValSHA256, TS) ->
    Headers = [{<<"x-amz-content-sha256">>, ValSHA256},
               {<<"x-amz-date">>, list_to_binary(iso8601_date_format(TS))}],
    SignParams = #sign_params{http_verb    = list_to_binary(HTTPMethod),
                              raw_uri      = list_to_binary(Url#url.path),
                              sign_ver     = ?AWS_SIGN_VER_4,
                              headers      = Headers},
    Credential = <<?S3_ACC_KEY, "/20130524/us-east-1/s3/aws4_request">>,
    SignedHeader = <<"x-amz-content-sha256">>,
    SignV4Params = #sign_v4_params{credential = Credential,
                                   signed_headers = SignedHeader},
    {Sig, SignHead, SignKey} = leo_s3_auth:get_signature(?S3_SEC_KEY, SignParams, SignV4Params),
    Auth = io_lib:format("AWS4-HMAC-SHA256 Credential=~s, SignedHeaders=~s, Signature=~s", [Credential, SignedHeader, Sig]),
    {Sig, SignHead, SignKey, Auth}.

do_put(Url, Headers, Value, RetryOnOL) ->
    TS = leo_date:now(),
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", gen_sig("PUT", Url, TS)}
                ],
    case send_request(Url, Headers ++ Headers_2,
                      put, Value, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> do_put(Url, Headers, Value, RetryOnOL) end, RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_put_v4(Url, Headers, Value, ValSHA256, RetryOnOL) ->
    TS = leo_date:now(),
    {_, _, _, Auth} = gen_sig_v4("PUT", Url, ValSHA256, TS),
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"x-amz-content-sha256", binary_to_list(ValSHA256)},
                 {"x-amz-date", iso8601_date_format(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", Auth}
                ],
    case send_request(Url, Headers ++ Headers_2,
                      put, Value, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> do_put_v4(Url, Headers, Value, ValSHA256, RetryOnOL) end, RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

do_put_v4_chunk(Url, Headers, Value, ChunkSize, NoHash, RetryOnOL) ->
    TS = leo_date:now(),
    {Sign, _SignHead, SignKey, Auth} = gen_sig_v4("PUT", Url, <<"STREAMING-AWS4-HMAC-SHA256-PAYLOAD">>, TS),
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
                 {"x-amz-date", iso8601_date_format(TS)},
                 {"x-amz-decoded-content-length", byte_size(Value)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", Auth}
                ],
    Chunks = gen_chunks(Value, Sign, ChunkSize, SignKey, NoHash, TS),
    case send_request(Url, Headers ++ Headers_2,
                      put, Chunks, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> do_put_v4_chunk(Url, Headers, Value, ChunkSize, NoHash, RetryOnOL) end , RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Generate aws-chunked Chunks
%% @private
gen_chunks(Bin, Signature, ChunkSize, SignKey, NoHash, TS) ->
    gen_chunks(Bin, Signature, <<>>, byte_size(Bin), ChunkSize, SignKey, NoHash, TS).

gen_chunks(_Bin, PrevSign, Acc, 0, _ChunkSize, SignKey, NoHash, TS) ->
    {Chunk, _Sign} = compute_chunk(<<>>, PrevSign, SignKey, NoHash, TS),
    <<Acc/binary, Chunk/binary>>;
gen_chunks(Bin, PrevSign, Acc, Remain, ChunkSize, SignKey, NoHash, TS) when Remain < ChunkSize ->
    <<ChunkPart:Remain/binary, _/binary>> = Bin,
    {Chunk, Sign} = compute_chunk(ChunkPart, PrevSign, SignKey, NoHash, TS),
    gen_chunks(<<>>, Sign, <<Acc/binary, Chunk/binary>>, 0, ChunkSize, SignKey, NoHash, TS);
gen_chunks(Bin, PrevSign, Acc, Remain, ChunkSize, SignKey, NoHash, TS) ->
    <<ChunkPart:ChunkSize/binary, Rest/binary>> = Bin,
    {Chunk, Sign} = compute_chunk(ChunkPart, PrevSign, SignKey, NoHash, TS),
    gen_chunks(Rest, Sign, <<Acc/binary, Chunk/binary>>, Remain - ChunkSize, ChunkSize, SignKey, NoHash, TS).

compute_chunk(Bin, PrevSign, SignKey, NoHash, TS) ->
    DateTimeBin = list_to_binary(iso8601_date_format(TS)),
    {{Year, Month, Date},{_,_,_}} =
        calendar:universal_time_to_local_time(
          calendar:gregorian_seconds_to_datetime(TS)),
    DateBin = list_to_binary(lists:flatten(io_lib:format("~4..0w~2..0w~2..0w", [Year, Month, Date]))),
    SignHead = <<DateTimeBin/binary, "\n", DateBin/binary, "/us-east-1/s3/aws4_request\n">>,
    Size = byte_size(Bin),
    SizeHex = leo_hex:integer_to_hex(Size, 6),
    ChunkHashBin = case NoHash of
                       true ->
                           <<"chunk-dummy">>;
                       _ ->
                           leo_hex:binary_to_hexbin(crypto:hash(sha256, Bin))
                   end,
    BinToSign = <<"AWS4-HMAC-SHA256-PAYLOAD\n",
                  SignHead/binary,
                  PrevSign/binary,  "\n",
                  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                  ChunkHashBin/binary>>,
    Signature = crypto:hmac(sha256, SignKey, BinToSign),
    Sign = leo_hex:binary_to_hexbin(Signature),
    SizeHexBin = list_to_binary(SizeHex),
    Chunk = <<SizeHexBin/binary, ";", "chunk-signature=", Sign/binary, "\r\n",
              Bin/binary, "\r\n">>,
    {Chunk, Sign}.

%% do_post(Url, Headers, ValueGen) ->
%%     case send_request(Url, Headers ++ [{'Content-Type', 'application/octet-stream'}],
%%                       post, ValueGen(), [{response_format, binary}]) of
%%         {ok, "200", _Header, _Body} ->
%%             ok;
%%         {ok, "201", _Header, _Body} ->
%%             ok;
%%         {ok, "204", _Header, _Body} ->
%%             ok;
%%         {ok, Code, _Header, _Body} ->
%%             {error, {http_error, Code}};
%%         {error, Reason} ->
%%            {error, Reason}
%%     end.

do_delete(Url, RetryOnOL) ->
    TS = leo_date:now(),
    case send_request(Url, [{"date", leo_http:rfc1123_date(TS)}, {'Authorization', gen_sig("DELETE", Url, TS)}], delete, [], [{response_format, binary}]) of
        {ok, "404", _Headers, _Body} ->
            {not_found, Url};
        {ok, "204", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, "200", Headers, _Body} ->
            {ok, Url, Headers};
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> do_delete(Url, RetryOnOL) end, RetryOnOL);
        {ok, Code, _Headers, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

connect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            {ok, Pid} = ibrowse_http_client:start({Url#url.host, Url#url.port}),
            erlang:put({ibrowse_pid, Url#url.host}, Pid),
            Pid;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    erlang:erase({ibrowse_pid, Url#url.host}),
                    connect(Url)
            end
    end.


disconnect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase({ibrowse_pid, Url#url.host}),
    ok.

maybe_disconnect(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, Count} -> should_disconnect_ops(Count,Url) andalso disconnect(Url);
        Seconds -> should_disconnect_secs(Seconds,Url) andalso disconnect(Url)
    end.

should_disconnect_ops(Count, Url) ->
    Key = {ops_since_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, 1),
            false;
        Count ->
            erlang:put(Key, 0),
            true;
        Incr ->
            erlang:put(Key, Incr + 1),
            false
    end.

should_disconnect_secs(Seconds, Url) ->
    Key = {last_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, erlang:now()),
            false;
        Time when is_tuple(Time) andalso size(Time) == 3 ->
            Diff = timer:now_diff(erlang:now(), Time),
            if
                Diff >= Seconds * 1000000 ->
                    erlang:put(Key, erlang:now()),
                    true;
                true -> false
            end
    end.

clear_disconnect_freq(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, _Count} -> erlang:put({ops_since_disconnect, Url#url.host}, 0);
        _Seconds -> erlang:put({last_disconnect, Url#url.host}, erlang:now())
    end.

send_request(Url, Headers, Method, Body, Options) ->
    send_request(Url, Headers, Method, Body, Options, 3).

send_request(_Url, _Headers, _Method, _Body, _Options, 0) ->
    {error, max_retries};
send_request(Url, Headers, Method, Body, Options, Count) ->
    Pid = connect(Url),
    case catch(ibrowse_http_client:send_req(
                 Pid, Url, Headers, Method, Body, Options,
                 basho_bench_config:get(http_raw_request_timeout, 5000))) of

        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Url),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Url),
            disconnect(Url),
            case should_retry(Error) of
                true ->
                    send_request(Url, Headers, Method, Body, Options, Count-1);

                false ->
                    normalize_error(Method, Error)
            end
    end.

retry_handler(Handler, RetryOnOL) ->
    case RetryOnOL of
        true ->
            io:format("Server Overloaded, Sleep ~p ms and Retry~n", [?OL_SLEEP_INTERNVAL]),
            timer:sleep(?OL_SLEEP_INTERNVAL),
            Handler();
        _ ->
            {error, {http_error, "503"}}
    end.

should_retry({error, send_failed})       -> true;
should_retry({error, connection_closed}) -> true;
should_retry({'EXIT', {normal, _}})      -> true;
should_retry({'EXIT', {noproc, _}})      -> true;
should_retry(_)                          -> false.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.

%% Init MP
do_mp(Url, Headers, Size, PartSize, PartBin, RetryOnOL) ->
    TS = leo_date:now(),
    Url_2 = Url#url{ path = lists:concat([Url#url.path, "?uploads"])},
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", gen_sig("POST", Url_2, TS)}
                ],
    case send_request(Url_2, Headers ++ Headers_2,
                      post, <<>>, [{response_format, binary}]) of
        {ok, "200", _Header, Body} ->
            {Start, _} = binary:match(Body, <<"<UploadId>">>),
            {End, _} = binary:match(Body, <<"</UploadId>">>),
            UploadId = binary:part(Body, Start + 10, End - Start - 10),
            upload_parts(Url, Headers, binary_to_list(UploadId),
                         Size, PartSize, PartBin, RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

part_header(Url, UploadId, PartNum) ->
    Url_2 = Url#url{ path = lists:concat([Url#url.path, "?partNumber=",
                                          integer_to_list(PartNum),
                                          "&uploadId=",
                                          UploadId
                                         ])},
    TS = leo_date:now(),
    Headers = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", gen_sig("PUT", Url_2, TS)}
                ],
    {Url_2, Headers}.

upload_parts(Url, Headers, UploadId, Size, PartSize, PartBin, RetryOnOL) ->
    upload_parts(Url, Headers, UploadId, Size, PartSize, PartBin, RetryOnOL, 1).

upload_parts(Url, Headers, UploadId, Size, PartSize, PartBin, RetryOnOL, Acc)
  when Size > PartSize ->
    {Url_2, Headers_2} = part_header(Url, UploadId, Acc),
    case send_request(Url_2, Headers ++ Headers_2,
                       put, PartBin, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            upload_parts(Url, Headers, UploadId, Size - PartSize, PartSize, PartBin, RetryOnOL, Acc + 1);
        {ok, "204", _Header, _Body} ->
            upload_parts(Url, Headers, UploadId, Size - PartSize, PartSize, PartBin, RetryOnOL, Acc + 1);
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> upload_parts(Url, Headers, UploadId, Size, PartSize, PartBin, RetryOnOL, Acc) end, RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end;

upload_parts(Url, Headers, UploadId, Size, PartSize, PartBin, RetryOnOL, Acc) ->
    <<Bin:Size/binary, _/binary>> = PartBin,
    PartBinETag = erlang:md5(PartBin),

    {Url_2, Headers_2} = part_header(Url, UploadId, Acc),
    case send_request(Url_2, Headers ++ Headers_2,
                       put, Bin, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            complete_mp(Url, Headers, UploadId, RetryOnOL, Acc, PartBinETag, erlang:md5(Bin));
        {ok, "204", _Header, _Body} ->
            complete_mp(Url, Headers, UploadId, RetryOnOL, Acc, PartBinETag, erlang:md5(Bin));
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() -> upload_parts(Url, Headers, UploadId, Size, PartSize, PartBin, RetryOnOL, Acc) end, RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

complete_mp(Url, Headers, UploadId, RetryOnOL, Count, PartBinETag, LastETag) ->
    EntriesBin = gen_mp_entries(1, Count, PartBinETag, LastETag, <<>>),
    Bin = <<"<CompleteMultipartUpload>",
            EntriesBin/binary,
            "</CompleteMultipartUpload>">>,
    
    Url_2 = Url#url{ path = lists:concat([Url#url.path, 
                                          "?uploadId=",
                                          UploadId
                                         ])},
    TS = leo_date:now(),
    Headers_2 = [
                 {"Date", leo_http:rfc1123_date(TS)},
                 {"content-type", ?S3_CONTENT_TYPE},
                 {"authorization", gen_sig("POST", Url_2, TS)}
                ],
    case send_request(Url_2, Headers ++ Headers_2,
                       post, Bin, [{response_format, binary}]) of
        {ok, "200", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, "503", _Header, _Body} ->
            retry_handler(fun() ->  complete_mp(Url, Headers, UploadId, RetryOnOL, Count, PartBinETag, LastETag) end, RetryOnOL);
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

gen_mp_entries(Count, Count, _PartBinETag, LastETag, Acc) ->
    CountStr = integer_to_binary(Count),
    ETagBin = leo_hex:binary_to_hexbin(LastETag),
    Bin = <<"<Part>",
            "<PartNumber>", CountStr/binary, "</PartNumber>",
            "<ETag>", ETagBin/binary, "</ETag>",
            "</Part>">>,
    <<Acc/binary, Bin/binary>>;

gen_mp_entries(Cur, Count, PartBinETag, LastETag, Acc) ->
    CurStr = integer_to_binary(Cur),
    ETagBin = leo_hex:binary_to_hexbin(PartBinETag),
    Bin = <<"<Part>",
            "<PartNumber>", CurStr/binary, "</PartNumber>",
            "<ETag>", ETagBin/binary, "</ETag>",
            "</Part>">>,
    gen_mp_entries(Cur + 1, Count, PartBinETag, LastETag, <<Acc/binary, Bin/binary>>).
