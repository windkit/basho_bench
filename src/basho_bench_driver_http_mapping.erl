-module(basho_bench_driver_http_mapping).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { hosts,
                 resize_groups
               }).

-record(url, {abspath, host, port, username, password, path, protocol, host_type}).

new(Id) ->
    case Id of 
        1 ->
            %% Make sure ibrowse is available
            case code:which(ibrowse) of
                non_existing ->
                    ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
                _ ->
                    ok
            end,

            MappingFile = basho_bench_config:get(mapping, "/tmp/mapping.list"),

            {ok, File} = file:open(MappingFile, [read]),

            mapping_list = ets:new(mapping_list, [set, public, named_table, {read_concurrency, true}]),

            load_mapping(File),

            ok = application:ensure_started(ibrowse);        
        _ ->
            void
    end,

    hackney:start(),

    erlang:put(disconnect_freq, infinity),

    Hosts = basho_bench_config:get(hosts,   ["http://127.0.0.1:12345"]),
    ResizeGroups = basho_bench_config:get(resize_groups, ["240:240"]),

    {ok, #state { hosts = Hosts,
                  resize_groups = ResizeGroups }}.

load_mapping(File) ->
    load_mapping(File, io:get_line(File, ""), 0).

load_mapping(_File, {error, _Reason}, Cnt) ->
    Cnt;
load_mapping(_File, eof, Cnt) ->
    Cnt;
load_mapping(File, Data, Cnt) ->
    Data2 = lists:sublist(Data, length(Data) - 1),
    ets:insert(mapping_list, {Cnt, Data2}),
    load_mapping(File, io:get_line(File, ""), Cnt + 1).

run(get, KeyGen, _ValueGen, #state{hosts = Hosts,
                                   resize_groups = ResizeGroups} = State) ->
    Host = pick_random(Hosts),
    Resize = pick_random(ResizeGroups), 
    Key = KeyGen(),
    [{Key, Mapped}|_] = ets:lookup(mapping_list, Key),

    Url = lists:flatten(io_lib:format("~s/~s?fitin=~s", [Host, Mapped, Resize])),
    case do_get(Url) of
        ok ->
            {ok, State};
        {error ,Reason} ->
            {error, Reason, State}
    end.

do_get(Url) ->
%%    case hackney:request(get, Url, []) of
%%        {ok, 200, _, Cli} ->
%%            hackney:body(Cli),
%%            ok;
%%        {ok, SC, _, Cli} ->
%%            hackney:body(Cli),
%%            {error, {http_error, SC}};
%%        Error ->
%%            Error
%%    end.
    Url_Parse = ibrowse_lib:parse_url(Url),
    case send_request(Url_Parse, [], get, [], [{response_format, binary}]) of
        {ok, "200", _, _} ->
            ok;
        {ok, SC, _, _} ->
            {error, {http_error, SC}};
        {error, Reason} ->
            {error, Reason}
    end.

pick_random(List) ->
    Len = length(List),
    Pick = random:uniform(Len),
    lists:nth(Pick, List).

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

should_retry({error, send_failed})       -> true;
should_retry({error, connection_closed}) -> true;
should_retry({error, connection_closing})-> true;
should_retry({'EXIT', {normal, _}})      -> true;
should_retry({'EXIT', {noproc, _}})      -> true;
should_retry(_)                          -> false.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.
