-module(basho_bench_driver_image_conv).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { hosts,
                 resize_groups
               }).

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

            case basho_bench_config:get(http_use_ssl, false) of
                false ->
                    ok;
                _ ->
                    case ssl:start() of
                        ok ->
                            ok;
                        {error, {already_started, ssl}} ->
                            ok;
                        _ ->
                            ?FAIL_MSG("Unable to enable SSL support.\n", [])
                    end
            end,

            ok = application:ensure_started(inets),
            ok = application:ensure_started(ibrowse);
        
        _ ->
            void
    end,

    Hosts = basho_bench_config:get(hosts,   ["http://127.0.0.1:12345"]),
    ResizeGroups = basho_bench_config:get(resize_groups, ["240:240"]),

    {ok, #state { hosts = Hosts,
                  resize_groups = ResizeGroups }}.

run(get, KeyGen, _ValueGen, #state{hosts = Hosts,
                                   resize_groups = ResizeGroups} = State) ->
    Host = pick_random(Hosts),
    Resize = pick_random(ResizeGroups), 
    Key = KeyGen(),
    Url = lists:flatten(io_lib:format("~s/~p.jpg?fitin=~s", [Host, Key, Resize])),
    Ret = case httpc:request(get, {Url, []}, [], [{full_result, false}]) of
              {ok, {200, _}} ->
                  ok;
              {ok, {SC, _}} ->
                  {error, SC};
              Error ->
                  Error
          end,
%    Ret = case ibrowse:send_req(Url, [], get) of
%              {ok, "200", _, _} ->
%                  ok;  
%              {ok, SC, _, _} ->
%                  {error, SC};
%              Error ->
%                  Error
%          end,
    {Ret, State}.

pick_random(List) ->
    Len = length(List),
    Pick = random:uniform(Len),
    lists:nth(Pick, List).
