-module(basho_bench_driver_layered_file).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { target_dir_path    :: list(), % target dir to issue read/write syscalls
                 total_num_file     :: integer(),
                 num_dir            :: integer(),
                 num_file_each      :: integer()}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    TargetDir = basho_bench_config:get(target_dir_path, ["./"]),
    NumDir = basho_bench_config:get(num_dir, 1),
    TotalNumFile = basho_bench_config:get(total_num_file, 10000),
    NumFileEach = TotalNumFile div NumDir + 1,
    filelib:ensure_dir(TargetDir),
    case Id of
        1 ->
            lists:foreach(fun(Ele) ->
                                  Path = filename:join([TargetDir, "dir" ++ integer_to_list(Ele), "dummy"]),
                                  filelib:ensure_dir(Path)
                          end, lists:seq(0,NumDir - 1));
        _ ->
            void
    end,
    {ok, #state{ target_dir_path = TargetDir,
                 total_num_file = TotalNumFile,
                 num_dir = NumDir,
                 num_file_each = NumFileEach}}.

run(read, KeyGen, _ValueGen, State) ->
    NextFile = next_file(KeyGen, State),
    case file:read_file(NextFile) of
        {ok, _Binary} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(write, KeyGen, ValueGen, State) ->
    NextFile = next_file(KeyGen, State),
%%    io:format("path: ~p~n", [NextFile]),
    case file:write_file(NextFile, ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

next_file(KeyGen, #state{ target_dir_path = TargetDir,
                          num_file_each = NumFileEach }) ->
    Key = KeyGen(),
    Dir = "dir" ++ integer_to_list(Key div NumFileEach),
    Obj = "obj" ++ integer_to_list(Key rem NumFileEach),
    filename:join([TargetDir, Dir, Obj]).

