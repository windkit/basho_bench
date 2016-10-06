-module(basho_bench_driver_s3cmd).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { hosts,
                 path,
                 s3cmd,
                 part_size_mb,
                 access_key,
                 secret_key,
                 tmp_path,
                 pick_groups }).

new(Id) ->
    Hosts = basho_bench_config:get(hosts,   ["192.168.100.35:8080"]),
    Path  = basho_bench_config:get(path,    "/test"),
    S3Cmd = basho_bench_config:get(s3cmd,   "~/s3cmd-1.6.1/s3cmd"),
    TmpPath    = basho_bench_config:get(tmp_path,       "/dev/shm"),
    PartSizeMB = basho_bench_config:get(part_size_mb,   5),
    AccessKey  = basho_bench_config:get(access_key,     "05236"),
    SecretKey  = basho_bench_config:get(secret_key,     "802562235"),
    SizeGroups = basho_bench_config:get(size_groups,    [{1, 1000000000}]),

    PickGroups = convert_size_groups_to_pick_groups(SizeGroups, 0, []),
    case Id of
        1 ->
            lists:foldl(fun(Ele, Count) ->
                                {_, FileSize} = Ele,
                                TmpFilePath = filename:join([TmpPath, "testfile_" ++ integer_to_list(Count)]),
                                {ok, TmpFile} = file:open(TmpFilePath, [write, binary]),
                                ok = file:allocate(TmpFile, 0, FileSize),
                                {ok, _} = file:position(TmpFile, FileSize),
                                ok = file:truncate(TmpFile),
                                ok = file:close(TmpFile),
                                Count + 1
                        end, 0, SizeGroups);
        _ ->
            void
    end,
    {ok, #state { hosts = Hosts,
                  path = Path,
                  s3cmd = S3Cmd,
                  part_size_mb = PartSizeMB,
                  access_key = AccessKey,
                  secret_key = SecretKey,
                  tmp_path = TmpPath,
                  pick_groups = PickGroups }}.

run(put, KeyGen, _ValueGen, #state{hosts = Hosts,
                                   path = Path,
                                   s3cmd = S3Cmd,
                                   part_size_mb = PartSizeMB,
                                   access_key = AccessKey,
                                   secret_key = SecretKey,
                                   tmp_path = TmpPath,
                                   pick_groups = PickGroups
                                  } = State) ->
    Key = KeyGen(),
    _ValueGen(),
    Host = pick_random(Hosts),
    {Pick, _PickSize} = pick_random(PickGroups),
    TmpFile = filename:join([TmpPath, "testfile_" ++ integer_to_list(Pick)]),
    S3Path = io_lib:format("s3://~s/~s", [Path, integer_to_list(Key)]),
    Cmd = io_lib:format("~s put ~s ~s --host=~s --access_key=~s --secret_key=~s --multipart-chunk-size-mb=~p",
                        [ S3Cmd,
                          TmpFile,
                          S3Path,
                          Host,
                          AccessKey,
                          SecretKey,
                          PartSizeMB ]),
    os:cmd(Cmd),

    {ok, State};

run(get, KeyGen, _ValueGen, #state{hosts = Hosts,
                                   path = Path,
                                   s3cmd = S3Cmd,
                                   access_key = AccessKey,
                                   secret_key = SecretKey
                                  } = State) ->
    Host = lists:nth(1, Hosts),
    Key = KeyGen(),
    _ValueGen(),
    S3Path = io_lib:format("s3://~s/~s", [Path, integer_to_list(Key)]),
    Cmd = io_lib:format("~s get ~s /dev/null --continue --host=~s --access_key=~s --secret_key=~s",
                        [ S3Cmd,
                          S3Path,
                          Host,
                          AccessKey,
                          SecretKey ]),
    os:cmd(Cmd),

    {ok, State}.

pick_random(List) ->
    Len = length(List),
    Pick = random:uniform(Len),
    lists:nth(Pick, List).

convert_size_groups_to_pick_groups([], _, Acc) ->
    lists:flatten(Acc);
convert_size_groups_to_pick_groups([{Weight, Size} | Rest], Count, Acc) ->
    List = lists:duplicate(Weight, {Count, Size}),
    convert_size_groups_to_pick_groups(Rest, Count + 1, [List | Acc]).
