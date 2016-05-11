-module(comm_utilities).

-compile(export_all).

get_home_dir() ->
    {ok, [[HomeDir]]} = init:get_argument(home),
    HomeDir.

is_initial_exec(History) when is_list(History) ->
    case History of
        [] -> true;
        _ -> false
    end.

get_exec_name(ExecId) ->
    FileName = io_lib:format("exec_~b",[ExecId]),
    FileName.

get_full_name(Name, Phase) ->
    RootDir = get_home_dir() ++ "/commander",
    Dir = case Phase of
            record ->
                RootDir ++ "/history/";
            _logging ->
                RootDir ++ "/log/"
          end,
    FullName = Dir ++ Name,
    ok = filelib:ensure_dir(FullName),
    FullName.