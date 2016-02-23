-module(comm_utilities).
-include("commander.hrl").

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

get_full_name(Name, OpType) ->
    RootDir = get_home_dir() ++ "/commander",
    Dir = case OpType of
            recording ->
                RootDir ++ "/history/";
            _logging ->
                RootDir ++ "/log/"
          end,
    FullName = Dir ++ Name,
    ok = filelib:ensure_dir(FullName),
    FullName.