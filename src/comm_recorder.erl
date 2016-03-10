-module(comm_recorder).

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([do_record/2, init_record/2]).

%%%====================================
%%% Public API
%%%====================================
init_record(ExecId, State) ->
    FileName = comm_utilities:get_exec_name(ExecId),
    FullName = comm_utilities:get_full_name(FileName, recording),
    CurrExec = #execution{id=ExecId, trace=[]},
    if
        ExecId == 1 ->
            NewState = State#comm_state{initial_exec = CurrExec, curr_exec = CurrExec, curr_delay_seq=[], replay_history=[], phase=recording, exec_counter = ExecId, upstream_events = []};
        true ->
            NewState = State
    end,
    write_to_file(FullName, io_lib:format("~b~n", [ExecId]), write),
    NewState.

do_record(Event, State) ->
    ExecId = State#comm_state.exec_counter,
    FileName = comm_utilities:get_exec_name(ExecId),
    FullName = comm_utilities:get_full_name(FileName, recording),
    ok = write_to_file(FullName, io_lib:format("~w~n", [Event]), append),

    %% Update common parts of the commander state for both upstream and downstream events
    CurrExec = State#comm_state.curr_exec,
    CurrExecTrace = CurrExec#execution.trace,
    NewCurrExecTrace = CurrExecTrace ++ [Event],
    NewCurrExec = CurrExec#execution{trace = NewCurrExecTrace},
    if
        ExecId == 1 ->
            NewState = State#comm_state{initial_exec = NewCurrExec, curr_exec = NewCurrExec};
        true ->
            NewState = State#comm_state{curr_exec = NewCurrExec}
    end,
    NewState.

%%%====================================
%%% Internal functions
%%%====================================
write_to_file(FullName, Data, Mode) ->
    case file:read_file_info(FullName) of
            {ok, _FileInfo} ->
                file:write_file(FullName, Data, [Mode]),
                ok;
            {error, Reason} ->
                throw(io_lib:format("Exception: ~p", [{error, Reason}]))
    end.