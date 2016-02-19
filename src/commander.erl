-module(commander).

-behaviour(gen_server).

%-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([test/0,
        start_link/0,
        stop/0,
        call_comm/0,
        do_record/1,
        %% callbacks
        init/1,
		handle_cast/2,
		handle_call/3,
		handle_info/2,
		code_change/3,
		terminate/2
		]).

-define(SERVER, ?MODULE).
-record(state, {}).

%%%====================================
%%% Public API
%%%====================================
test() ->
    returned.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

do_record(DS_Txn) ->
    gen_server:call(?SERVER, {do_record, {DS_Txn}}).

call_comm() ->
	Res = gen_server:call(?SERVER, call_comm),
	io:format("commander server was called from riak_test, and the result is: ~p", [Res]),
	Res.

stop() ->
    gen_server:cast(?SERVER, stop).

%%%====================================
%%% Callbacks
%%%====================================
init([]) ->
    io:format("commander server started on node: ~p", [node()]),
    {ok, #state{}, 0}.

log_fn() ->
 	{ok, [[HomeDir]]} = init:get_argument(home),
 	LogPath = HomeDir ++ "/commander/log/",
 	filelib:ensure_dir(LogPath),
 	LogPath ++ "comm_log".

handle_call({do_record, {_Txn}}, From, State) ->
    %% TODO: call appropriate functions from recorder module
    case file:open(log_fn(), [write]) of
    	{ok, IoDevice} ->
    		file:truncate(IoDevice),
    		file:write(IoDevice, io_lib:format("~p -----> ~s~n", [From, "HHHHHHHHAAAAAAAAAAA.........."])),
            {reply, {recorded, IoDevice}, State};
    	{error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(call_comm, _From, State) ->
    {reply, comm_called, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%%====================================
%%% Internal functions
%%%====================================
