-module(commander).

-behaviour(gen_server).

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([start_link/0,
        stop/0,
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

%%%====================================
%%% Public API
%%%====================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

do_record(Data) ->
    gen_server:call(?SERVER, {do_record, {Data}}).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%====================================
%%% Callbacks
%%%====================================
init([]) ->
    lager:info("commander server started on node: ~p", [node()]),
    ExecId = 1,
    NewState = comm_recorder:init_record(ExecId, #comm_state{}),
    lager:info("Recording initiated....~n~p~n", [NewState]),
    {ok, NewState}.

handle_call({do_record, {Data}}, _From, State) ->
    NewState = comm_recorder:do_record(Data, State),
    {reply, ok, NewState};

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
