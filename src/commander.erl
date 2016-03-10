-module(commander).

-behaviour(gen_server).

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([start_link/0,
        stop/0,
        do_record/1,
        update_upstream_event_data/1,
        get_upstream_event_data/1,
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

get_upstream_event_data(Data) ->
    gen_server:call(?SERVER, {get_upstream_event_data, {Data}}).

update_upstream_event_data(Data) ->
    gen_server:call(?SERVER, {update_upstream_event_data, {Data}}).

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

%% {TxId, DCID, CommitTime, SnapshotTime} = Data
handle_call({update_upstream_event_data, {Data}}, _From, State) ->
    %% TODO: Handle the case if the txn is aborted or there is an error (Data =/= {txnid, _})

    [CurrentUpstreamEvent | Tail] = State#comm_state.upstream_events,

    {TxId, DCID, CommitTime, SnapshotTime} = Data,
    NewEventTxns = CurrentUpstreamEvent#upstream_event.event_txns ++ [TxId],
    NewUpstreamEvent = CurrentUpstreamEvent#upstream_event{event_dc = DCID, event_commit_time = CommitTime, event_snapshot_time = SnapshotTime, event_txns = NewEventTxns},

    NewState1 = comm_recorder:do_record(NewUpstreamEvent, State),

    NewUpstreamEvents = Tail,
    NewState = NewState1#comm_state{upstream_events = NewUpstreamEvents},
    {reply, ok, NewState};

handle_call({get_upstream_event_data, {Data}}, _From, State) ->
    {_M, [EvNo | _ ]} = Data,
    NewUpstreamEvent = #upstream_event{event_no = EvNo, event_data = Data, event_txns = []},
    NewUpstreamEvents = State#comm_state.upstream_events ++ [NewUpstreamEvent],
    NewState = State#comm_state{upstream_events = NewUpstreamEvents},
    {reply, ok, NewState};

handle_call({do_record, {Data}}, _From, State) ->
    NewState = comm_recorder:do_record(Data, State),
    {reply, ok, NewState}.

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
