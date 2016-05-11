-module(commander).

-behaviour(gen_server).

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([start_link/0,
        stop/0,
        update_upstream_event_data/1,
        get_upstream_event_data/1,
        get_downstream_event_data/1,
        phase/0,
        get_clusters/1,
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

get_clusters(Clusters) ->
    gen_server:call(?SERVER, {get_clusters, Clusters}).

get_downstream_event_data(Data) ->
    gen_server:call(?SERVER, {get_downstream_event_data, {Data}}).

get_upstream_event_data(Data) ->
    gen_server:call(?SERVER, {get_upstream_event_data, {Data}}).

update_upstream_event_data(Data) ->
    gen_server:call(?SERVER, {update_upstream_event_data, {Data}}).

phase() ->
    gen_server:call(?SERVER, phase).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%====================================
%%% Callbacks
%%%====================================
init([]) ->
    lager:info("commander server started on node: ~p", [node()]),
    ExecId = 1,
    NewState = comm_recorder:init_record(ExecId, #comm_state{}),
    io:format("Recording initiated....~n~p~n", [NewState]),
    {ok, NewState}.

handle_call({get_clusters, Clusters}, _From, State) ->
    %%Only can get here in get_clusters from the initial run when setting the environment up
    NewState = State#comm_state{clusters = Clusters},
    {reply, ok, NewState};

handle_call(phase, _From, State) ->
    Phase = State#comm_state.phase,
    {reply, Phase, State};

handle_call({get_downstream_event_data, {Data}}, _From, State) ->
    {EventDc, EventNode, EventTxn} = Data,

    EventOriginalDc = EventTxn#interdc_txn.dcid,
    EventCommitTime = EventTxn#interdc_txn.timestamp,
    EventSnapshot = EventTxn#interdc_txn.snapshot,
    EventData = [EventTxn],

    Ops = EventTxn#interdc_txn.operations,
    Commit_op = lists:last(Ops),
    LogRecord = Commit_op#operation.payload,
    TxnId = LogRecord#log_record.tx_id,

    EventTxns = [TxnId],

    NewDownstreamEvent = #downstream_event{event_dc = EventDc, event_node = EventNode, event_original_dc = EventOriginalDc, event_commit_time = EventCommitTime, event_snapshot_time = EventSnapshot, event_data = EventData, event_txns = EventTxns},

    NewState1 = comm_recorder:do_record(NewDownstreamEvent, State),
    {reply, ok, NewState1};

%% {TxId, DCID, CommitTime, SnapshotTime} = Data
handle_call({update_upstream_event_data, {Data}}, _From, State) ->
    %% TODO: Handle the case if the txn is aborted or there is an error (Data =/= {txnid, _})
    UpEvents = State#comm_state.upstream_events,
    case UpEvents of
        [CurrentUpstreamEvent | Tail] ->
            {TxId, DCID, CommitTime, SnapshotTime, _Partition} = Data,
            NewEventTxns = CurrentUpstreamEvent#upstream_event.event_txns ++ [TxId],
            NewUpstreamEvent = CurrentUpstreamEvent#upstream_event{event_dc = DCID, event_commit_time = CommitTime, event_snapshot_time = SnapshotTime, event_txns = NewEventTxns},

            NewState1 = comm_recorder:do_record(NewUpstreamEvent, State),

            NewUpstreamEvents = Tail,
            NewState = NewState1#comm_state{upstream_events = NewUpstreamEvents},
            {reply, ok, NewState};
        [] ->
            {reply, ok, State}
    end;

handle_call({get_upstream_event_data, {Data}}, _From, State) ->
    {_M, [EvNo | _ ]} = Data,
    NewUpstreamEvent = #upstream_event{event_no = EvNo, event_data = Data, event_txns = []},
    NewUpstreamEvents = State#comm_state.upstream_events ++ [NewUpstreamEvent],
    NewState = State#comm_state{upstream_events = NewUpstreamEvents},
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
