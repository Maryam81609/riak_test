-module(commander).

-behaviour(gen_server).

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Public API
-export([start_link/2,
        stop/0,
        update_upstream_event_data/1,
        get_upstream_event_data/1,
        get_downstream_event_data/1,
        update_transactions_data/2,
        get_scheduling_data/0,
        phase/0,
        get_clusters/1,
        check/2,
        run_next_test1/0,
        test_initialized/0,
        update_replay_txns_data/3,
        acknowledge_delivery/2,
        test_passed/0,
        passed_test_count/0,
        get_app_objects/2,
        display_result/0,
        write_time/2,
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
start_link(Scheduler, DelayDirection) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Scheduler, DelayDirection], []).

get_clusters(Clusters) ->
    gen_server:call(?SERVER, {get_clusters, Clusters}).

get_downstream_event_data(Data) ->
    gen_server:call(?SERVER, {get_downstream_event_data, {Data}}).

get_upstream_event_data(Data) ->
    gen_server:call(?SERVER, {get_upstream_event_data, {Data}}).

update_upstream_event_data(Data) ->
    gen_server:call(?SERVER, {update_upstream_event_data, {Data}}).

update_transactions_data(TxId, InterDcTxn) ->
    gen_server:call(?SERVER, {update_transactions_data, {TxId, InterDcTxn}}).

get_scheduling_data() ->
    gen_server:call(?SERVER, get_scheduling_data).

phase() ->
    gen_server:call(?SERVER, phase).

test_passed() ->
  gen_server:call(?SERVER, test_passed).

passed_test_count() ->
  gen_server:call(?SERVER, passed_test_count).

get_app_objects(Mod, Objs) ->
  gen_server:call(?SERVER, {get_app_objects, {Mod, Objs}}).

check(SchParam, Bound) ->
  gen_server:cast(?SERVER, {check, {SchParam, Bound}}).

run_next_test1() ->
  gen_server:cast(?SERVER, run_next_test1).

test_initialized() ->
  gen_server:cast(?SERVER, test_initialized).

update_replay_txns_data(LocalTxnData, InterDCTxn, TxId) ->
  gen_server:cast(?SERVER, {update_replay_txns_data, {LocalTxnData, InterDCTxn, TxId}}).

acknowledge_delivery(TxId, Timestamp) ->
  gen_server:cast(?SERVER, {acknowledge_delivery, {TxId, Timestamp}}).

display_result() ->
  gen_server:call(?SERVER, display_result).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%====================================
%%% Callbacks
%%%====================================
init([Scheduler, DelayDirection]) ->
    lager:info("Commander started on: ~p", [node()]),
    ExecId = 1,
    NewState = comm_recorder:init_record(ExecId, #comm_state{scheduler = Scheduler, delay_direction = DelayDirection}),
    {ok, NewState}.

handle_call(display_result, _From, State) ->
  Scheduler = State#comm_state.scheduler,
  display_check_result(Scheduler),
  {reply, ok, State};

handle_call({get_app_objects, {Mod, Objs}}, _From, State) ->
  {ok, _} = comm_verifier:start_link(Mod, Objs),
  {reply, ok, State};

handle_call(passed_test_count, _From, State) ->
  Scheduler = State#comm_state.scheduler,
  PassedTestCount = Scheduler:schedule_count(),
  {reply, PassedTestCount, State};

handle_call(test_passed, _From, State) ->
  {reply, ok, State#comm_state{phase=init_test}};

handle_call(get_scheduling_data, _From, State) ->
  {execution, 1, OrigSch} = State#comm_state.initial_exec,
  OrigSymSch = comm_utilities:get_symbolic_sch(OrigSch),
  {reply, OrigSymSch, State};

handle_call({get_clusters, Clusters}, _From, State) ->
    %%Only can get here in the initial run while setting the environment up
    NewState = State#comm_state{clusters = Clusters},
    {reply, ok, NewState};

handle_call(phase, _From, State) ->
    Phase = State#comm_state.phase,
    {reply, Phase, State};

%% TODO: handle the case, where the transaction has no update operation
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
    DepClockPrgm = State#comm_state.dep_clock_prgm, %% (none, [{st, DepClockPrgm}, {ct, unknown}]),
    {TxId, DCID, CommitTime, SnapshotTime, _Partition} = Data,
    NewState3 = case UpEvents of
        [CurrentUpstreamEvent | Tail] ->
            NewEventTxns = CurrentUpstreamEvent#upstream_event.event_txns ++ [TxId],
            NewUpstreamEvent = CurrentUpstreamEvent#upstream_event{event_dc = DCID, event_commit_time = CommitTime, event_snapshot_time = SnapshotTime, event_txns = NewEventTxns},

            NewState1 = comm_recorder:do_record(NewUpstreamEvent, State),

            NewTxnsData = dict:store(TxId, [{local, NewUpstreamEvent#upstream_event.event_data}, {remote, []}], NewState1#comm_state.txns_data),

            {ok, Val} = dict:find(none, DepClockPrgm),
            %% Sanity check
            F = dict:filter(fun(K, _V) -> K == none end, DepClockPrgm),
            1 = dict:size(F),
            DepClockPrgm1 = dict:erase(none, DepClockPrgm),
            %% Sanity check
            unknown = proplists:get_value(ct, Val),

            STPrgm = proplists:get_value(st, Val),
            NewSTPrgm =
              case STPrgm of
                ignore ->
                  dict:new();
                _Else ->
                  STPrgm
              end,
            Val1 = lists:keyreplace(st, 1, Val, {st, NewSTPrgm}),
            VC_CT = dict:store(DCID, CommitTime, NewSTPrgm),
            NewVal = lists:keyreplace(ct, 1, Val1, {ct, VC_CT}),
            NewDepClockPrgm = dict:store(TxId, NewVal, DepClockPrgm1),

            NewUpstreamEvents = Tail,
            NewState1#comm_state{upstream_events = NewUpstreamEvents, txns_data = NewTxnsData, dep_clock_prgm = NewDepClockPrgm};
        [] ->
            State
    end,
    {reply, ok, NewState3};

handle_call({get_upstream_event_data, {Data}}, _From, State) ->
    {_M, [EvNo | Tail ]} = Data,
    [EventNode, ClockPrgm, _] = Tail,
    NewUpstreamEvent = #upstream_event{event_no = EvNo, event_node = EventNode, event_data = Data, event_txns = []},
    DepClockPrgm = State#comm_state.dep_clock_prgm,
    NewDepClockPrgm = dict:store(none, [{st, ClockPrgm}, {ct, unknown}], DepClockPrgm),
    NewUpstreamEvents = State#comm_state.upstream_events ++ [NewUpstreamEvent],

    NewState = State#comm_state{upstream_events = NewUpstreamEvents, dep_clock_prgm = NewDepClockPrgm},
    {reply, ok, NewState};

handle_call({update_transactions_data, {TxId, InterDcTxn}}, _From, State) ->
    NewState = case dict:find(TxId, State#comm_state.txns_data) of
                    {ok, TxData} ->
                        [LocalData, {remote, PartTxns}] = TxData,
                        NewPartTxns = PartTxns ++ [InterDcTxn],
                        NewTxData = [LocalData, {remote, NewPartTxns}],
                        NewTxnsData2 = dict:store(TxId, NewTxData, State#comm_state.txns_data),
                        State#comm_state{txns_data = NewTxnsData2};
                    error ->
                        io:format("~nTXN:~p not found in txnsData!~n", [TxId]),
                        State
                end,
    {reply, ok, NewState}.

handle_cast({check,{SchParam, Bound}}, State) ->
  {execution, 1, OrigSch} = State#comm_state.initial_exec,
  Scheduler = State#comm_state.scheduler,
  DelayDirection = State#comm_state.delay_direction,
  ok = write_time(Scheduler, starting),

  %%% Extract transactions dependency
  NewDepTxnsPrgm = extract_txns_dependency(State#comm_state.dep_clock_prgm),

  KeysDepClock1 = dict:fetch_keys(State#comm_state.dep_clock_prgm),

  lists:foreach(fun(T) ->
                  {ok, Deps2} = dict:find(T, NewDepTxnsPrgm),
                  io:format("~n ==--==--==--== For T: ~w; Txn Deps: ~w ==--==--==--==~n", [T, Deps2])
                end, KeysDepClock1),

  %%% DCs list is obtainedS dynamically
  Clusters = State#comm_state.clusters,
  DCs = comm_utilities:get_all_dcs(Clusters),

  OrigSymSch = comm_utilities:get_det_sym_sch(OrigSch),
  TxnsData = State#comm_state.txns_data,
  Clusters = State#comm_state.clusters,
  comm_replayer:start_link(Scheduler, DelayDirection, SchParam, Bound, TxnsData, NewDepTxnsPrgm, Clusters, DCs, OrigSymSch),
  NewState = State#comm_state{phase = init_test, dep_txns_prgm = NewDepTxnsPrgm},
  comm_replayer:setup_next_test1(),
  {noreply, NewState};

handle_cast(test_initialized, State) ->
  NewState = State#comm_state{phase = replay},
  comm_replayer:replay_next_async(),
  {noreply, NewState};

handle_cast(run_next_test1, State) ->
  comm_replayer:setup_next_test1(),
  {noreply, State};

handle_cast({update_replay_txns_data, {LocalTxnData, InterDCTxn, TxId}}, State) ->
  ok = comm_replayer:update_txns_data(LocalTxnData, InterDCTxn, TxId),

  Scheduler = State#comm_state.scheduler,
  %%% Check application invariant
  CurrSch = Scheduler:curr_schedule(),
  LatestEvent = lists:last(CurrSch),
  TestRes = comm_verifier:check_object_invariant(LatestEvent),
  %%% If test result is true continue exploring more schedules; otherwise provide a counter example
  case TestRes of
    true ->
      comm_replayer:replay_next_async();
    {caught, Exception, Reason} ->
      display_counter_example(Scheduler, Exception, Reason)
  end,
  {noreply, State};

handle_cast({acknowledge_delivery, {_TxId, _Timestamp}}, State) ->
  Scheduler = State#comm_state.scheduler,
  %%% Check application invariant
  CurrSch = Scheduler:curr_schedule(),
  LatestEvent = lists:last(CurrSch),
  timer:sleep(1000),
  TestRes = comm_verifier:check_object_invariant(LatestEvent),
  %%% If test result is true continue exploring more schedules; otherwise provide a counter example
  case TestRes of
    true ->
      comm_replayer:replay_next_async();
    {caught, Exception, Reason} ->
      display_counter_example(Scheduler, Exception, Reason)
  end,
  {noreply, State};

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
display_check_result(Scheduler) ->
  ok = comm_utilities:write_to_file("schedules/result", "~n~n===========================Verification Result===========================~n~n", anything),
  ok = comm_utilities:write_to_file("schedules/result", io_lib:format("~nChecking completed after exploring ~p schedules.~n", [Scheduler:schedule_count()]), anything),
  io:format("~n~n===========================Verification Result===========================~n~n"),
  io:format("Checking completed after exploring ~p schedules.~n", [Scheduler:schedule_count()]),
  riak_test ! stop.

display_counter_example(Scheduler, Exception, Reason) ->
  ok = comm_utilities:write_to_file("schedules/result",
    "~n~n===========================Verification Result===========================~n~n", anything),
  ok = comm_utilities:write_to_file("schedules/result",
    io_lib:format("~nChecking failed after exploring ~p schedules, by exception: ~p~nWith reason: ~p~n",
      [Scheduler:schedule_count(), Exception, Reason]), anything),
  io:format("~n~n===========================Verification Result===========================~n~n"),
  io:format("Checking failed after exploring ~p schedules, by exception: ~p~nWith reason: ~p~n", [Scheduler:schedule_count(), Exception, Reason]),
  if
    Scheduler == comm_delay_scheduler ->
      ok = comm_utilities:write_to_file("schedules/result",
        io_lib:format("~nDelay sequence: ~s~n", [Scheduler:get_delay_sequence()]), anything),
      io:format("Delay sequence: "),
      Scheduler:print_delay_sequence();
    true ->
      noop
  end,
  ok = comm_utilities:write_to_file("schedules/result",
    "~n===========================Counter Example===========================", anything),
  io:format("~n===========================Counter Example==========================="),
  CounterExample = Scheduler:curr_schedule(),
  ok = comm_utilities:write_to_file("/counter_example/ce",
    io_lib:format("~n~w~nCE length: ~p~n", [CounterExample, length(CounterExample)]), anything),
  io:format("~nCounter example length (written to HOME/commander/schedules/result): ~p~n", [length(CounterExample)]),
%%  write_time(Scheduler, ending).,
  riak_test ! stop.

%%% Extract transactions dependency
extract_txns_dependency(DepClockPrgm) ->
  print_dict_of_dict(DepClockPrgm),
  NewDepTxnsPrgm =
    dict:fold(fun(TxId, [{st, ST}, {ct, _CT}], AllDepTxns) ->
                case dict:size(ST) of
                  0 ->
                    D1 = dict:store(TxId, [], AllDepTxns),
                    D1;
                  _Else ->
                    KeysDepClock = dict:fetch_keys(DepClockPrgm),
                    Dependencies =
                    lists:foldl(fun(T, DepTxns) ->
                                  {ok, [{st, _T_ST}, {ct, T_CT}]} = dict:find(T, DepClockPrgm),
                                  case (TxId /= T) and vectorclock:ge(ST, T_CT) of
                                    true ->
                                      DepTxns ++ [T];
                                    false ->
                                      DepTxns
                                  end
                                end, [], KeysDepClock),
                    D2 = dict:store(TxId, Dependencies, AllDepTxns),
                    D2
                end
              end, dict:new(), DepClockPrgm),

  print_dict(NewDepTxnsPrgm),
  NewDepTxnsPrgm.

print_dict(D) ->
  Keys = dict:fetch_keys(D),
  lists:foreach(fun(Key) ->
                  {ok, KDeps} = dict:find(Key, D),
                  io:format("~n~n!!!!!!! K: ~w; KDeps: ~w !!!!!!!~n~n", [Key, KDeps])
                end, Keys).

print_dict_of_dict(D) ->
  Keys = dict:fetch_keys(D),
  lists:foreach(fun(Key) ->
                  {ok, KDeps} = dict:find(Key, D),
                  [{st, ST}, {ct, CT}] = KDeps,
                  STLst = dict:to_list(ST),
                  CTLst = dict:to_list(CT),
                  io:format("~n#########Txn: ~w ### ~nST: ~w~n CT: ~w #########~n", [Key, STLst, CTLst])
                end, Keys).

write_time(_Scheduler, P) -> %% P: starting | ending
  comm_utilities:write_to_file("schedules/result", io_lib:format("~n~w:~w~n", [P, erlang:localtime()]), anything).