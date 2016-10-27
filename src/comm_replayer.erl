%%%-------------------------------------------------------------------
%%% @author maryam
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. May 2016 8:34 AM
%%%-------------------------------------------------------------------
-module(comm_replayer).
-author("maryam").

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

%% API
-export([start_link/6,
  setup_next_test1/0,
  replay_next_async/0,
  update_txns_data/3,
%% gen_server callbacks
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Scheduler::atom(), DelayBound::non_neg_integer(), TxnsData::dict(), Clusters::list(), DCs::list(), OrigSymSch::list()) -> %%[]
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).

start_link(Scheduler, DelayBound, TxnsData, Clusters, DCs, OrigSymSch) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Scheduler, DelayBound, TxnsData, Clusters, DCs, OrigSymSch], []).

setup_next_test1() ->
  gen_server:cast(?SERVER, setup_next_test1).

replay_next_async() ->
  gen_server:cast(?SERVER, replay_next_async).

update_txns_data(LocalTxnData, InterDCTxn, TxId) ->
  gen_server:call(?SERVER, {update_txns_data, {LocalTxnData, InterDCTxn, TxId}}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #replay_state{}} | {ok, State :: #replay_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([Scheduler, DelayBound, TxnsData, Clusters, DCs, OrigSymSch]) ->
  TxIds = dict:fetch_keys(TxnsData),
  TxnMap = lists:foldl(fun(T, UpdatedTxnMap) ->
                            dict:store(T, T, UpdatedTxnMap)
                          end, dict:new(), TxIds),
  Scheduler:start_link([DelayBound, DCs, OrigSymSch]),
  State = #replay_state{scheduler = Scheduler, txns_data = TxnsData, txn_map = TxnMap, sch_count = 0, dcs = DCs, clusters = Clusters},
  {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #replay_state{}) ->
  {reply, Reply :: term(), NewState :: #replay_state{}} |
  {reply, Reply :: term(), NewState :: #replay_state{}, timeout() | hibernate} |
  {noreply, NewState :: #replay_state{}} |
  {noreply, NewState :: #replay_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #replay_state{}} |
  {stop, Reason :: term(), NewState :: #replay_state{}}).
handle_call({update_txns_data, {_EventData, InterDCTxn, TxId}}, _From, State) ->
  %%% Update txn_map
  TxnMap = State#replay_state.txn_map,
  [OrigTxId] = State#replay_state.latest_txids,
  {ok, PreReplayedTxId} = dict:find(OrigTxId, TxnMap),
  NewTxnMap = dict:store(OrigTxId, TxId, TxnMap),
  %%% Update txns_data
  TxnsData = State#replay_state.txns_data,
  {ok, [{local, LocalEventData}, _]} = dict:find(PreReplayedTxId, TxnsData),
  %%% TODO: Update the following to work for more than one partial transaction
  TxnsData1 = dict:erase(PreReplayedTxId, TxnsData),
  NewTxnsData = dict:store(TxId, [{local, LocalEventData}, {remote, [InterDCTxn]}], TxnsData1),
  NewState = State#replay_state{txn_map = NewTxnMap, txns_data = NewTxnsData},
  {reply, ok, NewState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #replay_state{}) ->
  {noreply, NewState :: #replay_state{}} |
  {noreply, NewState :: #replay_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #replay_state{}}).
handle_cast(replay_next_async, State) ->
  Scheduler = State#replay_state.scheduler,
  NewState = case Scheduler:is_end_current_schedule() of
                false ->
                  NextEvent = get_next_runnable_event(Scheduler),
                  replay(NextEvent, State);
                true ->
                  ok = commander:test_passed(),
                  commander:run_next_test1(),
                  State
              end,
  {noreply, NewState};

handle_cast(setup_next_test1, State) ->
  Scheduler = State#replay_state.scheduler,
  case Scheduler:has_next_schedule() of
    true ->
      comm_utilities:reset_dcs(State#replay_state.clusters),
      ok = Scheduler:setup_next_schedule(),
      commander:test_initialized();
    false ->
      commander:display_result(),
      riak_test!stop
  end,
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #replay_state{}) ->
  {noreply, NewState :: #replay_state{}} |
  {noreply, NewState :: #replay_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #replay_state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #replay_state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #replay_state{},
    Extra :: term()) ->
  {ok, NewState :: #replay_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_next_runnable_event(Scheduler) ->
  NextEvent = Scheduler:next_event(),
  case NextEvent of
    none -> get_next_runnable_event(Scheduler);
    _ -> NextEvent
  end.

-spec(replay(Event::remote_event(), State::#replay_state{}) -> #replay_state{}).
replay(Event, State) ->
  case comm_utilities:type(Event) of
    local ->
      replay(local, Event, State);
    remote ->
      replay(remote, Event, State)
  end.

replay(local, Event, State) ->
  TxnMap = State#replay_state.txn_map,
  TxnData = State#replay_state.txns_data,
  [OrigTxId] = Event#local_event.event_txns,
  {ok, TxId} = dict:find(OrigTxId, TxnMap),

  {ok, [{local, LTxnData}, _]} = dict:find(TxId, TxnData),
  {TestModule, Args} = LTxnData,
  TestModule:handle_event(Args),
  NewState = State#replay_state{latest_txids =[OrigTxId]},
  io:format("~n Replayed a local event. ~n"),
  NewState;

replay(remote, Event, State) ->
  TxnData = State#replay_state.txns_data, %%txId -> [{local, localData}, {remote,list(partialTxns)}]
  TxnMap = State#replay_state.txn_map,

  [PreTxId] = Event#remote_event.event_txns,
  EventNode = Event#remote_event.event_node,

  {ok, TxId} = dict:find(PreTxId, TxnMap),
  {ok, [_, {remote, PartialTxns}]} = dict:find(TxId, TxnData),

  ok = lists:foreach(fun(InterDcTxn) ->
                       ok = rpc:call(EventNode, inter_dc_sub_vnode, deliver_txn, [InterDcTxn])
                     end, PartialTxns),
  PT = hd(PartialTxns),
  NewTimestamp = PT#interdc_txn.timestamp,
  OriginalDCId = PT#interdc_txn.dcid,

  %%% Update clock on all partitions in the target DC
  Nodes = rpc:call(EventNode, dc_utilities, get_my_dc_nodes, []),
  lists:foreach(fun(Node) ->
                  Partitions = rpc:call(Node, dc_utilities, get_my_partitions, []),
                  lists:foreach(fun(Partition)->
                                  ok = rpc:call(Node, inter_dc_dep_vnode, update_partition_clock, [Partition, OriginalDCId,
                                    NewTimestamp])
                                end, Partitions)
                end, Nodes),
  %% TODO: sleep?????
  timer:sleep(1000),
  State.