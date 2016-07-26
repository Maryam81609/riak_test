%%%-------------------------------------------------------------------
%%% @author maryam
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. May 2016 11:06 AM
%%%-------------------------------------------------------------------
-module(comm_scheduler).
-author("maryam").

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
  has_next_schedule/0,
  setup_next_schedule/0,
  next_event/0,
  is_end_current_schedule/0,
  print_curr_event/0,
  schedule_count/0,
  curr_schedule/0,

  stop/0
  ]).

%% gen_server callbacks
-export([init/1,
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
-spec(start_link(DelayBound::non_neg_integer()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link([DelayBound, DCs, OrigSymSch]) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [[DelayBound, DCs, OrigSymSch]], []).

schedule_count() ->
  gen_server:call(?SERVER, schedule_count).

has_next_schedule() ->
  gen_server:call(?SERVER, has_next_schedule).

next_event() ->
  gen_server:call(?SERVER, next_event).

setup_next_schedule() ->
  gen_server:call(?SERVER, setup_next_schedule).

is_end_current_schedule() ->
  gen_server:call(?SERVER, is_end_current_schedule).

print_curr_event() ->
  gen_server:call(?SERVER, print_curr_event).

curr_schedule() ->
  gen_server:call(?SERVER, curr_schedule).

stop() ->
  gen_server:cast(?SERVER, stop).
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
  {ok, State :: #scheduler_state{}} | {ok, State :: #scheduler_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([[DelayBound, DCs, OrigSymSch]]) ->
  comm_delay_sequence:start_link(DelayBound, length(OrigSymSch)),
  EventCount = length(OrigSymSch),

  InitState = #scheduler_state{delay_bound = DelayBound, event_count = EventCount, orig_sch_sym = OrigSymSch, dcs = DCs, schedule_count = 0},
  {ok, InitState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #scheduler_state{}) ->
  {reply, Reply :: term(), NewState :: #scheduler_state{}} |
  {reply, Reply :: term(), NewState :: #scheduler_state{}, timeout() | hibernate} |
  {noreply, NewState :: #scheduler_state{}} |
  {noreply, NewState :: #scheduler_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #scheduler_state{}} |
  {stop, Reason :: term(), NewState :: #scheduler_state{}}).
handle_call(has_next_schedule, _From, State) ->
  SchCnt = State#scheduler_state.schedule_count,
  Reply = comm_delay_sequence:has_next() orelse (SchCnt == 0),
  {reply, Reply, State};

handle_call(curr_schedule, _From, State) ->
  CurrSch = State#scheduler_state.curr_sch,
  {reply, CurrSch, State};

handle_call(schedule_count, _From, State) ->
  SchCnt = State#scheduler_state.schedule_count,
  {reply, SchCnt, State};

handle_call(setup_next_schedule, _From, State) ->

  {OrigSchSym, _DCs} = commander:get_scheduling_data(),
  EventCount = length(OrigSchSym),

  DelaySeq = comm_delay_sequence:next(),
  DCs = State#scheduler_state.dcs,
  %%% Set stable snapshot in all DCs to 0
  InitSS = lists:foldl(fun(Dc, SS1) -> dict:store(Dc, 0, SS1) end, dict:new(), DCs),
  LogicalSS = lists:foldl(fun(Dc, LSS1) -> dict:store(Dc, InitSS, LSS1) end, dict:new(), DCs),
  SchCnt = State#scheduler_state.schedule_count,
  InitState = State#scheduler_state{orig_sch_sym = OrigSchSym, curr_delay_seq = DelaySeq, curr_sch = [], dependency = dict:new(),
    delayed = [], curr_event_index = 0, logical_ss = LogicalSS, delayed_count = 0, event_count = EventCount, schedule_count = SchCnt+1},
  {reply, ok, InitState};

handle_call(next_event, _From, State) ->
  OrigSymSch = State#scheduler_state.orig_sch_sym,
  CurrSch = State#scheduler_state.curr_sch,
  SS = State#scheduler_state.logical_ss,
  EventsCount = State#scheduler_state.event_count,
  NewCurrEventIndex = State#scheduler_state.curr_event_index + 1,
  CurrEvent = lists:nth(NewCurrEventIndex, OrigSymSch),
  NextDelIndex = comm_delay_sequence:get_next_delay_index(),

  NewOrigSch = update_event_data(CurrEvent, OrigSymSch, SS), %% Updates ST and CT of the local CurrEvent and its replications
  CurrEvent1 = lists:nth(NewCurrEventIndex, NewOrigSch),

  io:format("~nDelayed:~p~nNext Del Index:~p~nCurrSch length:~p~nTotalEventsCount:~p~n", [State#scheduler_state.delayed, NextDelIndex, length(CurrSch), EventsCount]),

  {NewState1, NewCurrEvent} = case type(CurrEvent) of
                              local ->
                                if
                                  NewCurrEventIndex == NextDelIndex ->
                                    %%% Delay current event
                                    Delayed = State#scheduler_state.delayed,
                                    NewDelayed = Delayed ++ [CurrEvent1],
                                    comm_delay_sequence:spend_current_delay_index(),

                                    State3 = State#scheduler_state{orig_sch_sym = NewOrigSch , curr_event_index = NewCurrEventIndex, delayed = NewDelayed}, %% dependency = NewDep,
                                    {State3, none};
                                  true ->
                                    NewCurrSch = CurrSch ++ [CurrEvent1],

                                    %%% Update SS %%%
                                    EventDC = CurrEvent1#local_event.event_dc,
                                    NewEventCT = CurrEvent1#local_event.event_commit_time,
                                    {ok, DCSS} = dict:find(EventDC, SS),
                                    NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
                                    NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
                                    %%% End of update SS %%%

                                    State1 = State#scheduler_state{orig_sch_sym = NewOrigSch ,curr_sch = NewCurrSch, curr_event_index = NewCurrEventIndex,
                                      logical_ss = NewLogicalSS1},
                                    {State1, CurrEvent1}
                                end;
                              remote ->
                                Delayed = State#scheduler_state.delayed,
                                EventDC2 = CurrEvent1#remote_event.event_dc,
                                {ok, DCSS2} = dict:find(EventDC2, SS),
                                DepSatisfied =  vectorclock:ge(DCSS2, CurrEvent1#remote_event.event_snapshot_time),
                                %%% Check if is replication
                                IsRepl = lists:any(fun(E) ->
                                                    case type(E) of
                                                      local ->
                                                        CurrEvent1#remote_event.event_txns == E#local_event.event_txns;
                                                      _ ->
                                                        false
                                                    end
                                                   end, Delayed),

                                if
                                  NewCurrEventIndex == NextDelIndex ->
                                    %%% Delay current event
                                    NewDelayed = Delayed ++ [CurrEvent1],
                                    comm_delay_sequence:spend_current_delay_index(),

                                    State4 = State#scheduler_state{orig_sch_sym = NewOrigSch , curr_event_index = NewCurrEventIndex, delayed = NewDelayed},
                                    {State4, none};
                                  not DepSatisfied or IsRepl ->
%%                                    io:format("~nDepSatisfied:~p~nIsRepl:~p~n", [DepSatisfied, IsRepl]),
                                    %%% Delay current event
                                    Delayed1 = State#scheduler_state.delayed,
                                    NewDelayed1 = Delayed1 ++ [CurrEvent1],

                                    State5 = State#scheduler_state{orig_sch_sym = NewOrigSch , curr_event_index = NewCurrEventIndex, delayed = NewDelayed1},
                                    {State5, none};
                                  true ->
                                    NewCurrSch = CurrSch ++ [CurrEvent1],

                                    %%% Update SS
                                    NewEventCT2 = CurrEvent1#remote_event.event_commit_time,
                                    EventOrigDC = CurrEvent1#remote_event.event_original_dc,
                                    NewDCSS2 = dict:store(EventOrigDC, NewEventCT2, DCSS2),
                                    NewLogicalSS2 = dict:store(EventDC2, NewDCSS2, SS),
                                    %%% End of update SS

                                    State2 = State#scheduler_state{orig_sch_sym = NewOrigSch ,curr_sch = NewCurrSch, curr_event_index = NewCurrEventIndex,
                                      logical_ss = NewLogicalSS2},
                                    {State2, CurrEvent1}
                                end
                              end,
  NewState = case NewState1#scheduler_state.curr_event_index of
               EventsCount ->
                  Delayed3 = NewState1#scheduler_state.delayed,
                  NewState1#scheduler_state{orig_sch_sym = Delayed3, delayed = [], curr_event_index = 0};
                _ ->
                  NewState1
              end,
  {reply, NewCurrEvent, NewState};

handle_call(is_end_current_schedule, _From, State) ->
  OrigSch = State#scheduler_state.orig_sch_sym,
  CurrEvIndex = State#scheduler_state.curr_event_index,
  DelayedEvents = State#scheduler_state.delayed,
  Reply = (CurrEvIndex == length(OrigSch)) and (DelayedEvents == []),
  {reply, Reply, State};

handle_call(print_curr_event, _From, State) ->
  CurrSch = State#scheduler_state.curr_sch,
  LSS = State#scheduler_state.logical_ss,
  CurrEvent = lists:last(CurrSch),
  EvCount = length(CurrSch),
  case type(CurrEvent) of
    local ->
      io:format("~n==================~nCurrent schedule length: ~p~nEvent no: ~p~nEvent DC: ~p~nEvent commit time: ~p~nEvent snapshot time: ~p~nEvent txns: ~p~n=================", [EvCount, CurrEvent#local_event.event_no,CurrEvent#local_event.event_dc, CurrEvent#local_event.event_commit_time, dict:to_list(CurrEvent#local_event.event_snapshot_time), CurrEvent#local_event.event_txns]),
      {ok, DCSS} = dict:find(CurrEvent#local_event.event_dc, LSS),
      io:format("~nCurrent DC ss: ~p~n", [dict:to_list(DCSS)]);
    remote ->
      io:format("~n==================~nCurrent schedule length: ~p~nEvent DC: ~p~nEvent node: ~p~nEvent original dc: ~p~nEvent commit time: ~p~nEvent snapshot time: ~p~nEvent txns: ~p~n=================", [EvCount, CurrEvent#remote_event.event_dc,CurrEvent#remote_event.event_node,CurrEvent#remote_event.event_original_dc, CurrEvent#remote_event.event_commit_time, dict:to_list(CurrEvent#remote_event.event_snapshot_time), CurrEvent#remote_event.event_txns]),
      {ok, DCSS2} = dict:find(CurrEvent#remote_event.event_dc, LSS),
      io:format("~nCurrent DC ss: ~p~n", [dict:to_list(DCSS2)])
  end,
  {reply, ok, State}.
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #scheduler_state{}) ->
  {noreply, NewState :: #scheduler_state{}} |
  {noreply, NewState :: #scheduler_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #scheduler_state{}}).
handle_cast(stop, State) ->
  {stop, normal, State}.

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
-spec(handle_info(Info :: timeout() | term(), State :: #scheduler_state{}) ->
  {noreply, NewState :: #scheduler_state{}} |
  {noreply, NewState :: #scheduler_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #scheduler_state{}}).
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
    State :: #scheduler_state{}) -> term()).
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #scheduler_state{},
    Extra :: term()) ->
  {ok, NewState :: #scheduler_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
update_event_data(CurrEvent, OrigSymSch, SS) ->
  NewOrigSch = case type(CurrEvent) of
                 local ->
                   [CurrEventTxnId] = CurrEvent#local_event.event_txns,
                   EventDC = CurrEvent#local_event.event_dc,
                   {ok, DCSS} = dict:find(EventDC, SS),
                   {ok, V1} = dict:find(EventDC, DCSS),
                   NewEventCT = V1 + 1,
                   lists:map(fun(E) ->
                               case type(E) of
                                 local ->
                                   [ETxId] = E#local_event.event_txns,
                                   if
                                     CurrEventTxnId == ETxId ->
                                       E#local_event{event_snapshot_time = DCSS, event_commit_time = NewEventCT};
                                     true ->
                                       E
                                   end;
                                 remote ->
                                   [ETxId] = E#remote_event.event_txns,
                                   if
                                     CurrEventTxnId == ETxId ->
                                       E#remote_event{event_snapshot_time = DCSS, event_commit_time = NewEventCT};
                                     true ->
                                       E
                                   end
                               end
                             end, OrigSymSch);
                 remote -> OrigSymSch
               end,
  NewOrigSch.

type(Event) ->
  if
    is_record(Event, local_event) -> local;
    is_record(Event, remote_event) -> remote
  end.