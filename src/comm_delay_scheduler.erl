-module(comm_delay_scheduler).

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
  print_delay_sequence/0,
  curr_state/0,

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
start_link([DelayBound, Bound, _DepTxnsPrgm ,DCs, OrigSymSch]) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [[DelayBound, Bound, DCs, OrigSymSch]], []).

has_next_schedule() ->
  gen_server:call(?SERVER, has_next_schedule).

setup_next_schedule() ->
  gen_server:call(?SERVER, setup_next_schedule).

next_event() ->
  gen_server:call(?SERVER, next_event).

is_end_current_schedule() ->
  gen_server:call(?SERVER, is_end_current_schedule).

schedule_count() ->
  gen_server:call(?SERVER, schedule_count).

curr_schedule() ->
  gen_server:call(?SERVER, curr_schedule).

curr_state() ->
  gen_server:call(?SERVER, curr_state).

print_curr_event() ->
  gen_server:call(?SERVER, print_curr_event).

print_delay_sequence() ->
  gen_server:call(?SERVER, print_delay_sequence).

stop() ->
  gen_server:cast(?SERVER, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([[DelayBound, Bound, DCs, OrigSymSch]]) ->
  EventCount = length(OrigSymSch),
  InitState = #delay_schlr_state{
    event_count_total = EventCount,
    orig_sch_sym_main = OrigSymSch,
    orig_sch_sym = OrigSymSch,
    dcs = DCs,
    schedule_count = 0,
    curr_sch = [],
    bound = Bound,
    common_prfx_bound = Bound,
    common_prfx_schl_cnt = 0,
    orig_event_index = 0,
    delay_bound = DelayBound,
    delayed = [],
    delayed_main = [],
    delayer = regular},

  %% Start regular delay sequence generator
  comm_delay_sequence:start_link(comm_delay_sequence_r, DelayBound, length(OrigSymSch)),

  {ok, InitState}.

handle_call(print_delay_sequence, _From, State) ->
  Delayer = State#delay_schlr_state.delayer,
  case Delayer of
    regular ->
      io:format("regular, ~w~n", [State#delay_schlr_state.curr_delay_seq]);
    delay ->
      io:format("regular, ~w; delay, ~w~n", [State#delay_schlr_state.curr_delay_seq, State#delay_schlr_state.curr_delayed_delay_seq])
  end,
  {reply, ok, State};

handle_call(curr_state, _From, State) ->
  {reply, State, State};

handle_call(has_next_schedule, _From, State) ->
  SchCnt = State#delay_schlr_state.schedule_count,
  Bound = State#delay_schlr_state.bound,
  CommonPrfxSchlCnt = State#delay_schlr_state.common_prfx_schl_cnt,
  CommonPrfxBound = State#delay_schlr_state.common_prfx_bound,
  DelayedMain = State#delay_schlr_state.delayed_main,
  Delayer = State#delay_schlr_state.delayer,

  HasNextSch =
    case Delayer of
      regular ->
        length(DelayedMain) > 1
          orelse (SchCnt < Bound andalso comm_delay_sequence:has_next(comm_delay_sequence_r))
          orelse SchCnt == 0;
      delay ->
        (CommonPrfxSchlCnt < CommonPrfxBound andalso comm_delay_sequence:has_next(comm_delay_sequence_d))
          orelse (SchCnt < Bound andalso comm_delay_sequence:has_next(comm_delay_sequence_r))
    end,
  {reply, HasNextSch, State};

handle_call(setup_next_schedule, _From, State) ->

  CurrSch = State#delay_schlr_state.curr_sch,
  SchCnt = State#delay_schlr_state.schedule_count,
  CommonPrfxSchlCnt = State#delay_schlr_state.common_prfx_schl_cnt,

  ok = comm_utilities:write_to_file(io_lib:format("~w", [SchCnt-CommonPrfxSchlCnt]), io_lib:format("~n~w~n", [CurrSch]), anything),

  DCs = State#delay_schlr_state.dcs,
%%  SchCnt = State#delay_schlr_state.schedule_count,
%%  CommonPrfxSchlCnt = State#delay_schlr_state.common_prfx_schl_cnt,
  %%CommonPrfxBound = State#delay_schlr_state.common_prfx_bound,
  OrigSchMain = State#delay_schlr_state.orig_sch_sym_main,
  DelayedMain = State#delay_schlr_state.delayed_main,
  Delayer = State#delay_schlr_state.delayer,
  DB = State#delay_schlr_state.delay_bound,

  %%% Set stable snapshot in all DCs to 0
  InitSS = lists:foldl(fun(Dc, SS1) ->
                          dict:store(Dc, 0, SS1)
                       end, dict:new(), DCs),
  LogicalSS = lists:foldl(fun(Dc, LSS1) ->
                            dict:store(Dc, InitSS, LSS1)
                          end, dict:new(), DCs),
  NewDelayer =
    case Delayer of
      regular ->
        if
          length(DelayedMain) =< 1 ->
            regular;
          true ->
            DB1 = min(DB, length(DelayedMain) - 1),
            comm_delay_sequence:start_link(comm_delay_sequence_d, DB1, length(DelayedMain)),
            delay
        end;
      delay ->
        HasNextDeley = comm_delay_sequence:has_next(comm_delay_sequence_d),
        if
          HasNextDeley ->
            delay;
          true ->
            %%% Sanity Check
            true = comm_delay_sequence:has_next(comm_delay_sequence_r),
            comm_delay_sequence:stop(comm_delay_sequence_d),
            regular
        end
    end,

  NewState =
    case NewDelayer of
      regular ->
        CurrDelSeq = comm_delay_sequence:next(comm_delay_sequence_r),
        State#delay_schlr_state{
          orig_sch_sym = OrigSchMain,
          orig_event_index = 0,
          curr_delay_seq = CurrDelSeq,
          common_prfx_schl = [],
          common_prfx_schl_cnt = 0,
          delayed_main = [],
          delayed = [],
          delayer = regular}; %% %%delayed_count = 0, %%
      delay ->
        CurrDelayedDelSeq = comm_delay_sequence:next(comm_delay_sequence_d),
        DelayedMain = State#delay_schlr_state.delayed_main,
        State#delay_schlr_state{
          curr_delayed_delay_seq = CurrDelayedDelSeq,
          delayed = DelayedMain,
          delayed_event_index = 0,
          delayed2 = [],
          common_prefix_event_index = 0,
          common_prfx_schl_cnt = CommonPrfxSchlCnt + 1,
          delayer = delay}
    end,

  InitState = NewState#delay_schlr_state{
    curr_sch = [],
    logical_ss = LogicalSS,
    schedule_count = SchCnt+1}, %% common_prefix_event_index = 0,

  {reply, ok, InitState};

handle_call(next_event, _From, State) ->
  Delayer = State#delay_schlr_state.delayer,

  {NewState, NewCurrEvent} = next_event(Delayer, State),
  {reply, NewCurrEvent, NewState};

handle_call(is_end_current_schedule, _From, State) ->
  OrigSchSym = State#delay_schlr_state.orig_sch_sym,
  OrigEvIndex = State#delay_schlr_state.orig_event_index,
  CommonPrfxSchl = State#delay_schlr_state.common_prfx_schl,
  CommonPrfxEvIndex = State#delay_schlr_state.common_prefix_event_index,
  Delayed = State#delay_schlr_state.delayed,
  DelayedEventIndex = State#delay_schlr_state.delayed_event_index,
  Delayed2 = State#delay_schlr_state.delayed2,
  Delayer = State#delay_schlr_state.delayer,

  IsEnd =
    case Delayer of
      regular ->
        (OrigEvIndex == length(OrigSchSym)) and (Delayed == []); %%(DelayedEventIndex >= length(Delayed));
      delay ->
        (CommonPrfxEvIndex >= length(CommonPrfxSchl)) and (DelayedEventIndex == length(Delayed)) and (Delayed2 == [])
    end,
  {reply, IsEnd, State};

handle_call(curr_schedule, _From, State) ->
  CurrSch = State#delay_schlr_state.curr_sch,
  {reply, CurrSch, State};

handle_call(schedule_count, _From, State) ->
  SchCnt = State#delay_schlr_state.schedule_count,
  {reply, SchCnt, State};

handle_call(print_curr_event, _From, State) ->
  CurrSch = State#delay_schlr_state.curr_sch,
  LSS = State#delay_schlr_state.logical_ss,
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

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
update_event_data(CurrEvent, EventsList, Delayed, SS) -> %% (CurrEvent, OrigSymSch, [], SS), for regular scheduler
  {NewEventsList, NewDelayed} =
    case type(CurrEvent) of
     local ->
       [CurrEventTxnId] = CurrEvent#local_event.event_txns,
       EventDC = CurrEvent#local_event.event_dc,
       {ok, DCSS} = dict:find(EventDC, SS),
       {ok, V1} = dict:find(EventDC, DCSS),
       NewEventCT = V1 + 1,
       EventsList1 =
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
                   end, EventsList),
       %% A local event in common_prefix_sch may have its replicated event in delayed
       Delayed1 =
         lists:map(fun(E) ->
                    case type(E) of
                      remote ->
                        [ETxId] = E#remote_event.event_txns,
                        if
                          CurrEventTxnId == ETxId ->
                            E#remote_event{event_snapshot_time = DCSS, event_commit_time = NewEventCT};
                          true ->
                            E
                        end;
                      local ->
                        E
                    end
                   end, Delayed),
       {EventsList1, Delayed1};
     remote -> {EventsList, Delayed}
   end,
  {NewEventsList, NewDelayed}.

type(Event) ->
  if
    is_record(Event, local_event) -> local;
    is_record(Event, remote_event) -> remote;
    true ->
      none
  end.

%%exists_proc(ProcName) ->
%%  lists:member(ProcName, erlang:registered()).

next_event(delay, State) ->
  CommonPrfxSch = State#delay_schlr_state.common_prfx_schl,
  Delayed = State#delay_schlr_state.delayed,
  Delayed2 = State#delay_schlr_state.delayed2,
  SS = State#delay_schlr_state.logical_ss,

  NewCommonPrfxEventIndex = State#delay_schlr_state.common_prefix_event_index + 1,
  NewDelayEventIndex = State#delay_schlr_state.delayed_event_index + 1,

  {NewState, NewCurrEvent} =
    if
      NewCommonPrfxEventIndex =< length(CommonPrfxSch) ->
        CurrEvent1 = lists:nth(NewCommonPrfxEventIndex, CommonPrfxSch),
        {NewCommonPrfSch, NewDelayed} = update_event_data(CurrEvent1, CommonPrfxSch, Delayed, SS),
        CurrEvent = lists:nth(NewCommonPrfxEventIndex, NewCommonPrfSch),

        %%% Update state
        CurrSch = State#delay_schlr_state.curr_sch,
        NewCurrSch = CurrSch ++ [CurrEvent],
        EventType = type(CurrEvent),
        NewLogicalSS1 = update_ss(EventType, CurrEvent, SS),

        State1 = State#delay_schlr_state{
          curr_sch = NewCurrSch,
          common_prfx_schl = NewCommonPrfSch,
          common_prefix_event_index = NewCommonPrfxEventIndex,
          delayed = NewDelayed,
          logical_ss = NewLogicalSS1},
        {State1, CurrEvent};
      true ->
        %%% Sanity Check
        true = (NewDelayEventIndex =< length(Delayed)),
        CurrEvent1 = lists:nth(NewDelayEventIndex, Delayed),
        {NewDelayed, _} = update_event_data(CurrEvent1, Delayed, [], SS),
        CurrEvent = lists:nth(NewDelayEventIndex, NewDelayed),
        get_next_and_update_state(delay, type(CurrEvent),
          [{curr_event, CurrEvent}, {event_list, NewDelayed}, {delayed_list, [Delayed2]}], State)
    end,
  {NewState, NewCurrEvent};

next_event(regular, State) ->
  OrigSymSch = State#delay_schlr_state.orig_sch_sym,
  Delayed = State#delay_schlr_state.delayed, %%_main,
  SS = State#delay_schlr_state.logical_ss,
  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,

  %%% Sanity check
  true = (NewCurrEventIndex =< length(OrigSymSch)),

  CurrEvent1 = lists:nth(NewCurrEventIndex, OrigSymSch),
  %%% Updates ST and CT of the local CurrEvent and its replications
  {NewOrigSch, _} = update_event_data(CurrEvent1, OrigSymSch, [], SS),
  CurrEvent = lists:nth(NewCurrEventIndex, NewOrigSch),

  {NewState1, NewCurrEvent} = get_next_and_update_state(regular, type(CurrEvent),
    [{curr_event, CurrEvent}, {new_orig_sch, NewOrigSch}, {new_delayed, Delayed}], State),
  {NewState1, NewCurrEvent}.

get_next_and_update_state(delay, EventType,
    [{curr_event, CurrEvent}, {event_list, NewDelayed}, {delayed_list, [Delayed2]}], State) ->

  CurrSch = State#delay_schlr_state.curr_sch,
  SS = State#delay_schlr_state.logical_ss,
  NewDelEventIndex = State#delay_schlr_state.delayed_event_index + 1,
  NextDelDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_d),

  {NewState1, NewCurrEvent} =
    case is_runnable(EventType, CurrEvent, Delayed2, SS, NewDelEventIndex, NextDelDelIndex) of
      true ->
        NewCurrSch = CurrSch ++ [CurrEvent],
        NewLogicalSS1 = update_ss(EventType, CurrEvent, SS),
        State1 = State#delay_schlr_state{
          curr_sch = NewCurrSch,
          delayed = NewDelayed,
          delayed_event_index = NewDelEventIndex,
          logical_ss = NewLogicalSS1},
        {State1, CurrEvent};
      false ->
        %%% Delay current event
        NewDelayed2 = Delayed2 ++ [CurrEvent],
        if
          NewDelEventIndex == NextDelDelIndex -> %% If current event is a delaying event
            comm_delay_sequence:spend_current_delay_index(comm_delay_sequence_d);
          true ->
            noop
        end,
        State1 = State#delay_schlr_state{
          delayed = NewDelayed,
          delayed_event_index = NewDelEventIndex,
          delayed2 = NewDelayed2},
        {State1, none}
    end,

  NewState =
    case length(NewDelayed) of
      NewDelEventIndex ->
        TempDelayed2 = NewState1#delay_schlr_state.delayed2,
        NewState1#delay_schlr_state{delayed = TempDelayed2, delayed2 = [], delayed_event_index = 0};
      _ ->
        NewState1
    end,
  {NewState, NewCurrEvent};

get_next_and_update_state(regular, EventType,
    [{curr_event, CurrEvent}, {new_orig_sch, NewOrigSch}, {new_delayed, Delayed}], State) ->
  CurrSch = State#delay_schlr_state.curr_sch,
  SS = State#delay_schlr_state.logical_ss,
  CommPrfSch = State#delay_schlr_state.common_prfx_schl,
  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,
  EventCountTot = State#delay_schlr_state.event_count_total,
  NextDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_r),
  if
    length(NewOrigSch) > EventCountTot ->
      throw(io_lib:format("~n=-=-=-=-= NextDelIndex: ~w =-=-=-=-= DelayedMain len: ~w =-=-=-=-= OrigSch: ~w =-=-=-=-=",
        [NextDelIndex, length(State#delay_schlr_state.delayed_main), NewOrigSch]));
    true -> noop
  end,

  {NewState1, NewCurrEvent} =
    case is_runnable(EventType, CurrEvent, Delayed, SS, NewCurrEventIndex, NextDelIndex) of
      true ->
        NewCurrSch = CurrSch ++ [CurrEvent],
        NewLogicalSS1 = update_ss(EventType, CurrEvent, SS),

        State1 = State#delay_schlr_state{
          orig_sch_sym = NewOrigSch,
          curr_sch = NewCurrSch,
          orig_event_index = NewCurrEventIndex,
          logical_ss = NewLogicalSS1}, %, delayed = NewDelayed

        State2 =
          case length(NewOrigSch) of
            EventCountTot ->
              NewCommPrfSch = CommPrfSch ++ [CurrEvent],
              State1#delay_schlr_state{common_prfx_schl = NewCommPrfSch};
            _Else -> State1
          end,
        {State2, CurrEvent};
      false ->
        %%% Delay current event
        NewDelayed = Delayed ++ [CurrEvent],
        if
          NewCurrEventIndex == NextDelIndex -> %% If current event is a delaying event
            comm_delay_sequence:spend_current_delay_index(comm_delay_sequence_r);
          true ->
            noop
        end,

        State1 = State#delay_schlr_state{
          orig_sch_sym = NewOrigSch,
          orig_event_index = NewCurrEventIndex,
          delayed_main = NewDelayed,
          delayed = NewDelayed},
        {State1, none}
    end,

  NewState =
    case length(NewOrigSch) of
      NewCurrEventIndex ->
        TempDelayed = NewState1#delay_schlr_state.delayed,
        NewState1#delay_schlr_state{orig_sch_sym = TempDelayed, delayed = [], orig_event_index = 0};
      _ ->
        NewState1
    end,
  {NewState, NewCurrEvent}.

%%% Update scheduler SS
update_ss(local, CurrEvent, SS) ->
  EventDC = CurrEvent#local_event.event_dc,
  NewEventCT = CurrEvent#local_event.event_commit_time,
  {ok, DCSS} = dict:find(EventDC, SS),
  NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
  dict:store(EventDC, NewDCSS, SS);

update_ss(remote, CurrEvent, SS) ->
  EventDC = CurrEvent#remote_event.event_dc,
  {ok, DCSS} = dict:find(EventDC, SS),
  NewEventCT = CurrEvent#remote_event.event_commit_time,
  EventOrigDC = CurrEvent#remote_event.event_original_dc,
  NewDCSS = dict:store(EventOrigDC, NewEventCT, DCSS),
  dict:store(EventDC, NewDCSS, SS).

is_runnable(local, _CurrEvent, _Delayed, _SS, CurrEventIndex, DelIndex) ->
  IsDelaying = (CurrEventIndex == DelIndex),
  not IsDelaying;

is_runnable(remote, CurrEvent, Delayed, SS, CurrEventIndex, DelIndex) ->

  IsDelaying = (CurrEventIndex == DelIndex),

  EventDC = CurrEvent#remote_event.event_dc,
  {ok, DCSS} = dict:find(EventDC, SS),
  DepSatisfied =  vectorclock:ge(DCSS, CurrEvent#remote_event.event_snapshot_time),

  %%% Check if CurrEvent is replication of a delayed event
  IsRepl =
    lists:any(fun(E) ->
                case type(E) of
                  local ->
                    CurrEvent#remote_event.event_txns == E#local_event.event_txns;
                  _ ->
                    false
                end
              end, Delayed),
  (not IsDelaying) andalso (not IsRepl) andalso DepSatisfied.