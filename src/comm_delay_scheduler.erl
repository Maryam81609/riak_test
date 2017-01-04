-module(comm_delay_scheduler).
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
start_link([DelayBound, Bound ,DCs, OrigSymSch]) ->
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

handle_call(curr_state, _From, State) ->
  {reply, State, State};

handle_call(has_next_schedule, _From, State) ->
  SchCnt = State#delay_schlr_state.schedule_count,
  Bound = State#delay_schlr_state.bound,
  CommonPrfxSchlCnt = State#delay_schlr_state.common_prfx_schl_cnt,
  CommonPrfxBound = State#delay_schlr_state.common_prfx_bound,
  DelayedMain = State#delay_schlr_state.delayed_main,


  Res0 = exists_proc(comm_delay_sequence_d), %%% Replace with length(DelayedMain) > 0 => not initial run, and ds_d must run
  Res1 = not Res0 orelse ((CommonPrfxSchlCnt < CommonPrfxBound) and comm_delay_sequence:has_next(comm_delay_sequence_d)),%% orelse (CommonPrfxSchlCnt == 0)),
  Res2 = ((SchCnt < Bound) and comm_delay_sequence:has_next(comm_delay_sequence_r)) orelse (SchCnt == 0),
  Result = Res1 or Res2,
  io:format("~n=======SchCnt: ~w =====CommonPrfxSchlCnt: ~w =====~n", [SchCnt, CommonPrfxSchlCnt]),
  %%io:format("~n=======Res0: ~w =====Res1: ~w =====Res2: ~w =====~n", [Res0, Res1, Res2]),
  io:format("~n=======Delayed main len: ~w ========~n", [length(DelayedMain)]),
  {reply, Result, State};

handle_call(setup_next_schedule, _From, State) ->
  DCs = State#delay_schlr_state.dcs,
  SchCnt = State#delay_schlr_state.schedule_count,
  CommonPrfxSchlCnt = State#delay_schlr_state.common_prfx_schl_cnt,
  CommonPrfxBound = State#delay_schlr_state.common_prfx_bound,
  OrigSchMain = State#delay_schlr_state.orig_sch_sym_main,


  %%% Set stable snapshot in all DCs to 0
  InitSS = lists:foldl(fun(Dc, SS1) ->
                          dict:store(Dc, 0, SS1)
                       end, dict:new(), DCs),
  LogicalSS = lists:foldl(fun(Dc, LSS1) ->
                            dict:store(Dc, InitSS, LSS1)
                          end, dict:new(), DCs),

  Exists_DS_d = exists_proc(comm_delay_sequence_d),
  {B1, B2} =
    case Exists_DS_d of
      true ->
        {comm_delay_sequence:has_next(comm_delay_sequence_d), CommonPrfxSchlCnt < CommonPrfxBound};
      false ->
        {false, false}
    end,

  io:format("~n========setup== Exists_d: ~w ===has_next_d: ~w == CommonPrfxSchlCnt: ~w ===========~n", [Exists_DS_d, B1, CommonPrfxSchlCnt]),

  NewState =
    if
      B1 and B2 ->
        CurrDelayedDelSeq = comm_delay_sequence:next(comm_delay_sequence_d),
        DelayedMain = State#delay_schlr_state.delayed_main,
        State#delay_schlr_state{
          curr_delayed_delay_seq = CurrDelayedDelSeq,
          delayed = DelayedMain,
          delayed2 = [],
          delayer = delay,
          common_prfx_schl_cnt = CommonPrfxSchlCnt + 1,
          common_prefix_event_index = 0};
      true ->
        if
          Exists_DS_d -> comm_delay_sequence:stop(comm_delay_sequence_d);
          true -> skip
        end,

        %%% Start delayed delay sequence generator
        DelayBound = State#delay_schlr_state.delay_bound,
        DelayedMain = State#delay_schlr_state.delayed_main,
        io:format("~n========setup== DelayedMain len: ~w ==========~n", [length(DelayedMain)]),
        case DelayedMain of
          [_|_] ->
            comm_delay_sequence:start_link(comm_delay_sequence_d, DelayBound, length(DelayedMain));
          _Else ->
            skip
        end,

        CurrDelSeq = comm_delay_sequence:next(comm_delay_sequence_r),
        io:format("~n=====setup=== CurrDelSeq: ~w ======", [CurrDelSeq]),
        State#delay_schlr_state{
          orig_sch_sym = OrigSchMain,
          curr_delay_seq = CurrDelSeq,
          common_prfx_schl = [],
          common_prfx_schl_cnt = 0,
          delayed_main = [],
          delayed_count = 0, %% ?
          delayer = regular} %% , schedule_count = SchCnt+1
    end,

  InitState = NewState#delay_schlr_state{
    orig_sch_sym = OrigSchMain, %%??????????????????????????
    curr_sch = [],
    orig_event_index = 0,
    common_prefix_event_index = 0,
    delayed_event_index = 0,
    logical_ss = LogicalSS,
    schedule_count = SchCnt+1},
  {reply, ok, InitState};

handle_call(next_event, _From, State) ->
  lager:info("Entered next_event"),
  Delayer = State#delay_schlr_state.delayer,
  {NewState, NewCurrEvent} = next_event(Delayer, State),
  io:format("~n===next_event==NewState.DealyedMain len: ~w ===~n", [length(NewState#delay_schlr_state.delayed_main)]),
%%  io:format("~n****Event type: ~w~n", [type(NewCurrEvent)]),
%%  {ST, DC} = case type(NewCurrEvent) of
%%                local ->
%%                  {NewCurrEvent#local_event.event_snapshot_time, NewCurrEvent#local_event.event_dc};
%%                remote ->
%%                  {NewCurrEvent#remote_event.event_snapshot_time, NewCurrEvent#remote_event.event_dc};
%%                _Else ->
%%                  {none, none}
%%             end,
%%  if
%%    ST /= none ->
%%      {ok, DC_SS} = dict:find(DC, NewState#delay_schlr_state.logical_ss),
%%      io:format("~n****Event ST: ~w ~n", [dict:to_list(ST)]),
%%      io:format("~n****Event DC: ~w ~n", [DC]),
%%      io:format("~n****DC Clock: ~w ~n", [dict:to_list(DC_SS)]);
%%    true -> noop
%%  end,

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

  io:format("~n====hc===end_sch====Delayer: ~w=====OrigEvIndex: ~w=====Orig Sch len: ~w=====Delayed len: ~w", [Delayer, OrigEvIndex, length(OrigSchSym), length(Delayed)]),
  IsEnd =
    case Delayer of
      regular ->
        (OrigEvIndex == length(OrigSchSym)) and (Delayed == []); %%(DelayedEventIndex >= length(Delayed));
      delay ->
        (CommonPrfxEvIndex >= length(CommonPrfxSchl)) and (DelayedEventIndex >= length(Delayed)) and (Delayed2 == [])
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
                        end%;
                      %true ->
                      %  E
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

exists_proc(ProcName) ->
  lists:member(ProcName, erlang:registered()).

next_event(delay, State) ->
  %%OrigSymSch = State#delay_schlr_state.orig_sch_sym,
  Delayed = State#delay_schlr_state.delayed,
  Delayed2 = State#delay_schlr_state.delayed2,
  CommonPrfxSch = State#delay_schlr_state.common_prfx_schl,
  SS = State#delay_schlr_state.logical_ss,

  NewDelayEventIndex = State#delay_schlr_state.delayed_event_index + 1,
  NewCommonPrfxEventIndex = State#delay_schlr_state.common_prefix_event_index + 1,

  {NewState, NewCurrEvent} =
    if
      NewCommonPrfxEventIndex =< length(CommonPrfxSch) ->
        CurrEvent1 = lists:nth(NewCommonPrfxEventIndex, CommonPrfxSch),
        {NewCommonPrfSch, NewDelayed} = update_event_data(CurrEvent1, CommonPrfxSch, Delayed, SS),
        CurrEvent = lists:nth(NewCommonPrfxEventIndex, NewCommonPrfSch),
        update_state_for_next_event(delay, type(CurrEvent), [{curr_event, CurrEvent},
          {new_common_prfx_sch, NewCommonPrfSch}, {new_delayed, NewDelayed}, {new_delayed2, Delayed2}], State);
      true ->
        {NewDelayed1, NewDelayed2, NewDelayEventIndex1} =
          if
            NewDelayEventIndex > length(Delayed) andalso length(Delayed2 > 0)->
              NewDelayed3 = Delayed2,
              NewDelayed2_1 = [],
              {NewDelayed3, NewDelayed2_1, 1};
            true ->
              {Delayed, Delayed2, NewDelayEventIndex}
          end,
        CurrEvent1 = lists:nth(NewDelayEventIndex1, NewDelayed1),
        {NewDelayed, _} = update_event_data(CurrEvent1, NewDelayed1, [], SS),
        CurrEvent = lists:nth(NewDelayEventIndex1, NewDelayed),
        update_state_for_next_event(delay, type(CurrEvent), [{curr_event, CurrEvent},
          {new_common_prfx_sch, CommonPrfxSch}, {new_delayed, NewDelayed}, {new_delayed2, NewDelayed2}], State)
    end,
    {NewState, NewCurrEvent};

next_event(regular, State) ->
  OrigSymSch = State#delay_schlr_state.orig_sch_sym,
  Delayed = State#delay_schlr_state.delayed_main,
  SS = State#delay_schlr_state.logical_ss,

  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,
  %%NewDelayEventIndex = State#delay_schlr_state.delayed_event_index + 1,

  io:format("~n===next_regular===before sanity check====NewCurrEventIndex: ~w====OrigSch len:~w~n", [NewCurrEventIndex, length(OrigSymSch)]),
  %%% Sanity check
  true = NewCurrEventIndex =< length(OrigSymSch),

  CurrEvent1 = lists:nth(NewCurrEventIndex, OrigSymSch),
  case length(OrigSymSch) of
    1 -> io:format("~n===next_regular===before update data====OrigSch:~w~n", [OrigSymSch]);
    _ -> noop
  end,
  %%% Updates ST and CT of the local CurrEvent and its replications
  {NewOrigSch, _} = update_event_data(CurrEvent1, OrigSymSch, [], SS),
  case length(NewOrigSch) of
    1 -> io:format("~n===next_regular===after update data====OrigSch:~w~n", [NewOrigSch]);
    _ -> noop
  end,
  CurrEvent = lists:nth(NewCurrEventIndex, NewOrigSch),

  {NewState1, NewCurrEvent} = get_next_and_update_state(regular, type(CurrEvent),
    [{curr_event, CurrEvent}, {new_orig_sch, NewOrigSch}, {new_delayed, Delayed}], State),

  case length(NewOrigSch) of
    1 -> io:format("~n===next_regular===after update data====NewCurrEv:~w~n", [NewCurrEvent]);
    _ -> noop
  end,
%%  NewState =
%%    case NewCurrEventIndex of
%%      length(OrigSymSch) ->
%%        TempDelayed = NewState1#delay_schlr_state.delayed,
%%        NewState1#delay_schlr_state{orig_sch_sym = TempDelayed, delayed = [], orig_event_index = 0};
%%      _ ->
%%        NewState1
%%    end,
  io:format("~n===next_regular==Delayed len: ~w ==NewState.DealyedMain len: ~w ===~n", [length(Delayed), length(NewState1#delay_schlr_state.delayed_main)]),
  io:format("~n===next_regular==origSchEventIndex: ~w===OrigSch len:~w~n", [NewState1#delay_schlr_state.orig_event_index, length(NewState1#delay_schlr_state.orig_sch_sym)]),
  {NewState1, NewCurrEvent}.

%%next_event(regular, State) ->
%%  OrigSymSch = State#delay_schlr_state.orig_sch_sym,
%%  Delayed = State#delay_schlr_state.delayed,
%%  SS = State#delay_schlr_state.logical_ss,
%%
%%  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,
%%  NewDelayEventIndex = State#delay_schlr_state.delayed_event_index + 1,
%%
%%  {NewState, NewCurrEvent} =
%%    if
%%      NewCurrEventIndex =< length(OrigSymSch) ->
%%        CurrEvent1 = lists:nth(NewCurrEventIndex, OrigSymSch),
%%        %%% Updates ST and CT of the local CurrEvent and its replications;
%%        {NewOrigSch, _} = update_event_data(CurrEvent1, OrigSymSch, [], SS),
%%        CurrEvent = lists:nth(NewCurrEventIndex, NewOrigSch),
%%        update_state_for_next_event(regular, type(CurrEvent),
%%          [{curr_event, CurrEvent},
%%            {new_orig_sch, NewOrigSch},
%%            {new_delayed, Delayed}],
%%          State);
%%      true -> %% CurrEvent is in delayed list
%%        CurrEvent1 = lists:nth(NewDelayEventIndex, Delayed),
%%        {NewDelayed, _} = update_event_data(CurrEvent1, Delayed, [], SS),
%%        CurrEvent = lists:nth(NewDelayEventIndex, NewDelayed),
%%        update_state_for_next_event(regular, type(CurrEvent),
%%          [{curr_event, CurrEvent},
%%            {new_orig_sch, OrigSymSch},
%%            {new_delayed, NewDelayed}],
%%          State)
%%    end,
%%  io:format("~n===next_regular==Delayed len: ~w ==NewState.DealyedMain len: ~w ===~n", [length(Delayed), length(NewState#delay_schlr_state.delayed_main)]),
%%  {NewState, NewCurrEvent}.

update_state_for_next_event(delay, remote, Params, State) ->
  CurrEvent = lists:keyfind(curr_event, 1, Params),
  NewCommonPrfSch = lists:keyfind(new_common_prfx_sch, 1, Params),
  NewDelayed = lists:keyfind(new_delayed, 1, Params),
  NewDelayed2 = lists:keyfind(new_delayed2, 1, Params),

  CurrSch = State#delay_schlr_state.curr_sch,
  SS = State#delay_schlr_state.logical_ss,
  NewCommonPrfxEventIndex = State#delay_schlr_state.common_prefix_event_index + 1,
  NewDelEventIndex = State#delay_schlr_state.delayed_event_index + 1,

  EventDC = CurrEvent#remote_event.event_dc,
  {ok, DCSS2} = dict:find(EventDC, SS),

  DepSatisfied =  vectorclock:ge(DCSS2, CurrEvent#remote_event.event_snapshot_time),

  %%% Check if CurrEvent is replication of a delayed event
  IsRepl = lists:any(fun(E) ->
                        case type(E) of
                          local ->
                            CurrEvent#remote_event.event_txns == E#local_event.event_txns;
                          _ ->
                            false
                        end
                     end, NewDelayed2),

  if
    NewCommonPrfxEventIndex =< length(NewCommonPrfSch) ->
      NewCurrSch = CurrSch ++ [CurrEvent],

      %%% Update SS %%%
      NewEventCT = CurrEvent#remote_event.event_commit_time,
      NewDCSS = dict:store(EventDC, NewEventCT, DCSS2),
      NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
      %%% End of update SS %%%

      State1 = State#delay_schlr_state{
        curr_sch = NewCurrSch,
        common_prfx_schl = NewCommonPrfSch,
        common_prefix_event_index = NewCommonPrfxEventIndex,
        logical_ss = NewLogicalSS1,
        delayed = NewDelayed,
        delayed2 = NewDelayed2 },
      {State1, CurrEvent};
    true ->
      NextDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_d),
      true = NewDelEventIndex =< length(NewDelayed), %Sanity check%
      if
        NewDelEventIndex == NextDelIndex ->
          %%% Delay current event
          NewDelayed2_1 = NewDelayed2 ++ [CurrEvent],
          comm_delay_sequence:spend_current_delay_index(comm_delay_sequence_d),

          State1 = State#delay_schlr_state{
            delayed = NewDelayed,
            delayed_event_index = NewDelEventIndex,
            delayed2 = NewDelayed2_1},
          {State1, none};
        not DepSatisfied or IsRepl ->
          NewDelayed2_1 = NewDelayed2 ++ [CurrEvent],

          State1 = State#delay_schlr_state{
            delayed = NewDelayed,
            delayed_event_index = NewDelEventIndex,
            delayed2 = NewDelayed2_1},
          {State1, none};
        true ->
          NewCurrSch = CurrSch ++ [CurrEvent],
          io:format("~n====DepSat: ~w====~n", [DepSatisfied]),
          %%% Update SS %%%
          NewEventCT = CurrEvent#remote_event.event_commit_time,
          NewDCSS = dict:store(EventDC, NewEventCT, DCSS2),
          NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
          %%% End of update SS %%%

          State1 = State#delay_schlr_state{
            curr_sch = NewCurrSch,
            logical_ss = NewLogicalSS1,
            delayed = NewDelayed,
            delayed_event_index = NewDelEventIndex,
            delayed2 = NewDelayed2 },
          {State1, CurrEvent}
      end
  end;

update_state_for_next_event(delay, local, Params, State) ->
  CurrEvent = lists:keyfind(curr_event, 1, Params),
  NewCommonPrfSch = lists:keyfind(new_common_prfx_sch, 1, Params),
  NewDelayed = lists:keyfind(new_delayed, 1, Params),
  NewDelayed2 = lists:keyfind(new_delayed2, 1, Params),

  CurrSch = State#delay_schlr_state.curr_sch,
  SS = State#delay_schlr_state.logical_ss,
  NewCommonPrfxEventIndex = State#delay_schlr_state.common_prefix_event_index + 1,
  NewDelEventIndex = State#delay_schlr_state.delayed_event_index + 1,

  if
    NewCommonPrfxEventIndex =< length(NewCommonPrfSch) ->
      NewCurrSch = CurrSch ++ [CurrEvent],

      %%% Update SS %%%
      EventDC = CurrEvent#local_event.event_dc,
      NewEventCT = CurrEvent#local_event.event_commit_time,
      {ok, DCSS} = dict:find(EventDC, SS),
      NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
      NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
      %%% End of update SS %%%

      State1 = State#delay_schlr_state{
        curr_sch = NewCurrSch,
        common_prfx_schl = NewCommonPrfSch,
        common_prefix_event_index = NewCommonPrfxEventIndex,
        logical_ss = NewLogicalSS1,
        delayed = NewDelayed,
        delayed2 = NewDelayed2},
      {State1, CurrEvent};
    true ->
      NextDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_d),
      true = NewDelEventIndex =< length(NewDelayed), %Sanity check%
      if
        NewDelEventIndex == NextDelIndex ->
          %%% Delay current event
          NewDelayed2_1 = NewDelayed2 ++ [CurrEvent],
          comm_delay_sequence:spend_current_delay_index(comm_delay_sequence_d),

          State1 = State#delay_schlr_state{
            delayed = NewDelayed,
            delayed_event_index = NewDelEventIndex,
            delayed2 = NewDelayed2_1 }, %% dependency = NewDep,
          {State1, none};
        true ->
          NewCurrSch = CurrSch ++ [CurrEvent],

          %%% Update SS %%%
          EventDC = CurrEvent#local_event.event_dc,
          NewEventCT = CurrEvent#local_event.event_commit_time,
          {ok, DCSS} = dict:find(EventDC, SS),
          NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
          NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
          %%% End of update SS %%%

          State1 = State#delay_schlr_state{
            curr_sch = NewCurrSch,
            logical_ss = NewLogicalSS1,
            delayed = NewDelayed,
            delayed_event_index = NewDelEventIndex,
            delayed2 = NewDelayed2 },
          {State1, CurrEvent}
      end
  end.

%%update_state_for_next_event(regular, local, [{curr_event, CurrEvent},
%%  {new_orig_sch, NewOrigSch}, {new_delayed, NewDelayed}], State) ->
%%
%%  CurrSch = State#delay_schlr_state.curr_sch,
%%  SS = State#delay_schlr_state.logical_ss,
%%  CommPrfSch = State#delay_schlr_state.common_prfx_schl,
%%  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,
%%  NewDelEventIndex = State#delay_schlr_state.delayed_event_index + 1,
%%
%%  {NewState, NewCurrEvent} =
%%  if
%%    NewCurrEventIndex =< length(NewOrigSch) ->
%%      NextDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_r),
%%      if
%%        NewCurrEventIndex == NextDelIndex ->
%%          %%% Delay current event
%%          NewDelayed1 = NewDelayed ++ [CurrEvent],
%%          comm_delay_sequence:spend_current_delay_index(comm_delay_sequence_r),
%%
%%          State1 = State#delay_schlr_state{
%%            orig_sch_sym = NewOrigSch,
%%            orig_event_index = NewCurrEventIndex,
%%            delayed_main = NewDelayed1,
%%            delayed = NewDelayed1}, %% dependency = NewDep,
%%          {State1, none};
%%        true ->
%%          NewCurrSch = CurrSch ++ [CurrEvent],
%%          NewCommPrfSch = CommPrfSch ++ [CurrEvent],
%%
%%          %%% Update SS %%%
%%          EventDC = CurrEvent#local_event.event_dc,
%%          NewEventCT = CurrEvent#local_event.event_commit_time,
%%          {ok, DCSS} = dict:find(EventDC, SS),
%%          NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
%%          NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
%%          %%% End of update SS %%%
%%
%%          State1 = State#delay_schlr_state{
%%            orig_sch_sym = NewOrigSch,
%%            curr_sch = NewCurrSch,
%%            common_prfx_schl = NewCommPrfSch,
%%            orig_event_index = NewCurrEventIndex,
%%            logical_ss = NewLogicalSS1,
%%            delayed = NewDelayed},
%%          {State1, CurrEvent}
%%      end;
%%    true ->
%%      NewCurrSch = CurrSch ++ [CurrEvent],
%%
%%      %%% Update SS %%%
%%      EventDC = CurrEvent#local_event.event_dc,
%%      NewEventCT = CurrEvent#local_event.event_commit_time,
%%      {ok, DCSS} = dict:find(EventDC, SS),
%%      NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
%%      NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),
%%      %%% End of update SS %%%
%%
%%      State1 = State#delay_schlr_state{
%%        orig_sch_sym = NewOrigSch,
%%        curr_sch = NewCurrSch,
%%        logical_ss = NewLogicalSS1,
%%        delayed = NewDelayed,
%%        delayed_event_index = NewDelEventIndex},
%%      {State1, CurrEvent}
%%  end,
%%  io:format("~n===next_regular_local==Input Delayed len: ~w ==NewState.DealyedMain len: ~w ===~n", [length(NewDelayed), length(NewState#delay_schlr_state.delayed_main)]),
%%  {NewState, NewCurrEvent};

%%update_state_for_next_event(regular, remote, [{curr_event, CurrEvent},
%%      {new_orig_sch, NewOrigSch}, {new_delayed, NewDelayed}], State) ->
%%
%%  CurrSch = State#delay_schlr_state.curr_sch,
%%  SS = State#delay_schlr_state.logical_ss,
%%  CommPrfSch = State#delay_schlr_state.common_prfx_schl,
%%  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,
%%  NewDelEventIndex = State#delay_schlr_state.delayed_event_index + 1,
%%
%%  EventDC = CurrEvent#remote_event.event_dc,
%%  {ok, DCSS2} = dict:find(EventDC, SS),
%%
%%  DepSatisfied =  vectorclock:ge(DCSS2, CurrEvent#remote_event.event_snapshot_time),
%%
%%  %%% Check if CurrEvent is replication
%%  IsRepl = lists:any(fun(E) ->
%%                        case type(E) of
%%                          local ->
%%                            CurrEvent#remote_event.event_txns == E#local_event.event_txns;
%%                          _ ->
%%                            false
%%                        end
%%                     end, NewDelayed),
%%
%%  {NewState, NewCurrEvent} =
%%  if
%%    NewCurrEventIndex =< length(NewOrigSch) ->
%%      NextDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_r),
%%      if
%%        NewCurrEventIndex == NextDelIndex ->
%%          %%% Delay current event
%%          NewDelayed1 = NewDelayed ++ [CurrEvent],
%%          comm_delay_sequence:spend_current_delay_index(comm_delay_sequence_r),
%%
%%          State1 = State#delay_schlr_state{
%%            orig_sch_sym = NewOrigSch,
%%            orig_event_index = NewCurrEventIndex,
%%            delayed_main = NewDelayed1,
%%            delayed = NewDelayed1},
%%          {State1, none};
%%        not DepSatisfied or IsRepl ->
%%          NewDelayed1 = NewDelayed ++ [CurrEvent],
%%          State1 = State#delay_schlr_state{
%%            orig_sch_sym = NewOrigSch,
%%            orig_event_index = NewCurrEventIndex,
%%            delayed_main = NewDelayed1,
%%            delayed = NewDelayed1},
%%          {State1, none};
%%        true ->
%%          NewCurrSch = CurrSch ++ [CurrEvent],
%%          NewCommPrfSch = CommPrfSch ++ [CurrEvent],
%%          io:format("~n====DepSat: ~w====~n", [DepSatisfied]),
%%          %%% Update SS
%%          NewEventCT2 = CurrEvent#remote_event.event_commit_time,
%%          EventOrigDC = CurrEvent#remote_event.event_original_dc,
%%          NewDCSS2 = dict:store(EventOrigDC, NewEventCT2, DCSS2),
%%          NewLogicalSS2 = dict:store(EventDC, NewDCSS2, SS),
%%          %%% End of update SS
%%
%%          State1 = State#delay_schlr_state{
%%            orig_sch_sym = NewOrigSch,
%%            common_prfx_schl = NewCommPrfSch,
%%            curr_sch = NewCurrSch,
%%            orig_event_index = NewCurrEventIndex,
%%            logical_ss = NewLogicalSS2},
%%          {State1, CurrEvent}
%%      end;
%%    true ->
%%      NewCurrSch = CurrSch ++ [CurrEvent],
%%      io:format("~n====DepSat: ~w====~n", [DepSatisfied]),
%%      %%% Update SS %%%
%%      NewEventCT2 = CurrEvent#remote_event.event_commit_time,
%%      EventOrigDC = CurrEvent#remote_event.event_original_dc,
%%      NewDCSS2 = dict:store(EventOrigDC, NewEventCT2, DCSS2),
%%      NewLogicalSS2 = dict:store(EventDC, NewDCSS2, SS),
%%      %%% End of update SS %%%
%%
%%      State1 = State#delay_schlr_state{
%%        orig_sch_sym = NewOrigSch,
%%        curr_sch = NewCurrSch,
%%        logical_ss = NewLogicalSS2,
%%        delayed = NewDelayed,
%%        delayed_event_index = NewDelEventIndex},
%%      {State1, CurrEvent}
%%  end,
%%  io:format("~n===next_regular_remote==Input Delayed len: ~w ==NewState.DealyedMain len: ~w ===~n", [length(NewDelayed), length(NewState#delay_schlr_state.delayed_main)]),
%%  {NewState, NewCurrEvent}.

get_next_and_update_state(regular, EventType, %%local
    [{curr_event, CurrEvent}, {new_orig_sch, NewOrigSch}, {new_delayed, Delayed}], State) ->

  CurrSch = State#delay_schlr_state.curr_sch,
  SS = State#delay_schlr_state.logical_ss,
  CommPrfSch = State#delay_schlr_state.common_prfx_schl,
  NewCurrEventIndex = State#delay_schlr_state.orig_event_index + 1,
  %% NewDelEventIndex = State#delay_schlr_state.delayed_event_index + 1,
  NextDelIndex = comm_delay_sequence:get_next_delay_index(comm_delay_sequence_r),

  io:format("~n=====get_next_and_update_state======CurrEvent: ~w~n", [CurrEvent]),
  io:format("~n=====get_next_and_update_state======is runnable:~w~n", [is_runnable(EventType, CurrEvent, Delayed, SS, NewCurrEventIndex, NextDelIndex)]),

  {NewState1, NewCurrEvent} =
    case is_runnable(EventType, CurrEvent, Delayed, SS, NewCurrEventIndex, NextDelIndex) of
      true ->
        NewCurrSch = CurrSch ++ [CurrEvent],
        NewCommPrfSch = CommPrfSch ++ [CurrEvent],
        NewLogicalSS1 = update_ss(EventType, CurrEvent, SS),

        State1 = State#delay_schlr_state{
          orig_sch_sym = NewOrigSch,
          curr_sch = NewCurrSch,
          common_prfx_schl = NewCommPrfSch,
          orig_event_index = NewCurrEventIndex,
          logical_ss = NewLogicalSS1}, %, delayed = NewDelayed
        {State1, CurrEvent};
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
          delayed = NewDelayed}, %%  dependency = NewDep,
        {State1, none}
    end,

  NewState =
    case length(NewOrigSch) of
      NewCurrEventIndex ->
        %%DelMain = NewState1#delay_schlr_state.delayed_main,
        %%NewState2 = NewState1#delay_schlr_state{delayed = DelMain},
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
  io:format("~n=====is_runnable=====IsDelaying: ~w=======IsRepl: ~w=======DepSatisfied:~w ~n", [IsDelaying, IsRepl, DepSatisfied]),
  (not IsDelaying) andalso (not IsRepl) andalso DepSatisfied.

