-module(comm_random_scheduler).

-include("commander.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
  has_next_schedule/0,
  setup_next_schedule/0,
  next_event/0,
  is_end_current_schedule/0,
  schedule_count/0,
  curr_schedule/0,
  stop/0]).

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
start_link([_DelayBound, Bound, DCs, OrigSchSym]) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [[Bound, DCs, OrigSchSym]], []).

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

stop() ->
  gen_server:call(?SERVER, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([[Bound, DCs, OrigSchSym]]) ->
  EventCount = length(OrigSchSym),
  InitState = #rand_schlr_state{
    event_count_total = EventCount,
    orig_sch_sym = OrigSchSym,
    dcs = DCs,
    schedule_count = 0,
    curr_sch=[],
    remained=OrigSchSym,
    bound = Bound,
    initial_seed = {110,220,330}},
  random:seed(InitState#rand_schlr_state.initial_seed),
  {ok, InitState}.

handle_call(has_next_schedule, _From, State) ->
  SchCnt = State#rand_schlr_state.schedule_count,
  Bound = State#rand_schlr_state.bound,
  Result = (SchCnt < Bound),
  {reply, Result, State};

handle_call(setup_next_schedule, _From, State)->
  OrigSchSym = State#rand_schlr_state.orig_sch_sym, %%commander:get_scheduling_data(),
  %EventCount = length(OrigSchSym),
  DCs = State#rand_schlr_state.dcs,

  %%% Set stable snapshot in all DCs to 0
  InitSS = lists:foldl(fun(Dc, SS1) ->
                          dict:store(Dc, 0, SS1)
                       end, dict:new(), DCs),
  LogicalSS = lists:foldl(fun(Dc, LSS1) ->
                            dict:store(Dc, InitSS, LSS1)
                          end, dict:new(), DCs),
  SchCnt = State#rand_schlr_state.schedule_count,
  InitState = State#rand_schlr_state{
    %event_count_total = EventCount,
    %orig_sch_sym = OrigSchSym,
    curr_sch = [],
    logical_ss = LogicalSS,
    schedule_count = SchCnt+1,
    remained=OrigSchSym},

  {A, B, C} = InitState#rand_schlr_state.initial_seed,
  A1 = A+SchCnt,
  B1 = B+SchCnt,
  C1 = C+SchCnt,
  random:seed({A1, B1, C1}),
  {reply, ok, InitState};

handle_call(next_event, _From, State) -> %% event | none
  CurrSch = State#rand_schlr_state.curr_sch,
  OrigSymSch = State#rand_schlr_state.orig_sch_sym,
  SS = State#rand_schlr_state.logical_ss,
  Remained = State#rand_schlr_state.remained,

  %% - pick an event randomly from the remained events
  EventIndx = random:uniform(length(Remained)),
  Event = lists:nth(EventIndx, Remained),

  %% - check the event type
  EventType = comm_utilities:type(Event),

  {NewState, NextEvent} =
    case EventType of
      local -> %% - if event is a local event, schedule it, and update its CT, and DC's SS
        %%% Update the scheduler state
        NewOrigSch = update_event_data(Event, OrigSymSch, SS),
        NewCurrSch = CurrSch ++ [Event],
        NewRemained = lists:delete(Event, Remained),

        %%% Update receiver SS %%%
        EventDC = Event#local_event.event_dc,
        NewEventCT = Event#local_event.event_commit_time,
        {ok, DCSS} = dict:find(EventDC, SS),
        NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
        NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),

        %%% Update the rand_schlr_state record
        State1 = State#rand_schlr_state{
          orig_sch_sym = NewOrigSch,
          curr_sch = NewCurrSch,
          remained = NewRemained,
          logical_ss = NewLogicalSS1},
        {State1, Event};
      remote -> %% - if the event is a remote event, check its dependency
        %% - check if it is not blocked? (its dependency is satisfied in te target DC)
        case is_blocked(remote, Event, SS, CurrSch, Remained) of
          false ->
            NewCurrSch = CurrSch ++ [Event],
            NewRemained = lists:delete(Event, Remained),

            %%% Update receiver SS %%%
            EventDC = Event#remote_event.event_dc,
            NewEventCT = Event#remote_event.event_commit_time,
            {ok, DCSS} = dict:find(EventDC, SS),
            NewDCSS = dict:store(EventDC, NewEventCT, DCSS),
            NewLogicalSS1 = dict:store(EventDC, NewDCSS, SS),

            %%% Update the rand_schlr_state record
            State1 = State#rand_schlr_state{
              curr_sch = NewCurrSch,
              remained = NewRemained,
              logical_ss = NewLogicalSS1},
            {State1, Event};
          true ->
            {State, none}
        end
    end,
  {reply, NextEvent, NewState};

handle_call(is_end_current_schedule, _From, State) ->
  Remained = State#rand_schlr_state.remained,
  Res = (length(Remained) == 0),
  {reply, Res, State};

handle_call(schedule_count, _From, State) ->
  SchCnt = State#rand_schlr_state.schedule_count,
  {reply, SchCnt, State};

handle_call(curr_schedule, _From, State) ->
  CurrSch = State#rand_schlr_state.curr_sch,
  {reply, CurrSch, State}.

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

%%% Returns true if Event is
%%% either the replication of an event which has not been replayed
%%% or its dependency is not satisfied
is_blocked(remote, Event, SS, CurrSch, Remained) ->
  %%% Check if it is the replication of an unscheduled local event
  Possible =
    not lists:any(fun(E) ->
                    case comm_utilities:type(E) of
                      local ->
                        Event#remote_event.event_txns == E#local_event.event_txns;
                      _ ->
                        false
                    end end, Remained),

  %%% Sanity check
  Possible = lists:any(fun(E) ->
                          case comm_utilities:type(E) of
                            local ->
                              Event#remote_event.event_txns == E#local_event.event_txns;
                            _ ->
                              false
                          end end, CurrSch),

  %%% If scheduling Event is possible, check if its dependency is satisfied
  case Possible of
    true ->
      EventDC = Event#remote_event.event_dc,
      {ok, DC_SS} = dict:find(EventDC, SS),
      not vectorclock:ge(DC_SS, Event#remote_event.event_snapshot_time);
    false ->
      true
  end.

%%% Event is the scheduled event;
%%% its CT is updated using lamport logical clocks;
%%% the update must be applied both local event and its corresponding remote event
%%% This function is called only for local events
update_event_data(Event, OrigSymSch, SS) ->
  [EventTxnId] = Event#local_event.event_txns,
  EventDC = Event#local_event.event_dc,
  {ok, DCSS} = dict:find(EventDC, SS),
  {ok, V1} = dict:find(EventDC, DCSS),
  NewEventCT = V1 + 1,
  lists:map(fun(E) ->
    case comm_utilities:type(E) of
      local ->
        [ETxId] = E#local_event.event_txns,
        if
          EventTxnId == ETxId ->
            E#local_event{
              event_snapshot_time = DCSS,
              event_commit_time = NewEventCT};
          true ->
            E
        end;
      remote ->
        [ETxId] = E#remote_event.event_txns,
        if
          EventTxnId == ETxId ->
            E#remote_event{
              event_snapshot_time = DCSS,
              event_commit_time = NewEventCT};
          true ->
            E
        end
    end end, OrigSymSch).