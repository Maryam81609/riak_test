-module(comm_delay_sequence_forward).

-behaviour(gen_server).

%% API
-export([start_link/3,
        has_next/1,
        next/1,
        get_next_delay_index/1,
        spend_current_delay_index/1,
        is_end_current_delay_seq/1,
        print_sequence/1, stop/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {delay_seq=[], delay_bound, max_delay_indx, current_delay_seq_indx, count}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Server, DelayBound, MaxDelayIndex) ->
  gen_server:start_link({local, Server}, ?MODULE, [DelayBound, MaxDelayIndex], []).

has_next(Server) ->
  gen_server:call(Server, has_next).

next(Server) ->
  gen_server:call(Server, next).

get_next_delay_index(Server) ->
  gen_server:call(Server, get_next_delay_index).

spend_current_delay_index(Server) ->
  gen_server:call(Server, spend_current_delay_index).

is_end_current_delay_seq(Server) ->
  gen_server:call(Server, is_end_current_delay_seq).

print_sequence(Server) ->
  gen_server:call(Server, print_sequence).

stop(Server) ->
  gen_server:cast(Server, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([DelayBound, MaxDelayIndex]) ->
  InitState =
    #state{delay_seq = [],
      delay_bound = DelayBound,
      max_delay_indx = MaxDelayIndex,
      current_delay_seq_indx = 0,
      count = 0},
  {ok, InitState}.

handle_call(has_next, _From, State) ->
  DelayBound = State#state.delay_bound,
  MaxDelayIndex = State#state.max_delay_indx,
  Reply =
    case State#state.delay_seq of
     [] when DelayBound > 0 -> true;
     [_H | _T] -> lists:any(fun(E) ->
                              E < MaxDelayIndex
                            end, State#state.delay_seq);
      _Else -> false
    end,
  {reply, Reply, State};

handle_call(next, _From, State) ->
  DelaySeq = State#state.delay_seq,
  DelayBound = State#state.delay_bound,
  MaxDelayIndex = State#state.max_delay_indx,

  NewDelaySeq =
    case DelaySeq of
      [] ->
        lists:seq(1, DelayBound);
      [_|_] ->
        {L1, L2} =
          lists:splitwith(fun(N) ->
                            N < MaxDelayIndex
                          end, DelaySeq),
        case L1 of
          [] ->
            L2;
          _Else ->
            Last = lists:last(L1),
            L1NewLast = Last + 1,
            L1_1 = lists:delete(Last, L1),
            NewL1 = L1_1 ++ [L1NewLast],
            {NewL2, _} = lists:mapfoldl(fun(_E, I) ->
                                          %% Sanity check
                                          true = I =< length(L2),
                                          {min(L1NewLast + I, 10), I+1}
                                        end, 1, L2),
            NewL1 ++ NewL2
        end
    end,
  Count = State#state.count,
  NewState = State#state{delay_seq = NewDelaySeq, current_delay_seq_indx = 1, count = Count+1},
  {reply, NewDelaySeq, NewState};

handle_call(get_next_delay_index, _From, State) ->
  DelaySeq = State#state.delay_seq,
  CurrDSIndex = State#state.current_delay_seq_indx,
  DelayBound = State#state.delay_bound,
  MaxDelayIndex = State#state.max_delay_indx,

  {L1, _L2} =
    lists:splitwith(fun(N) ->
                      N < MaxDelayIndex
                    end, DelaySeq),

  {NewState, DelayIndex} =
    if
      L1 == [] ->
        {State, 0};
      CurrDSIndex =< length(L1) ->
        if
          (CurrDSIndex >= 1) and (CurrDSIndex =< DelayBound) ->
            DelIdx = lists:nth(CurrDSIndex, DelaySeq),
            {State, DelIdx};
          true ->
            {State, 0}
        end;
      CurrDSIndex > length(L1) ->
        {State, 0}
    end,
  {reply, DelayIndex, NewState};

handle_call(spend_current_delay_index, _From, State) ->
  CurrDelSeqIndex = State#state.current_delay_seq_indx,
  NewDSIndex = CurrDelSeqIndex+1,
  NewState = State#state{current_delay_seq_indx = NewDSIndex},
  {reply, ok, NewState};

handle_call(is_end_current_delay_seq, _From, State = #state{delay_bound = DelayBound, current_delay_seq_indx = CurrentDSIndex}) ->
  Reply =
    if
      CurrentDSIndex >= DelayBound ->
        true;
      true ->
        false
    end,
  {reply, Reply, State};

handle_call(print_sequence, _From, State) ->
  io:format("~p, ~p", [State#state.delay_seq, State#state.count]),
  {reply, ok ,State}.

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