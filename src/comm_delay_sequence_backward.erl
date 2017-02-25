-module(comm_delay_sequence_backward).

-behaviour(gen_server).

%% API
-export([start_link/3,
        has_next/1,
        next/1,
        get_next_delay_index/1,
        spend_current_delay_index/1,
        is_end_current_delay_seq/1,
        get_delay_at_index/2,
        print_sequence/1,
        stop/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%%-define(SERVER, ?MODULE).

-record(state, {delay_seq=[], delay_bound, max_delay_indx, current_delay_seq_indx}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Server, DelayBound, MaxDelayIndex) -> %% Type :: regular | delayed
  %%Server = ?SERVER ++ Type,
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

get_delay_at_index(Server, Index) ->
  gen_server:call(Server, {get_delay_at_index, {Index}}).

print_sequence(Server) ->
  gen_server:call(Server, print_sequence).

stop(Server) ->
  gen_server:cast(Server, stop).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([DelayBound, MaxDelayIndex]) ->
  {ok, #state{delay_seq = [], delay_bound = DelayBound, max_delay_indx = MaxDelayIndex, current_delay_seq_indx = 0}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(has_next, _From, State) ->
  DelayBound = State#state.delay_bound,
  Reply =
    case State#state.delay_seq of
      [] when DelayBound > 0 -> true;
      [_H | _T] -> lists:any(fun(E) -> E>0 end, State#state.delay_seq);
      _Else -> false
    end,
  {reply, Reply, State};

handle_call(next, _From, State = #state{delay_seq = DelaySeq, delay_bound = DelayBound, max_delay_indx = MaxDelayIndex}) ->
  NewDelaySeq =
    case DelaySeq of
      [] ->
        if
          DelayBound > MaxDelayIndex ->
            L = lists:seq(0, MaxDelayIndex - 1),
            N = DelayBound - MaxDelayIndex,
            add_0_to_L(N, L);
          true ->
            lists:seq(MaxDelayIndex-DelayBound, MaxDelayIndex - 1)
        end;
      [_H | _T] ->
        {L1, [Item | Tail]} = lists:splitwith(fun(N) -> N =< 0 end, DelaySeq),
        L2 = [Item-1 | Tail],
        {NewL1, _} = lists:mapfoldl(fun(_E, I)->{max((Item-1)-length(L1)+I, 0), I+1} end, 0, L1),
        NewL1 ++ L2
    end,
  NewState = State#state{delay_seq = NewDelaySeq, current_delay_seq_indx = 1},
  {reply, NewDelaySeq, NewState};

handle_call(get_next_delay_index, _From, State) ->
  DelaySeq = State#state.delay_seq,
  CurrDSIndex = State#state.current_delay_seq_indx,
  DelayBound = State#state.delay_bound,
  {L1, L2} = lists:splitwith(fun(N) -> N =< 0 end, DelaySeq),
  DelayIndex = if
                 L2 == [] ->
                   NewState = State,
                   0;
                 CurrDSIndex > length(L1) -> %L1 == [] ->
                   NewState = State,
                   if
                     (CurrDSIndex >= 1) and (CurrDSIndex =< DelayBound) ->
                       lists:nth(CurrDSIndex, DelaySeq);
                     true -> 0
                   end;
                 CurrDSIndex =< length(L1) ->
                   [Item | _Tail] = L2,
                   NewState = State#state{current_delay_seq_indx = length(L1)+1},
                   Item
               end,
  {reply, DelayIndex, NewState};

handle_call(spend_current_delay_index, _From, State) ->
  CurrDelSeqIndex = State#state.current_delay_seq_indx,
  NewDSIndex = CurrDelSeqIndex+1,
  NewState = State#state{current_delay_seq_indx = NewDSIndex},
  {reply, ok, NewState};

handle_call(is_end_current_delay_seq, _From, State = #state{delay_bound = DelayBound, current_delay_seq_indx = CurrentDSIndex}) ->
  Reply = if
          CurrentDSIndex >= DelayBound -> true;
          true -> false
          end,
  {reply, Reply, State};

handle_call({get_delay_at_index, {Index}}, _From, State) ->
  Reply =if
        (Index >= 1) and (Index =< State#state.delay_bound)  -> lists:nth(Index, State#state.delay_seq);
        true -> 0
        end,
  {reply, Reply, State};

handle_call(print_sequence, _From, State) ->
  io:format("~p", [State#state.delay_seq]),
  {reply, ok ,State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(stop, State) ->
  {stop, normal, State}.

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_0_to_L(0, L) ->
  L;

add_0_to_L(N, L) ->
  NewL = add_0_to_L(N - 1, L),
  [0] ++ NewL.