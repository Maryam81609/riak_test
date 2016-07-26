%%%-------------------------------------------------------------------
%%% @author maryam
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. May 2016 9:39 PM
%%%-------------------------------------------------------------------
-module(comm_delay_sequence).
-author("maryam").

-behaviour(gen_server).

%% API
-export([start_link/2,
        has_next/0,
        next/0,
        get_next_delay_index/0,
        spend_current_delay_index/0,
        is_end_current_delay_seq/0,
        get_delay_at_index/1,
        print_sequence/0, stop/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {delay_seq=[], delay_bound, max_delay_indx, current_delay_seq_indx}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(DelayBound::non_neg_integer(), MaxDelayIndex::pos_integer()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(DelayBound, MaxDelayIndex) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [DelayBound, MaxDelayIndex], []).

has_next() ->
  gen_server:call(?SERVER, has_next).

next() ->
  gen_server:call(?SERVER, next).

get_next_delay_index() ->
  gen_server:call(?SERVER, get_next_delay_index).

spend_current_delay_index() ->
  gen_server:call(?SERVER, spend_current_delay_index).

is_end_current_delay_seq() ->
  gen_server:call(?SERVER, is_end_current_delay_seq).

get_delay_at_index(Index) ->
  gen_server:call(?SERVER, {get_delay_at_index, {Index}}).

print_sequence() ->
  gen_server:call(?SERVER, print_sequence).

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
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([DelayBound, MaxDelayIndex]) ->
  {ok, #state{delay_seq = [], delay_bound = DelayBound, max_delay_indx = MaxDelayIndex, current_delay_seq_indx = 0}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
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
      [] -> lists:seq(MaxDelayIndex-DelayBound, MaxDelayIndex - 1);
      [_H | _T] ->
        {L1, [Item | Tail]} = lists:splitwith(fun(N) -> N =< 0 end, DelaySeq),
        L2 = [Item-1 | Tail],
        {NewL1, _} = lists:mapfoldl(fun(_E, I)->{max((Item-1)-length(L1)+I, 0), I+1} end, 0, L1),
        NewL1 ++ L2
    end,
  NewState = State#state{delay_seq = NewDelaySeq, current_delay_seq_indx = 1},
  {reply, ok, NewState};

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
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
    State :: #state{}) -> term()).
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
