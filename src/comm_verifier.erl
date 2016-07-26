%%%-------------------------------------------------------------------
%%% @author maryam
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jul 2016 12:27 AM
%%%-------------------------------------------------------------------
-module(comm_verifier).
-author("maryam").

-include("commander.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2,
  check_object_invariant/1]).

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
-spec(start_link(Mod :: atom(), Objs::list()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Mod, Objs) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Mod, Objs], []).

check_object_invariant(Event) ->
  gen_server:call(?SERVER, {check_object_invariant, {Event}}).
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
  {ok, State :: #verifier_state{}} | {ok, State :: #verifier_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([Mod, Objs]) ->
  InitState = #verifier_state{test_module = Mod, app_objects = Objs},
  {ok, InitState}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #verifier_state{}) ->
  {reply, Reply :: term(), NewState :: #verifier_state{}} |
  {reply, Reply :: term(), NewState :: #verifier_state{}, timeout() | hibernate} |
  {noreply, NewState :: #verifier_state{}} |
  {noreply, NewState :: #verifier_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #verifier_state{}} |
  {stop, Reason :: term(), NewState :: #verifier_state{}}).
handle_call({check_object_invariant, {Event}}, _From, State) ->
  Objects = State#verifier_state.app_objects,
  TestModule = State#verifier_state.test_module,
  EventNode = get_event_node(Event),
  Res = try
          TestModule:handle_object_invariant(EventNode, Objects)
        of
          true -> true
        catch
            Exception:Reason -> {caught, Exception, Reason}
        end,
  {reply, Res, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #verifier_state{}) ->
  {noreply, NewState :: #verifier_state{}} |
  {noreply, NewState :: #verifier_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #verifier_state{}}).
handle_cast(_Request, State) ->
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
-spec(handle_info(Info :: timeout() | term(), State :: #verifier_state{}) ->
  {noreply, NewState :: #verifier_state{}} |
  {noreply, NewState :: #verifier_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #verifier_state{}}).
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
    State :: #verifier_state{}) -> term()).
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #verifier_state{},
    Extra :: term()) ->
  {ok, NewState :: #verifier_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_event_node(Event) ->
  case comm_utilities:type(Event) of
    local ->
      Event#local_event.event_node;
    remote ->
      Event#remote_event.event_node
  end.