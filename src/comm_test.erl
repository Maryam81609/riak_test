-module(comm_test).

-export([event/2]).

-callback handle_event(Args :: list(term())) -> Result :: term() | tuple(error, Reason :: string()).

-spec(event(Module :: atom(), Args :: list(term())) -> term()).
event(Module, Args) ->
    Data = {Module, Args},
    ok = commander:get_upstream_event_data(Data),
    Res = Module:handle_event(Args),
    Res.