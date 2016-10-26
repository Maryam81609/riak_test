-module(comm_utilities).

-include("commander.hrl").

-compile(export_all).

get_home_dir() ->
    {ok, [[HomeDir]]} = init:get_argument(home),
    HomeDir.

is_initial_exec(History) when is_list(History) ->
    case History of
        [] -> true;
        _ -> false
    end.

get_exec_name(ExecId) ->
    FileName = io_lib:format("exec_~b",[ExecId]),
    FileName.

get_full_name(Name, Phase) ->
    RootDir = get_home_dir() ++ "/commander",
    Dir = case Phase of
            record ->
                RootDir ++ "/history/";
            _logging ->
                RootDir ++ "/log/"
          end,
    FullName = Dir ++ Name,
    ok = filelib:ensure_dir(FullName),
    FullName.

get_symbolic_sch(OrigSch) ->
    TrimedSch = lists:map(fun(E) -> trim(E) end, OrigSch),
    RevTrimedSch = lists:reverse(TrimedSch),
    lists:reverse(remove_dups(RevTrimedSch)).

%%%===== Internals
remove_dups([]) -> [];
remove_dups([H | T]) -> [H | [E || E <- remove_dups(T), not_equal(H, E)]].

not_equal(E1, E2) ->
    if
        (is_record(E1, remote_event) and is_record(E2, remote_event)) ->
            not ((E1#remote_event.event_dc == E2#remote_event.event_dc) and (E1#remote_event.event_txns == E2#remote_event.event_txns));
        true ->
            true
    end.

trim(Event) ->
    if
        is_record(Event, upstream_event) -> trim(local_event, Event);
        is_record(Event, downstream_event) -> trim(remote_event, Event)
    end.

trim(local_event, Event) ->
    EvNo = Event#upstream_event.event_no,
    EvNode = Event#upstream_event.event_node,
    EvDc = Event#upstream_event.event_dc,
    EvCT = Event#upstream_event.event_commit_time,
    EvST = Event#upstream_event.event_snapshot_time,
    EvTxns = Event#upstream_event.event_txns,
    #local_event{event_no = EvNo, event_node = EvNode, event_dc = EvDc, event_commit_time = EvCT, event_snapshot_time = EvST, event_txns =EvTxns};

trim(remote_event, Event) ->
    EvDc = Event#downstream_event.event_dc,
    EvNode = Event#downstream_event.event_node,
    EvOrigDc = Event#downstream_event.event_original_dc,
    EvCT = Event#downstream_event.event_commit_time,
    EvST = Event#downstream_event.event_snapshot_time,
    EvTxns = Event#downstream_event.event_txns,
    #remote_event{event_dc = EvDc, event_node = EvNode, event_original_dc = EvOrigDc, event_commit_time = EvCT, event_snapshot_time = EvST, event_txns = EvTxns}.

reset_dcs(Clusters) ->
    io:format("~nReseting test environment...~nCluaters: ~p~n", [Clusters]),
    Clean = rt_config:get(clean_cluster, true),
    Clusters1 = common:clean_clusters(Clusters),
    ok = common:setup_dc_manager(Clusters1, Clean),
    Clusters1.

type(Event) ->
    if
        is_record(Event, local_event) -> local;
        is_record(Event, remote_event) -> remote
    end.

get_all_dcs(Clusters) ->
    lists:map(fun(Cluster) ->
                Node = hd(Cluster),
                rpc:call(Node, dc_utilities, get_my_dc_id, [])
              end, Clusters).

get_all_partitions(ReplayerState) ->
    Clusters = ReplayerState#replay_state.clusters,
    HeadNodes = lists:map(fun(Cluster) ->
                            hd(Cluster)
                          end, Clusters),

    lists:map(fun(HeadNode) ->
                rpc:call(HeadNode, dc_utilities, get_all_partitions, [])
              end, HeadNodes).