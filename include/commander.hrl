-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-define(DEV(N), list_to_atom(lists:concat(["dev", N, "@127.0.0.1"]))).
-define(NODES_PATH, (comm_config:fetch(cluster_path))).

-type dc() :: [node()].

-record(upstream_event, {event_no :: pos_integer(),
                        event_dc :: dcid(),
                        event_commit_time :: non_neg_integer(),
                        event_snapshot_time :: dict(),
                        event_data :: [term()],
                        event_txns :: [txid()]}).

-type upstream_event() :: #upstream_event{}.

-record(downstream_event, {event_dc :: dcid(),
                           event_node :: node(),
                           event_original_dc :: dcid(),
                           event_commit_time :: non_neg_integer(),
                           event_snapshot_time :: dict(),
                           event_data :: [term()],
                           event_txns :: [txid()]}).

-type downstream_event() :: #downstream_event{}.

-record(execution, {id :: non_neg_integer(),
                    trace :: [term()]}). %% Each execution trace is a list of Interdc_txn() or txn()
-type execution() :: #execution{}.
-type delay_seq() :: [non_neg_integer()].
-type phase() :: recording | replaying.

-record(exec_state, {test_module :: atom(),
                     exec_events :: list(pos_integer())}).
-type exec_state() :: #exec_state{}.

-record(comm_state, {initial_exec :: execution(),
                    curr_exec :: execution(),
                    curr_delay_seq :: delay_seq(),
                    replay_history :: [execution()], %%[history_record()],
                    phase :: phase(),
                    exec_counter :: non_neg_integer(),
                    curr_exec_state :: exec_state(),
                    upstream_events :: [upstream_event]}).