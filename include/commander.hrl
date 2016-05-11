-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-define(DEV(N), list_to_atom(lists:concat(["dev", N, "@127.0.0.1"]))).
-define(NODES_PATH, (comm_config:fetch(cluster_path))).
-define(TEST_NODE, 'riak_test@127.0.0.1').

-type dc() :: [node()].

-record(upstream_event, {event_no :: pos_integer(),
                        event_data :: term(),
                        event_dc :: dcid(),
                        %% TODO: merge following fields into one field: dict({txid(), {txn_commit_time :: non_neg_integer(), txn_snapshot_time :: dict()}})
                        event_commit_time :: non_neg_integer(),
                        event_snapshot_time :: dict(),
                        event_txns :: [txid()]}).

-type upstream_event() :: #upstream_event{}.

-record(downstream_event, {event_dc :: dcid(),
                           event_node :: node(),
                           event_original_dc :: dcid(),
                           event_commit_time :: non_neg_integer(),
                           event_snapshot_time :: dict(),
                           event_data :: [term()], %% => event_txns :: [inetrdc_txn()]; event_txns list contains partial transactions
                           event_txns :: [txid()]}). %% => event_txid :: txid()

-type downstream_event() :: #downstream_event{}.

-type event() :: upstream_event | downstream_event.

-record(execution, {id :: non_neg_integer(),
                    trace :: [event()]}).
-type execution() :: #execution{}.
-type delay_seq() :: [non_neg_integer()].
-type phase() :: record | replay.

-record(exec_state, {test_module :: atom(),
                     exec_events :: list(pos_integer())}).
-type exec_state() :: #exec_state{}.

-record(comm_state, {%%% Common fields
                    initial_exec :: execution(),
                    curr_exec :: execution(), %% Shows the scheduled execution is getting replayed
                    phase :: phase(),
                    %%% Used by Replayer
                    recent_tx :: txid(),
                    txn_map :: dict(), %% {key:OriginalTxId{snapshot_time, server_pid}, val:NewInterDCTxn}
                    clusters :: [dc()],
                    replay_history :: [execution()], %% Consider keeping only the list of event indices in the original exec
                    exec_counter :: non_neg_integer(), %% Name the recorded files
                    %%% Used by Recorder to record the initial execution
                    upstream_events :: [upstream_event],
                    %%% Used by Scheduler
                    curr_delay_seq :: delay_seq()}).