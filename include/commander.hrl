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

-record(local_event, {
  event_no :: pos_integer(),
  event_dc :: dcid(),
  event_commit_time :: non_neg_integer(),
  event_snapshot_time :: dict(),
  event_txns :: [txid()]}).

-record(remote_event, {
  event_dc :: dcid(),
  event_node :: node(),
  event_original_dc :: dcid(),
  event_commit_time :: non_neg_integer(),
  event_snapshot_time :: dict(),
  event_txns :: [txid()]}).

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
                    txns_data :: dict(), %%txId -> [{local, localData}, {remote,list(partialTxns)}]
                    initial_exec :: execution(),
                    curr_exec :: execution(), %% Shows the scheduled execution is getting replayed
                    phase :: phase(),
                    %%% Used by Replayer
                    recent_tx :: txid(),
                    %txn_map :: dict(), %% {key:OriginalTxId{snapshot_time, server_pid}, val:NewInterDCTxn}
                    clusters :: [dc()],
                    replay_history :: [execution()], %% Consider keeping only the list of event indices in the original exec
                    exec_counter :: non_neg_integer(), %% Name the recorded files
                    %%% Used by Recorder to record the initial execution
                    upstream_events :: [upstream_event],
                    %%% Used by Scheduler
                    curr_delay_seq :: delay_seq()}).

-record(replay_state, {
  txns_data :: dict(), %%txId -> [{local, localData}, {remote,list(partialTxns)}]
  txn_map :: dict(),
  sch_count :: pos_integer(),
  clusters :: list(),
  dcs :: list(dc())
  %%history :: list(event())
}).

-record(scheduler_state, {
  delayed_count :: non_neg_integer(),
  delay_bound :: non_neg_integer(),
  event_count :: pos_integer(),
  curr_delay_seq :: delay_seq(),
  curr_sch :: list(term()),
  dependency :: dict(),
  delayed :: list(term()),
  orig_sch :: list(event()),
  orig_sch_sym :: list(term()),
  curr_event_index :: non_neg_integer(),
  logical_ss :: dict(),
  dcs :: list()
}).