-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-define(DEV(N), list_to_atom(lists:concat(["dev", N, "@127.0.0.1"]))).
-define(NODES_PATH, (comm_config:fetch(cluster_path))).

-type dc() :: [node()].

-record(execution, {id :: non_neg_integer(),
                    trace :: [term()]}). %% Each execution trace is a list of Interdc_txn() or txn()
-type execution() :: #execution{}.
-type delay_seq() :: [non_neg_integer()].
-type phase() :: recording | replaying.

-record(comm_state, {initial_exec :: execution(), %%base_execs :: [execution()], %%curr_base_exec_idx :: non_neg_integer(), %% Index is 0 in the initial state
                    curr_exec :: execution(),
                    curr_delay_seq :: delay_seq(),
                    replay_history :: [execution()], %%[history_record()],
                    phase :: phase(),
                    exec_counter :: non_neg_integer()}).

%%-record(history_record, {exec_id :: non_neg_integer(),
  %%                       replayed_delays :: [delay_seq()]}).

%%-type history_record() :: #history_record{}.