-ifndef(ERLYMAIL_SCHEDULER_SRV_HRL).
-define(ERLYMAIL_SCHEDULER_SRV_HRL, "erlymail_scheduler.hrl").

-record(state, {
    message_db,     % ets table that stores the messages to be sent
    next_id = 1,    % nonce to be used as db_row id
    time_index,     % time sorted index of the message_db entries
    workers         % keeps track of spawn jobs {pid, db_row_id}
}).

-record(db_row, {
    id,             % unique ID
    time,           % time to send the message
    msg             % message data
}).

-endif.
