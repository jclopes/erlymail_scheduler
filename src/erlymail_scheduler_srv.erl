-module(erlymail_scheduler_srv).

-behaviour(gen_server).

-include("erlymail_scheduler_srv.hrl").

%% API interface
-export ([
    add/3,
    remove/1,
    list/0,
    send_ready/0,
    start_link/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_info/2,
    handle_cast/2,
    handle_call/3,
    code_change/3,
    terminate/2
]).

%% Internal functions
-export([worker/2]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, no_args, [])
.

% Time: tuple format {{Year, Month, Day}, {Hour, Minute, Second}}
% SmtpConn: term() as return by esmtp:conn/3
% Message: term() as return by esmtp_mime:msg/4
add(Time, SmtpConn, Message) ->
    gen_server:call(?MODULE, {add, {Time, SmtpConn, Message}})
.

remove(MessageId) ->
    gen_server:call(?MODULE, {remove, MessageId})
.

list() ->
    gen_server:call(?MODULE, list)
.

send_ready() ->
    gen_server:cast(?MODULE, send_ready)
.

%% ===================================================================
%% Server functions
%% ===================================================================

init(_) ->
    process_flag(trap_exit, true),
    timer:apply_interval(60000, ?MODULE, send_ready, []),
    State = #state{
        message_db = ets:new(messages, [{keypos, #db_row.id}]),
        time_index = gb_sets:new(),
        workers = dict:new()
    },
    {ok, State}
.

handle_call(
    {remove, MsgId},
    _From,
    #state{message_db=MsgDb, time_index=TimeIx}=S
) ->
    NTimeIx = case ets:lookup(MsgDb, MsgId) of
        [Row] ->
            gb_sets:delete({Row#db_row.time, MsgId}, TimeIx)
        ;
        _ -> TimeIx
    end,
    Reply = {ok, ets:delete(MsgDb, MsgId)},
    {reply, Reply, S#state{time_index=NTimeIx}}
;
handle_call(
    {add, {Time, SmtpConn, Message}},
    _From,
    #state{message_db=MsgDb, time_index=TimeIx, next_id=Id}=S
) ->
    Row = #db_row{id=Id, time=Time, msg={SmtpConn,Message}},
    ets:insert(MsgDb, Row),
    NTimeIx = gb_sets:add({Time, Id}, TimeIx),
    Reply = {ok, Message},
    {reply, Reply, S#state{time_index=NTimeIx, next_id=Id+1}}
;
handle_call(list, _From, #state{message_db=MsgDb, time_index=TimeIx}=S) ->
    Reply = {ok, ets:tab2list(MsgDb), TimeIx},
    {reply, Reply, S}
;
handle_call(Request, _From, State) ->
    Reply = {unknown, Request},
    {reply, Reply, State}
.

handle_cast(
    send_ready,
    #state{message_db=MsgDb, time_index=TimeIx, workers=Workers}=S
) ->
    Now = calendar:now_to_datetime(now()),

    io:format("~p~n~p~n", [Now, S]),   % TODO: remove debug line

    {NTimeIx, MsgIdList} = pop_msg_older(TimeIx, Now),
    NWorkers = lists:foldl(
        fun(MsgId, WrkDict) ->
            [Row] = ets:lookup(MsgDb, MsgId),
            {SmtpConn, Message} = Row#db_row.msg,
            Pid = spawn_link(?MODULE, worker, [SmtpConn, Message]),
            dict:store(Pid, MsgId, WrkDict)
        end,
        Workers,
        MsgIdList
    ),
    {noreply, S#state{time_index=NTimeIx, workers=NWorkers}}
;
handle_cast(_Msg, State) ->
    {noreply, State}
.

handle_info(
    {'EXIT', From, normal},
    #state{message_db=MsgDb, workers=Workers}=S
) ->
    % message successful delivery
    MsgId = dict:fetch(From, Workers),
    NWorkers = dict:erase(From, Workers),
    ets:delete(MsgDb, MsgId),
    {noreply, S#state{workers=NWorkers}}
;
handle_info(
    {'EXIT', From, Reason},
    #state{message_db=MsgDb, workers=Workers}=S
) ->
    % message fail delivery
    MsgId = dict:fetch(From, Workers),
    NWorkers = dict:erase(From, Workers),
    ets:delete(MsgDb, MsgId),
    % TODO: log and store message to notify user
    io:format("INFO: worker died with reason: ~p~n", [Reason]),
    {noreply, S#state{workers=NWorkers}}
;
handle_info(Info, State) ->
    io:format("INFO_MSG: ~p~n", [Info]),
    {noreply, State}
.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}
.

terminate(_Reason, _State) ->
    ok
.

%% ===================================================================
%% Internal functions
%% ===================================================================

% @spec (Gb_set, Datetime) -> {Gb_set, MsgList}
pop_msg_older(TimeIx, DateTime) ->
    pop_msg_older_acc(TimeIx, DateTime, [])
.

pop_msg_older_acc(TimeIx, DateTime, Acc) ->
    case gb_sets:is_empty(TimeIx) of
        true ->
            {TimeIx, Acc}
        ;
        false ->
            {{T, MsgId}, NTimeIx} = gb_sets:take_smallest(TimeIx),
            case T =< DateTime of
                true ->
                    io:format("Msg ~p is ready.", [MsgId]),
                    pop_msg_older_acc(NTimeIx, DateTime, [MsgId | Acc])
                ;
                false -> {TimeIx, Acc}
            end
    end
.

worker(SmtpConn, EmailMsg) ->
    Res = esmtp:send(SmtpConn, EmailMsg),
    io:format("Message sent! : ~p~n", [EmailMsg]),
    Res
.
