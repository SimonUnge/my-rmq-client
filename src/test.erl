-module(test).
-export([setup/0, stop/1,
         pub/2,
         sub/2,
         sub_n/2
        ]).

-export([connection/0,
         channel/1,
         declare/2,
         declare/3,
         basic_get/2,
         basic_publish/2]).

-include_lib("amqp_client/include/amqp_client.hrl").


setup() ->
    Connection = connection(),
    Channel = channel(Connection),
    #{connection => Connection, channel => Channel}.

connection() ->
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{host = "localhost"}),
    Connection.

channel(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Channel.

basic_get(Channel, Queue) ->
    amqp_channel:call(Channel,
                      #'basic.get'{queue = list_to_binary(Queue), no_ack = false}).

basic_publish(Channel, Queue) ->
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = list_to_binary(Queue)},
                      #amqp_msg{payload = <<"Hello World!">>}).

declare(Channel, Queue) ->
    amqp_channel:call(Channel,
                      #'queue.declare'{
                         queue = list_to_binary(Queue),
                         durable = true}).
declare(Channel, Queue, Type) ->
    amqp_channel:call(Channel,
                      #'queue.declare'{
                         queue = list_to_binary(Queue),
                         durable = true,
                         arguments = [{<<"x-queue-type">>,
                                       longstr,
                                       list_to_binary(Type)}]}).

sub_n(Q, N) ->
    #{channel := Channel} = _Config = setup(),
    lists:foreach(fun(_) -> sub(Channel, Q) end, lists:seq(1, N)),
    sub(Channel, Q).

sub(Channel, Queue) ->
    Method = #'basic.consume'{queue = list_to_binary(Queue),
                              no_ack = true},
    amqp_channel:subscribe(Channel, Method, self()).

pub(Channel, Key) ->
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = Key},
                      #amqp_msg{payload = <<"Hello World!">>}).

stop(#{connection := Connection, channel := Channel}) ->
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).

wrapper(F) ->
    #{channel := Channel} = Config = setup(),
    R = F(Channel),
    stop(Config),
    R.
