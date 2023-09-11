-module(test).
-export([setup/0, gen_queues/2, stop/1,
         declare_queue/1,
         declare_queue/3,
         declare_queue_1/0,
         declare_queue_2/0,
         declare_queue_3/0,
         publish/1,
         read/1,
         run/1,
         pub/1,
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


connection() ->
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{host = "localhost"}),
    Connection.

channel(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Channel.

basic_get(Channel, Queue) ->
    amqp_channel:call(Channel,
                      #'basic.get'{queue = Queue, no_ack = false}).

basic_publish(Channel, Queue) ->
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = Queue},
                      #amqp_msg{payload = <<"Hello World!">>}).

declare(Channel, Queue) ->
    amqp_channel:call(Channel,
                      #'queue.declare'{
                         queue = Queue,
                         durable = true}).
declare(Channel, Queue, Type) ->
    amqp_channel:call(Channel,
                      #'queue.declare'{
                         queue = Queue,
                         durable = true,
                         arguments =[{<<"x-queue-type">>,
                                      longstr,
                                      list_to_binary(Type)}]}).

sub_n(Q, N) ->
    #{channel := Channel} = _Config = setup(),
    lists:foreach(fun(_) -> sub(Channel, Q) end, lists:seq(1, N)),
    sub(Channel, Q).


pub(Channel, Key) ->
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = Key},
                      #amqp_msg{payload = <<"Hello World!">>}).













setup() ->
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{host = "localhost"}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #{connection => Connection, channel => Channel}.


gen_queues(N, #{connection := _Connection, channel := Channel}) ->
    [amqp_channel:call(Channel,
                       #'queue.declare'{
                          queue = list_to_binary("QQ_"++integer_to_list(I)),
                          durable = true
                          })
     || I <- lists:seq(1,N)].

stop(#{connection := Connection, channel := Channel}) ->
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).

wrapper(F) ->
    #{channel := Channel} = Config = setup(),
    R = F(Channel),
    stop(Config),
    R.

run(Queue) ->
    #{channel := Channel} = Config = setup(),
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 10}),
    declare(Channel, Queue, "aws-journal"),
    Config.

sub(Channel, Queue) ->
    Method = #'basic.consume'{queue = Queue,
                              no_ack = true},
    amqp_channel:subscribe(Channel, Method, self()).



pub(#{connection := _Connection, channel := Channel}) ->
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = <<"foobar">>},
                      #amqp_msg{payload = <<"Hello World!">>}).




declare_queue(Name) ->
    declare_queue(Name, "quorum", true).
declare_queue(Name, Type, Durable) ->
    wrapper(fun(Channel) ->
                    amqp_channel:call(Channel,
                                      #'queue.declare'{
                                         queue = list_to_binary(Name),
                                         durable = Durable,
                                         arguments =[{<<"x-queue-type">>,
                                                      longstr,
                                                      list_to_binary(Type)}]})
            end).


publish(Name) ->
    wrapper(fun(Channel) ->
                    amqp_channel:cast(Channel,
                                      #'basic.publish'{
                                         exchange = <<"">>,
                                         routing_key = list_to_binary(Name)},
                                      #amqp_msg{payload = <<"Hello World!">>})
            end).

read(Name) ->
        wrapper(fun(Channel) ->
                        Method = #'basic.consume'{queue = list_to_binary(Name),
                                                  no_ack = true},
                        amqp_channel:subscribe(Channel, Method, self())
                end).































declare_queue_1() ->
    wrapper(fun(Channel) ->
                    amqp_channel:call(Channel,
                                      #'queue.declare'{
                                         queue = <<"jeff">>,
                                         durable = true,
                                         arguments =[{<<"x-queue-type">>,
                                                      longstr,
                                                      <<"quorum">>}]})
            end).


declare_queue_2() ->
    wrapper(fun(Channel) ->
                    amqp_channel:call(Channel,
                                      #'queue.declare'{
                                         queue = <<"azure-is-better">>,
                                         durable = true,
                                         arguments =[{<<"x-queue-type">>,
                                                      longstr,
                                                      <<"quorum">>}]})
            end).


declare_queue_3() ->
    wrapper(fun(Channel) ->
                    amqp_channel:call(Channel,
                                      #'queue.declare'{
                                         queue = <<"stream-queue">>,
                                         durable = true,
                                         arguments =[{<<"x-queue-type">>,
                                                      longstr,
                                                      <<"stream">>}]})
            end).
