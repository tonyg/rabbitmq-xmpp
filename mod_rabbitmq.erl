%% RabbitMQ gateway module for ejabberd.
%% Based on ejabberd's mod_echo.erl
%%---------------------------------------------------------------------------
%% @author Tony Garnock-Jones <tonyg@lshift.net>
%% @author LShift Ltd. <query@lshift.net>
%% @copyright 2008 Tony Garnock-Jones and LShift Ltd.
%% @license
%%
%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%% General Public License for more details.
%%                         
%% You should have received a copy of the GNU General Public License
%% along with this program; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
%% 02111-1307 USA
%%---------------------------------------------------------------------------
%%
%% @doc RabbitMQ gateway module for ejabberd.

-module(mod_rabbitmq).

-behaviour(gen_server).
-behaviour(gen_mod).

-export([start_link/2, start/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([route/3]).
-export([consumer_init/4, consumer_main/1]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include_lib("rabbitmq_server/include/rabbit.hrl").

-define(VHOST, <<"/">>).
-define(REALM, #resource{virtual_host = ?VHOST, kind = realm, name = <<"/data">>}).
-define(XNAME(Name), #resource{virtual_host = ?VHOST, kind = exchange, name = Name}).
-define(QNAME(Name), #resource{virtual_host = ?VHOST, kind = queue, name = Name}).

-record(state, {host}).
-record(rabbitmq_consumer_process, {queue, pid}).
-record(consumer_state, {lserver, consumer_tag, queue, priorities}).

-define(PROCNAME, ejabberd_mod_rabbitmq).
-define(TABLENAME, ?PROCNAME).

start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    mnesia:create_table(rabbitmq_consumer_process,
			[{attributes, record_info(fields, rabbitmq_consumer_process)}]),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    ChildSpec = {Proc,
		 {?MODULE, start_link, [Host, Opts]},
		 temporary,
		 1000,
		 worker,
		 [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:call(Proc, stop),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).

%%---------------------------------------------------------------------------

init([Host, Opts]) ->
    case catch rabbit_mnesia:create_tables() of
	{error, {table_creation_failed, _, _, {already_exists, _}}} ->
	    ok;
	ok ->
	    ok
    end,
    ok = rabbit:start(),

    MyHost = gen_mod:get_opt_host(Host, Opts, "rabbitmq.@HOST@"),
    ejabberd_router:register_route(MyHost, {apply, ?MODULE, route}),

    probe_queues(MyHost),

    {ok, #state{host = MyHost}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({route, From, To, Packet}, State) ->
    safe_route(non_shortcut, From, To, Packet),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ejabberd_router:unregister_route(State#state.host),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------------------------------------------

route(From, To, Packet) ->
    safe_route(shortcut, From, To, Packet).

safe_route(ShortcutKind, From, To, Packet) ->
    ?INFO_MSG("~p~n~p ->~n~p~n~p", [ShortcutKind, From, To, Packet]),
    case catch do_route(From, To, Packet) of
	{'EXIT', Reason} ->
	    ?ERROR_MSG("~p~nwhen processing: ~p",
		       [Reason, {From, To, Packet}]);
	_ ->
	    ok
    end.

do_route(#jid{lserver = FromServer} = From,
	 #jid{lserver = ToServer} = To,
	 {xmlelement, "presence", _, _})
  when FromServer == ToServer ->
    %% Break tight loops by ignoring these presence packets.
    ?WARNING_MSG("Tight presence loop between~n~p and~n~p~nbroken.",
		 [From, To]),
    ok;
do_route(From, #jid{luser = ""} = To, {xmlelement, "presence", _, _} = Packet) ->
    case xml:get_tag_attr_s("type", Packet) of
	"subscribe" ->
	    send_presence(To, From, "unsubscribed");
	"subscribed" ->
	    send_presence(To, From, "unsubscribe"),
	    send_presence(To, From, "unsubscribed");
	"unsubscribe" ->
	    send_presence(To, From, "unsubscribed");

	"probe" ->
	    send_presence(To, From, "");

	_Other ->
	    ?INFO_MSG("Other kind of presence for empty-user JID~n~p", [Packet])
    end,
    ok;
do_route(From, To, {xmlelement, "presence", _, _} = Packet) ->
    QNameBin = jid_to_qname(From),
    XNameBin = jid_to_xname(To),
    case xml:get_tag_attr_s("type", Packet) of
	"subscribe" ->
	    case rabbit_exchange:lookup(?XNAME(XNameBin)) of
		{ok, _X} -> send_presence(To, From, "subscribe");
		{error, not_found} -> send_presence(To, From, "unsubscribed")
	    end;
	"subscribed" ->
	    case check_and_bind(XNameBin, QNameBin) of
		true ->
		    send_presence(To, From, "subscribed"),
		    send_presence(To, From, "");
		false ->
		    send_presence(To, From, "unsubscribed"),
		    send_presence(To, From, "unsubscribe")
	    end;
	"unsubscribe" ->
	    maybe_unsub(From, To, XNameBin, QNameBin),
	    send_presence(To, From, "unsubscribed");
	"unsubscribed" ->
	    maybe_unsub(From, To, XNameBin, QNameBin);

	"" ->
	    start_consumer(QNameBin, From, To#jid.lserver, extract_priority(Packet));
	"unavailable" ->
	    stop_consumer(QNameBin, From, false);

	"probe" ->
	    case is_subscribed(XNameBin, QNameBin) of
		true ->
		    send_presence(To, From, "");
		false ->
		    ok
	    end;

	_Other ->
	    ?INFO_MSG("Other kind of presence~n~p", [Packet])
    end,
    ok;
do_route(From, To, {xmlelement, "message", _, _} = Packet) ->
    case xml:get_subtag_cdata(Packet, "body") of
	"" ->
	    ?DEBUG("Ignoring message with empty body", []);
	Body ->
	    XNameBin = jid_to_xname(To),
	    case To#jid.luser of
		"" ->
		    send_command_reply(To, From, do_command(To, From, Body, parse_command(Body)));
		_ ->
		    case xml:get_tag_attr_s("type", Packet) of
			"error" ->
			    ?ERROR_MSG("Received error message~n~p -> ~p~n~p", [From, To, Packet]);
			_ ->
			    rabbit_exchange:simple_publish(false, false, ?XNAME(XNameBin), <<>>,
							   <<"text/plain">>,
							   list_to_binary(Body))
		    end
	    end
    end,
    ok;
do_route(_From, _To, _Packet) ->
    ?INFO_MSG("**** DROPPED", []),
    ok.

extract_priority(Packet) ->
    case xml:get_subtag_cdata(Packet, "priority") of
	"" ->
	    0;
	S ->
	    list_to_integer(S)
    end.

jid_to_qname(#jid{luser = U, lserver = S}) ->
    list_to_binary(U ++ "@" ++ S).

jid_to_xname(#jid{luser = U}) ->
    list_to_binary(U).

maybe_unsub(From, To, XNameBin, QNameBin) ->
    case is_subscribed(XNameBin, QNameBin) of
	true ->
	    do_unsub(From, To, XNameBin, QNameBin);
	false ->
	    ok
    end,
    ok.

do_unsub(QJID, XJID, XNameBin, QNameBin) ->
    send_presence(XJID, QJID, "unsubscribed"),
    send_presence(XJID, QJID, "unsubscribe"),
    case unbind_and_delete(XNameBin, QNameBin) of
	no_subscriptions_left ->
	    stop_consumer(QNameBin, QJID, true),
	    ok;
	subscriptions_remain ->
	    ok
    end.

get_bound_queues(XNameBin) ->
    XName = ?XNAME(XNameBin),
    [QNameBin ||
	#binding{handlers = Handlers} <- mnesia:read({binding, {XName, fanout}}),
	#handler{queue = #resource{name = QNameBin}} <- Handlers].

unsub_all(XNameBin, ExchangeJID) ->
    {atomic, QNameBins} =
	mnesia:transaction(
	  fun () ->
		  BoundQueues = get_bound_queues(XNameBin),
		  rabbit_exchange:delete(?XNAME(XNameBin), false),
		  BoundQueues
	  end),
    ?INFO_MSG("unsub_all~n~p~n~p~n~p", [XNameBin, ExchangeJID, QNameBins]),
    lists:foreach(fun (QNameBin) ->
			  do_unsub(jlib:string_to_jid(binary_to_list(QNameBin)),
				   ExchangeJID,
				   XNameBin,
				   QNameBin)
		  end, QNameBins),
    ok.

send_presence(From, To, "") ->
    ?INFO_MSG("Sending sub reply of type ((available))~n~p -> ~p", [From, To]),
    ejabberd_router:route(From, To, {xmlelement, "presence", [], []});
send_presence(From, To, TypeStr) ->
    ?INFO_MSG("Sending sub reply of type ~p~n~p -> ~p", [TypeStr, From, To]),
    ejabberd_router:route(From, To, {xmlelement, "presence", [{"type", TypeStr}], []}).

send_message(From, To, TypeStr, BodyStr) ->
    XmlBody = {xmlelement, "message",
	       [{"type", TypeStr},
		{"from", jlib:jid_to_string(From)},
		{"to", jlib:jid_to_string(To)}],
	       [{xmlelement, "body", [],
		 [{xmlcdata, BodyStr}]}]},
    ?INFO_MSG("Delivering ~p -> ~p~n~p", [From, To, XmlBody]),
    ejabberd_router:route(From, To, XmlBody).

is_subscribed(XNameBin, QNameBin) ->
    XName = ?XNAME(XNameBin),
    case rabbit_amqqueue:lookup(?QNAME(QNameBin)) of
	{ok, #amqqueue{binding_specs = Specs}} ->
	    lists:any(fun 
			  (#binding_spec{exchange_name = N}) when N == XName -> true;
			  (_) -> false
		      end, Specs);
	{error, not_found} ->
	    false
    end.

check_and_bind(XNameBin, QNameBin) ->
    ?DEBUG("Checking ~p ~p", [XNameBin, QNameBin]),
    case rabbit_exchange:lookup(?XNAME(XNameBin)) of
	{ok, _X} ->
	    ?DEBUG("... exists", []),
	    #amqqueue{} = rabbit_amqqueue:declare(?REALM, QNameBin, true, false, []),
	    {ok, _NBindings} =
		rabbit_amqqueue:add_binding(?QNAME(QNameBin), ?XNAME(XNameBin), <<>>, []),
	    true;
	{error, not_found} ->
	    ?DEBUG("... not present", []),
	    false
    end.

unbind_and_delete(XNameBin, QNameBin) ->
    ?DEBUG("Unbinding ~p ~p", [XNameBin, QNameBin]),
    QName = ?QNAME(QNameBin),
    case rabbit_amqqueue:delete_binding(QName, ?XNAME(XNameBin), <<>>, []) of
	{error, _} ->
	    ?DEBUG("... queue, binding or exchange not found. Ignoring", []),
	    no_subscriptions_left;
	{ok, 0} ->
	    ?INFO_MSG("... and deleting", []),
	    case rabbit_amqqueue:lookup(QName) of
		{ok, Q} -> rabbit_amqqueue:delete(Q, false, false);
		{error, not_found} -> ok
	    end,
	    ?INFO_MSG("here", []),
	    no_subscriptions_left;
	{ok, _PositiveCountOfBindings} ->
	    ?INFO_MSG("... and leaving the queue alone", []),
	    subscriptions_remain
    end.

all_exchanges() ->
    [XNameBin ||
	#exchange{name = #resource{name = XNameBin}, type = fanout}
	    <- rabbit_exchange:list_vhost_exchanges(?VHOST)].

probe_queues(Server) ->
    probe_queues(Server, rabbit_amqqueue:list_vhost_queues(?VHOST)).

probe_queues(_Server, []) ->
    ok;
probe_queues(Server, [#amqqueue{name = #resource{name = QNameBin},
				binding_specs = Specs} | Rest]) ->
    ?INFO_MSG("**** Probing ~p", [QNameBin]),
    case jlib:string_to_jid(binary_to_list(QNameBin)) of
	error ->
	    probe_queues(Server, Rest);
	JID ->
	    probe_bindings(Server, JID, Specs),
	    probe_queues(Server, Rest)
    end.

probe_bindings(_Server, _JID, []) ->
    ok;
probe_bindings(Server, JID,
	       [#binding_spec{exchange_name = #resource{name = XNameBin}}
		| Rest]) ->
    ?DEBUG("**** Probing ~p ~p ~p", [JID, XNameBin, Server]),
    SourceJID = jlib:make_jid(binary_to_list(XNameBin), Server, ""),
    send_presence(SourceJID, JID, "probe"),
    send_presence(SourceJID, JID, ""),
    probe_bindings(Server, JID, Rest).

start_consumer(QNameBin, JID, Server, Priority) ->
    mnesia:transaction(
      fun () ->
	      case mnesia:read({rabbitmq_consumer_process, QNameBin}) of
		  [#rabbitmq_consumer_process{pid = Pid}] ->
		      Pid ! {presence, JID, Priority},
		      ok;
		  [] ->
		      %% TODO: Link into supervisor
		      Pid = spawn(?MODULE, consumer_init, [QNameBin, JID, Server, Priority]),
		      mnesia:write(#rabbitmq_consumer_process{queue = QNameBin, pid = Pid}),
		      ok
	      end
      end),
    ok.

stop_consumer(QNameBin, JID, AllResources) ->
    mnesia:transaction(
      fun () ->
	      case mnesia:read({rabbitmq_consumer_process, QNameBin}) of
		  [#rabbitmq_consumer_process{pid = Pid}] ->
		      Pid ! {unavailable, JID, AllResources},
		      ok;
		  [] ->
		      ok
	      end
      end),
    ok.

consumer_init(QNameBin, JID, Server, Priority) ->
    ?INFO_MSG("**** starting consumer for ~p ~p ~p", [QNameBin, JID, Priority]),
    ConsumerTag = rabbit_misc:binstring_guid("amq.xmpp"),
    rabbit_amqqueue:with(?QNAME(QNameBin),
			 fun(Q) ->
				 rabbit_amqqueue:basic_consume(
				   Q, true, self(), self(),
				   ConsumerTag, false, undefined)
			 end),
    ?MODULE:consumer_main(#consumer_state{lserver = Server,
					  consumer_tag = ConsumerTag,
					  queue = QNameBin,
					  priorities = [{-Priority, JID}]}).

jids_equal_upto_resource(J1, J2) ->
    jlib:jid_remove_resource(J1) == jlib:jid_remove_resource(J2).

consumer_main(#consumer_state{priorities = Priorities} = State) ->
    ?INFO_MSG("**** consumer ~p", [State]),
    receive
	{unavailable, JID, AllResources} ->
	    {atomic, NewState} =
		mnesia:transaction(
		  fun () ->
			  NewPriorities =
			      case AllResources of
				  true ->
				      lists:filter(fun ({_, J}) ->
							   not jids_equal_upto_resource(J, JID)
						   end, Priorities);
				  false ->
				      lists:keydelete(JID, 2, Priorities)
			      end,
			  case NewPriorities of
			      [] ->
				  mnesia:delete({rabbitmq_consumer_process,
						 State#consumer_state.queue}),
				  terminate;
			      _ ->
				  State#consumer_state{priorities = NewPriorities}
			  end
		  end),
	    case NewState of
		terminate ->
		    ?INFO_MSG("**** terminating consumer ~p", [State#consumer_state.queue]),
		    consumer_done(State#consumer_state{priorities = []}),
		    done;
		_ ->
		    ?MODULE:consumer_main(NewState)
	    end;
	{presence, JID, Priority} ->
	    NewPriorities = lists:keysort(1, keystore(JID, 2, Priorities, {-Priority, JID})),
	    ?MODULE:consumer_main(State#consumer_state{priorities = NewPriorities});
	{deliver, _ConsumerTag, false, {_QName, _QPid, _Id, _Redelivered, Msg}} ->
	    #basic_message{exchange_name = #resource{name = XNameBin},
			   content = #content{payload_fragments_rev = PayloadRev}} = Msg,
	    [{_, TopPriorityJID} | _] = Priorities,
	    send_message(jlib:make_jid(binary_to_list(XNameBin), State#consumer_state.lserver, ""),
			 TopPriorityJID,
			 "chat",
			 binary_to_list(list_to_binary(lists:reverse(PayloadRev)))),
	    ?MODULE:consumer_main(State);
	Other ->
	    ?INFO_MSG("Consumer main ~p got ~p", [State#consumer_state.queue, Other]),
	    ?MODULE:consumer_main(State)
    end.

%% implementation from R12B-0. When we drop support for R11B, we can
%% use the system's implementation.
keystore(Key, N, [H|T], New) when element(N, H) == Key ->
    [New|T];
keystore(Key, N, [H|T], New) ->
    [H|keystore(Key, N, T, New)];
keystore(_Key, _N, [], New) ->
    [New].

consumer_done(#consumer_state{queue = QNameBin, consumer_tag = ConsumerTag}) ->
    rabbit_amqqueue:with(?QNAME(QNameBin),
			 fun (Q) ->
				 rabbit_amqqueue:basic_cancel(Q, self(), ConsumerTag, undefined)
			 end),
    ok.

parse_command(Str) ->
    [Cmd | Args] = string:tokens(Str, " "),
    {stringprep:tolower(Cmd), Args}.

command_list() ->
    [{"help", "'help (command)'. Provides help for other commands."},
     {"exchange.declare", "'exchange.declare (name)'. Creates a new AMQP fanout exchange."},
     {"exchange.delete", "'exchange.delete (name)'. Deletes an AMQP exchange."},
     {"bind", "'bind (exchange) (jid)'. Binds an exchange to another JID."},
     {"unbind", "'unbind (exchange) (jid)'. Unbinds an exchange from another JID."},
     {"list", "'list'. List available exchanges.\nOR 'list (exchange)'. List subscribers to an exchange."}].

command_names() ->
    [Cmd || {Cmd, _} <- command_list()].

do_command(_To, _From, _RawCommand, {"help", []}) ->
    {ok,
     "Here is a list of commands. Use 'help (command)' to get details on any one.~n~p",
     [command_names()]};
do_command(_To, _From, _RawCommand, {"help", [Cmd | _]}) ->
    case lists:keysearch(stringprep:tolower(Cmd), 1, command_list()) of
	{value, {_, HelpText}} ->
	    {ok, HelpText};
	false ->
	    {ok, "Unknown command ~p. Try plain old 'help'.", [Cmd]}
    end;
do_command(_To, _From, _RawCommand, {"exchange.declare", [NameStr]}) ->
    case NameStr of
	"amq." ++ _ ->
	    {ok, "Names may not start with 'amq.'."};
	_ ->
	    #exchange{} = rabbit_exchange:declare(?REALM, list_to_binary(NameStr),
						  fanout, true, false, []),
	    {ok, "Exchange ~p declared. Now you can subscribe to it.", [NameStr]}
    end;
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"exchange.delete", [NameStr]}) ->
    case NameStr of
	"amq." ++ _ ->
	    {ok, "You are not allowed to delete names starting with 'amq.'."};
	_ ->
	    XNameBin = list_to_binary(NameStr),
	    unsub_all(XNameBin, jlib:make_jid(NameStr, Server, "")),
	    %%rabbit_exchange:delete(?XNAME(XNameBin), false),
	    {ok, "Exchange ~p deleted.", [NameStr]}
    end;
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"bind", [NameStr, JIDStr]}) ->
    XJID = jlib:make_jid(NameStr, Server, ""),
    QJID = jlib:string_to_jid(JIDStr),
    send_presence(XJID, QJID, "subscribe"),
    {ok, "Subscription process initiated. Good luck!"};
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr]}) ->
    XJID = jlib:make_jid(NameStr, Server, ""),
    QJID = jlib:string_to_jid(JIDStr),
    do_unsub(QJID, XJID, list_to_binary(NameStr), list_to_binary(JIDStr)),
    {ok, "Unsubscription process initiated. Good luck!"};
do_command(_To, _From, _RawCommand, {"list", []}) ->
    {ok, "Exchanges available:~n~p",
     [[binary_to_list(XN) || XN <- all_exchanges()]]};
do_command(_To, _From, _RawCommand, {"list", [NameStr]}) ->
    {atomic, BoundQueues} = mnesia:transaction(fun () ->
						       get_bound_queues(list_to_binary(NameStr))
					       end),
    {ok, "Subscribers to ~p:~n~p",
     [NameStr, [binary_to_list(QN) || QN <- BoundQueues]]};
do_command(_To, _From, RawCommand, _Parsed) ->
    {error,
     "I am a rabbitmq bot. Your command ~p was not understood.~n" ++
     "Here is a list of commands:~n~p",
     [RawCommand, command_names()]}.

send_command_reply(From, To, {Status, Fmt, Args}) ->
    send_command_reply(From, To, {Status, io_lib:format(Fmt, Args)});
send_command_reply(From, To, {ok, ResponseIoList}) ->
    send_message(From, To, "chat", lists:flatten(ResponseIoList));
send_command_reply(From, To, {error, ResponseIoList}) ->
    send_message(From, To, "chat", lists:flatten(ResponseIoList)).
