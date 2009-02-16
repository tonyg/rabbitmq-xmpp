%% RabbitMQ gateway module for ejabberd.
%% Based on ejabberd's mod_echo.erl
%%---------------------------------------------------------------------------
%% @author Tony Garnock-Jones <tonyg@lshift.net>
%% @author Rabbit Technologies Ltd. <info@rabbitmq.com>
%% @author LShift Ltd. <query@lshift.net>
%% @copyright 2008 Tony Garnock-Jones and Rabbit Technologies Ltd.; Copyright © 2008-2009 Tony Garnock-Jones and LShift Ltd.
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
%%
%% All of the exposed functions of this module are private to the
%% implementation. See the <a
%% href="overview-summary.html">overview</a> page for more
%% information.

-module(mod_rabbitmq).

-behaviour(gen_server).
-behaviour(gen_mod).

-export([start_link/2, start/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([route/3]).
-export([consumer_init/5, consumer_main/1]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include_lib("rabbitmq_server/include/rabbit.hrl").

-define(VHOST, <<"/">>).
-define(XNAME(Name), #resource{virtual_host = ?VHOST, kind = exchange, name = Name}).
-define(QNAME(Name), #resource{virtual_host = ?VHOST, kind = queue, name = Name}).

-record(state, {host}).
-record(rabbitmq_consumer_process, {queue, pid}).
-record(consumer_state, {lserver, consumer_tag, queue, priorities}).

-define(PROCNAME, ejabberd_mod_rabbitmq).
-define(TABLENAME, ?PROCNAME).

%% @hidden
start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    mnesia:create_table(rabbitmq_consumer_process,
			[{attributes, record_info(fields, rabbitmq_consumer_process)}]),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

%% @hidden
start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    ChildSpec = {Proc,
		 {?MODULE, start_link, [Host, Opts]},
		 temporary,
		 1000,
		 worker,
		 [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

%% @hidden
stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:call(Proc, stop),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).

%%---------------------------------------------------------------------------

%% @hidden
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

%% @hidden
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% @hidden
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @hidden
handle_info({route, From, To, Packet}, State) ->
    safe_route(non_shortcut, From, To, Packet),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% @hidden
terminate(_Reason, State) ->
    ejabberd_router:unregister_route(State#state.host),
    ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------------------------------------------

%% @hidden
route(From, To, Packet) ->
    safe_route(shortcut, From, To, Packet).

safe_route(ShortcutKind, From, To, Packet) ->
    ?DEBUG("~p~n~p ->~n~p~n~p", [ShortcutKind, From, To, Packet]),
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
    {XNameBin, RKBin} = jid_to_xname(To),
    case xml:get_tag_attr_s("type", Packet) of
	"subscribe" ->
	    case rabbit_exchange:lookup(?XNAME(XNameBin)) of
		{ok, _X} -> send_presence(To, From, "subscribe");
		{error, not_found} -> send_presence(To, From, "unsubscribed")
	    end;
	"subscribed" ->
	    case check_and_bind(XNameBin, RKBin, QNameBin) of
		true ->
		    send_presence(To, From, "subscribed"),
		    send_presence(To, From, "");
		false ->
		    send_presence(To, From, "unsubscribed"),
		    send_presence(To, From, "unsubscribe")
	    end;
	"unsubscribe" ->
	    maybe_unsub(From, To, XNameBin, RKBin, QNameBin),
	    send_presence(To, From, "unsubscribed");
	"unsubscribed" ->
	    maybe_unsub(From, To, XNameBin, RKBin, QNameBin);

	"" ->
	    start_consumer(QNameBin, From, RKBin, To#jid.lserver, extract_priority(Packet));
	"unavailable" ->
	    stop_consumer(QNameBin, From, RKBin, false);

	"probe" ->
	    case is_subscribed(XNameBin, RKBin, QNameBin) of
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
	    {XNameBin, RKBin} = jid_to_xname(To),
	    case To#jid.luser of
		"" ->
		    send_command_reply(To, From, do_command(To, From, Body, parse_command(Body)));
		_ ->
		    case xml:get_tag_attr_s("type", Packet) of
			"error" ->
			    ?ERROR_MSG("Received error message~n~p -> ~p~n~p", [From, To, Packet]);
			_ ->
			    rabbit_exchange:simple_publish(false, false, ?XNAME(XNameBin), RKBin,
							   <<"text/plain">>,
							   list_to_binary(Body))
		    end
	    end
    end,
    ok;
do_route(From, To, {xmlelement, "iq", _, Els0} = Packet) ->
    Els = xml:remove_cdata(Els0),
    IqId = xml:get_tag_attr_s("id", Packet),
    case xml:get_tag_attr_s("type", Packet) of
	"get" -> reply_iq(To, From, IqId, do_iq(get, From, To, Els));
	"set" -> reply_iq(To, From, IqId, do_iq(set, From, To, Els));
	Other -> ?WARNING_MSG("Unsolicited IQ of type ~p~n~p ->~n~p~n~p",
			      [Other, From, To, Packet])
    end,
    ok;
do_route(_From, _To, _Packet) ->
    ?INFO_MSG("**** DROPPED~n~p~n~p~n~p", [_From, _To, _Packet]),
    ok.

reply_iq(From, To, IqId, {OkOrError, Els}) ->
    ?DEBUG("IQ reply ~p~n~p ->~n~p~n~p", [IqId, From, To, Els]),
    TypeStr = case OkOrError of
		  ok ->
		      "result";
		  error ->
		      "error"
	      end,
    Attrs = case IqId of
		"" -> [{"type", TypeStr}];
		_ -> [{"type", TypeStr}, {"id", IqId}]
	    end,
    ejabberd_router:route(From, To, {xmlelement, "iq", Attrs, Els}).

do_iq(_GetOrSet, _From, _To, []) ->
    {error, iq_error("modify", "bad-request", "Missing IQ element")};
do_iq(_GetOrSet, _From, _To, [_, _ | _]) ->
    {error, iq_error("modify", "bad-request", "Too many IQ elements")};
do_iq(GetOrSet, From, To, [Elt]) ->
    Xmlns = xml:get_tag_attr_s("xmlns", Elt),
    do_iq1(GetOrSet, From, To, Xmlns, Elt).


do_iq1(get, _From, To, "http://jabber.org/protocol/disco#info",
       {xmlelement, "query", _, _}) ->
    {XNameBin, RKBin} = jid_to_xname(To),
    case XNameBin of
	<<>> -> disco_info_module(To#jid.lserver);
	_ -> disco_info_exchange(XNameBin, RKBin)
    end;
do_iq1(get, _From, To, "http://jabber.org/protocol/disco#items",
       {xmlelement, "query", _, _}) ->
    {XNameBin, RKBin} = jid_to_xname(To),
    case XNameBin of
	<<>> -> disco_items_module(To#jid.lserver);
	_ -> disco_items_exchange(XNameBin, RKBin)
    end;
do_iq1(_GetOrSet, _From, _To, Xmlns, RequestElement) ->
    ?DEBUG("Unimplemented IQ feature ~p~n~p", [Xmlns, RequestElement]),
    {error, iq_error("cancel", "feature-not-implemented", "")}.

disco_info_module(_Server) ->
    {ok, disco_info_result([{"component", "generic", "AMQP router module"},
			    {"component", "router", "AMQP router module"},
			    {"component", "presence", "AMQP router module"},
			    {"client", "bot", "RabbitMQ control interface"},
			    {"gateway", "amqp", "AMQP gateway"},
			    {"hierarchy", "branch", ""},
			    "http://jabber.org/protocol/disco#info",
			    "http://jabber.org/protocol/disco#items"])}.

disco_info_exchange(XNameBin, _RKBin) ->
    Tail = case rabbit_exchange:lookup(?XNAME(XNameBin)) of
	       {ok, #exchange{type = TypeAtom}} ->
		   ["amqp-exchange-" ++ atom_to_list(TypeAtom)];
	       {error, not_found} ->
		   []
	   end,
    {ok, disco_info_result([{"component", "bot", "AMQP Exchange"},
			    {"hierarchy", "leaf", ""},
			    "amqp-exchange"
			    | Tail])}.

disco_items_module(Server) ->
    {ok, disco_items_result([jlib:make_jid(XNameStr, Server, "")
			     || XNameStr <- all_exchange_names()])}.

disco_items_exchange(_XNameStr, _RKBin) ->
    {ok, disco_items_result([])}.

disco_info_result(Pieces) ->
    disco_info_result(Pieces, []).

disco_info_result([], ConvertedPieces) ->
    [{xmlelement, "query", [{"xmlns", "http://jabber.org/protocol/disco#info"}], ConvertedPieces}];
disco_info_result([{Category, Type, Name} | Rest], ConvertedPieces) ->
    disco_info_result(Rest, [{xmlelement, "identity", [{"category", Category},
							      {"type", Type},
							      {"name", Name}], []}
				    | ConvertedPieces]);
disco_info_result([Feature | Rest], ConvertedPieces) ->
    disco_info_result(Rest, [{xmlelement, "feature", [{"var", Feature}], []}
				    | ConvertedPieces]).

disco_items_result(Pieces) ->
    [{xmlelement, "query", [{"xmlns", "http://jabber.org/protocol/disco#items"}],
      [{xmlelement, "item", [{"jid", jlib:jid_to_string(Jid)}], []} || Jid <- Pieces]}].

iq_error(TypeStr, ConditionStr, MessageStr) ->
    iq_error(TypeStr, ConditionStr, MessageStr, []).

iq_error(TypeStr, ConditionStr, MessageStr, ExtraElements) ->
    [{xmlelement, "error", [{"type", TypeStr}],
      [{xmlelement, ConditionStr, [], []},
       {xmlelement, "text", [{"xmlns", "urn:ietf:params:xml:ns:xmpp-stanzas"}],
	[{xmlcdata, MessageStr}]}
       | ExtraElements]}].

extract_priority(Packet) ->
    case xml:get_subtag_cdata(Packet, "priority") of
	"" ->
	    0;
	S ->
	    list_to_integer(S)
    end.

jid_to_qname(#jid{luser = U, lserver = S}) ->
    list_to_binary(U ++ "@" ++ S).

jid_to_xname(#jid{luser = U, lresource = R}) ->
    {list_to_binary(U), list_to_binary(R)}.

qname_to_jid(QNameBin) when is_binary(QNameBin) ->
    case jlib:string_to_jid(binary_to_list(QNameBin)) of
	error ->
	    error;
	JID ->
	    case JID#jid.luser of
		"" ->
		    error;
		_ ->
		    JID
	    end
    end.

maybe_unsub(From, To, XNameBin, RKBin, QNameBin) ->
    case is_subscribed(XNameBin, RKBin, QNameBin) of
	true ->
	    do_unsub(From, To, XNameBin, RKBin, QNameBin);
	false ->
	    ok
    end,
    ok.

do_unsub(QJID, XJID, XNameBin, RKBin, QNameBin) ->
    send_presence(XJID, QJID, "unsubscribed"),
    send_presence(XJID, QJID, "unsubscribe"),
    case unbind_and_delete(XNameBin, RKBin, QNameBin) of
	no_subscriptions_left ->
	    stop_consumer(QNameBin, QJID, none, true),
	    ok;
	subscriptions_remain ->
	    ok
    end.

get_bound_queues(XNameBin) ->
    XName = ?XNAME(XNameBin),
    [{QNameBin, RKBin} ||
	{#resource{name = QNameBin}, RKBin, _} <- rabbit_exchange:list_exchange_bindings(XName)].

unsub_all(XNameBin, ExchangeJID) ->
    {atomic, BindingDescriptions} =
	mnesia:transaction(
	  fun () ->
		  BoundQueues = get_bound_queues(XNameBin),
		  rabbit_exchange:delete(?XNAME(XNameBin), false),
		  BoundQueues
	  end),
    ?INFO_MSG("unsub_all~n~p~n~p~n~p", [XNameBin, ExchangeJID, BindingDescriptions]),
    lists:foreach(fun ({QNameBin, RKBin}) ->
			  case qname_to_jid(QNameBin) of
			      error ->
				  ignore;
			      QJID ->
				  do_unsub(QJID,
					   jlib:jid_replace_resource(ExchangeJID,
								     binary_to_list(RKBin)),
					   XNameBin,
					   RKBin,
					   QNameBin)
			  end
		  end, BindingDescriptions),
    ok.

send_presence(From, To, "") ->
    ?DEBUG("Sending sub reply of type ((available))~n~p -> ~p", [From, To]),
    ejabberd_router:route(From, To, {xmlelement, "presence", [], []});
send_presence(From, To, TypeStr) ->
    ?DEBUG("Sending sub reply of type ~p~n~p -> ~p", [TypeStr, From, To]),
    ejabberd_router:route(From, To, {xmlelement, "presence", [{"type", TypeStr}], []}).

send_message(From, To, TypeStr, BodyStr) ->
    XmlBody = {xmlelement, "message",
	       [{"type", TypeStr},
		{"from", jlib:jid_to_string(From)},
		{"to", jlib:jid_to_string(To)}],
	       [{xmlelement, "body", [],
		 [{xmlcdata, BodyStr}]}]},
    ?DEBUG("Delivering ~p -> ~p~n~p", [From, To, XmlBody]),
    ejabberd_router:route(From, To, XmlBody).

is_subscribed(XNameBin, RKBin, QNameBin) ->
    XName = ?XNAME(XNameBin),
    lists:any(fun 
		  ({N, R, _Arguments})
		  when N == XName andalso R == RKBin ->
		      true;
		  (_) ->
		      false
	      end, rabbit_exchange:list_queue_bindings(?QNAME(QNameBin))).

check_and_bind(XNameBin, RKBin, QNameBin) ->
    ?DEBUG("Checking ~p ~p ~p", [XNameBin, RKBin, QNameBin]),
    case rabbit_exchange:lookup(?XNAME(XNameBin)) of
	{ok, _X} ->
	    ?DEBUG("... exists", []),
	    #amqqueue{} = rabbit_amqqueue:declare(?QNAME(QNameBin), true, false, []),
	    ok = rabbit_exchange:add_binding(?XNAME(XNameBin), ?QNAME(QNameBin), RKBin, []),
	    true;
	{error, not_found} ->
	    ?DEBUG("... not present", []),
	    false
    end.

unbind_and_delete(XNameBin, RKBin, QNameBin) ->
    ?DEBUG("Unbinding ~p ~p ~p", [XNameBin, RKBin, QNameBin]),
    QName = ?QNAME(QNameBin),
    case rabbit_exchange:delete_binding(?XNAME(XNameBin), QName, RKBin, []) of
	{error, _Reason} ->
	    ?DEBUG("... queue or exchange not found: ~p. Ignoring", [_Reason]),
	    no_subscriptions_left;
	ok ->
	    ?DEBUG("... checking count of remaining bindings ...", []),
	    %% Obvious (small) window where Problems (races) May Occur here
	    case length(rabbit_exchange:list_queue_bindings(QName)) of
		0 ->
		    ?DEBUG("... and deleting", []),
		    case rabbit_amqqueue:lookup(QName) of
			{ok, Q} -> rabbit_amqqueue:delete(Q, false, false);
			{error, not_found} -> ok
		    end,
		    ?DEBUG("... deletion complete.", []),
		    no_subscriptions_left;
		_PositiveCountOfBindings ->
		    ?DEBUG("... and leaving the queue alone", []),
		    subscriptions_remain
	    end
    end.

all_exchange_names() ->
    [binary_to_list(XNameBin) ||
	#exchange{name = #resource{name = XNameBin}}
	    <- rabbit_exchange:list(?VHOST),
	XNameBin =/= <<>>].

all_exchanges() ->
    [{binary_to_list(XNameBin),
      TypeAtom,
      case IsDurable of
	  true -> durable;
	  false -> transient
      end,
      Arguments}
     ||
	#exchange{name = #resource{name = XNameBin},
		  type = TypeAtom,
		  durable = IsDurable,
		  arguments = Arguments}
	    <- rabbit_exchange:list(?VHOST),
	XNameBin =/= <<>>].

probe_queues(Server) ->
    probe_queues(Server, rabbit_amqqueue:list(?VHOST)).

probe_queues(_Server, []) ->
    ok;
probe_queues(Server, [#amqqueue{name = QName = #resource{name = QNameBin}} | Rest]) ->
    ?DEBUG("**** Probing ~p", [QNameBin]),
    case qname_to_jid(QNameBin) of
	error ->
	    probe_queues(Server, Rest);
	JID ->
	    probe_bindings(Server, JID, rabbit_exchange:list_queue_bindings(QName)),
	    probe_queues(Server, Rest)
    end.

probe_bindings(_Server, _JID, []) ->
    ok;
probe_bindings(Server, JID, [{#resource{name = XNameBin}, _RoutingKey, _Arguments} | Rest]) ->
    ?DEBUG("**** Probing ~p ~p ~p", [JID, XNameBin, Server]),
    SourceJID = jlib:make_jid(binary_to_list(XNameBin), Server, ""),
    send_presence(SourceJID, JID, "probe"),
    send_presence(SourceJID, JID, ""),
    probe_bindings(Server, JID, Rest).

start_consumer(QNameBin, JID, RKBin, Server, Priority) ->
    mnesia:transaction(
      fun () ->
	      case mnesia:read({rabbitmq_consumer_process, QNameBin}) of
		  [#rabbitmq_consumer_process{pid = Pid}] ->
		      Pid ! {presence, JID, RKBin, Priority},
		      ok;
		  [] ->
		      %% TODO: Link into supervisor
		      Pid = spawn(?MODULE, consumer_init, [QNameBin, JID, RKBin, Server, Priority]),
		      mnesia:write(#rabbitmq_consumer_process{queue = QNameBin, pid = Pid}),
		      ok
	      end
      end),
    ok.

stop_consumer(QNameBin, JID, RKBin, AllResources) ->
    mnesia:transaction(
      fun () ->
	      case mnesia:read({rabbitmq_consumer_process, QNameBin}) of
		  [#rabbitmq_consumer_process{pid = Pid}] ->
		      Pid ! {unavailable, JID, RKBin, AllResources},
		      ok;
		  [] ->
		      ok
	      end
      end),
    ok.

%% @hidden
consumer_init(QNameBin, JID, RKBin, Server, Priority) ->
    ?INFO_MSG("**** starting consumer for queue ~p~njid ~p~npriority ~p rkbin ~p",
	      [QNameBin, JID, Priority, RKBin]),
    ConsumerTag = rabbit_guid:binstring_guid("amq.xmpp"),
    rabbit_amqqueue:with(?QNAME(QNameBin),
			 fun(Q) ->
				 rabbit_amqqueue:basic_consume(
				   Q, true, self(), self(), undefined,
				   ConsumerTag, false, undefined)
			 end),
    ?MODULE:consumer_main(#consumer_state{lserver = Server,
					  consumer_tag = ConsumerTag,
					  queue = QNameBin,
					  priorities = [{-Priority, {JID, RKBin}}]}).

jids_equal_upto_resource(J1, J2) ->
    jlib:jid_remove_resource(J1) == jlib:jid_remove_resource(J2).

%% @hidden
consumer_main(#consumer_state{priorities = Priorities} = State) ->
    ?DEBUG("**** consumer ~p", [State]),
    receive
	{unavailable, JID, RKBin, AllResources} ->
	    {atomic, NewState} =
		mnesia:transaction(
		  fun () ->
			  NewPriorities =
			      case AllResources of
				  true ->
				      [E || E = {_, {J, _}} <- Priorities,
					    not jids_equal_upto_resource(J, JID)];
				  false ->
				      lists:keydelete({JID, RKBin}, 2, Priorities)
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
		    ?INFO_MSG("**** terminating consumer~n~p", [State#consumer_state.queue]),
		    consumer_done(State#consumer_state{priorities = []}),
		    done;
		_ ->
		    ?MODULE:consumer_main(NewState)
	    end;
	{presence, JID, RKBin, Priority} ->
	    NewPriorities = lists:keysort(1, keystore({JID, RKBin}, 2, Priorities,
						      {-Priority, {JID, RKBin}})),
	    ?MODULE:consumer_main(State#consumer_state{priorities = NewPriorities});
	{'$gen_cast', {deliver, _ConsumerTag, false, {_QName, QPid, _Id, _Redelivered, Msg}}} ->
	    #basic_message{exchange_name = #resource{name = XNameBin},
			   routing_key = RKBin,
			   content = #content{payload_fragments_rev = PayloadRev}} = Msg,
	    [{_, {TopPriorityJID, _}} | _] = Priorities,
	    send_message(jlib:make_jid(binary_to_list(XNameBin),
				       State#consumer_state.lserver,
				       binary_to_list(RKBin)),
			 TopPriorityJID,
			 "chat",
			 binary_to_list(list_to_binary(lists:reverse(PayloadRev)))),
	    rabbit_amqqueue:notify_sent(QPid, self()),
	    ?MODULE:consumer_main(State);
	Other ->
	    ?INFO_MSG("Consumer main ~p got~n~p", [State#consumer_state.queue, Other]),
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
     {"exchange.declare", "'exchange.declare (name) [-type (type)] [-transient]'. Creates a new AMQP exchange."},
     {"exchange.delete", "'exchange.delete (name)'. Deletes an AMQP exchange."},
     {"bind", "'bind (exchange) (jid)'. Binds an exchange to another JID, with an empty routing key. 'bind (exchange) (jid) (key)'. Binds an exchange to another JID, with the given routing key."},
     {"unbind", "'unbind (exchange) (jid)'. Unbinds an exchange from another JID, using an empty routing key. 'unbind (exchange) (jid) (key)'. Unbinds using the given routing key."},
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
do_command(_To, _From, _RawCommand, {"exchange.declare", [NameStr | Args]}) ->
    do_command_declare(NameStr, Args, []);
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
    do_command_bind(Server, NameStr, JIDStr, "");
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"bind", [NameStr, JIDStr, RK]}) ->
    do_command_bind(Server, NameStr, JIDStr, RK);
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr]}) ->
    do_command_unbind(Server, NameStr, JIDStr, "");
do_command(#jid{lserver = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr, RK]}) ->
    do_command_unbind(Server, NameStr, JIDStr, RK);
do_command(_To, _From, _RawCommand, {"list", []}) ->
    {ok, "Exchanges available:~n~p",
     [all_exchanges()]};
do_command(_To, _From, _RawCommand, {"list", [NameStr]}) ->
    {atomic, BindingDescriptions} =
	mnesia:transaction(fun () -> get_bound_queues(list_to_binary(NameStr)) end),
    {ok, "Subscribers to ~p:~n~p",
     [NameStr, [{binary_to_list(QN), binary_to_list(RK)} || {QN, RK} <- BindingDescriptions]]};
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

get_arg(ParsedArgs, Key, DefaultValue) ->
    case lists:keysearch(Key, 1, ParsedArgs) of
	{value, {_, V}} ->
	    V;
	false ->
	    DefaultValue
    end.

do_command_declare(NameStr, ["-type", TypeStr | Rest], ParsedArgs) ->
    do_command_declare(NameStr, Rest, [{type, TypeStr} | ParsedArgs]);
do_command_declare(NameStr, ["-transient" | Rest], ParsedArgs) ->
    do_command_declare(NameStr, Rest, [{durable, false} | ParsedArgs]);
do_command_declare(NameStr, [], ParsedArgs) ->
    case NameStr of
	"amq." ++ _ ->
	    {ok, "Names may not start with 'amq.'."};
	_ ->
	    case catch (rabbit_exchange:check_type(list_to_binary(get_arg(ParsedArgs,
									  type,
									  "fanout")))) of
		{'EXIT', _} -> {error, "Bad exchange type."};
		TypeAtom ->
		    #exchange{} = rabbit_exchange:declare(?XNAME(list_to_binary(NameStr)),
							  TypeAtom,
							  get_arg(ParsedArgs, durable, true),
							  false,
							  []),
		    {ok, "Exchange ~p of type ~p declared. Now you can subscribe to it.",
		     [NameStr, TypeAtom]}
	    end
    end.

do_command_bind(Server, NameStr, JIDStr, RK) ->
    XJID = jlib:make_jid(NameStr, Server, RK),
    QJID = jlib:string_to_jid(JIDStr),
    send_presence(XJID, QJID, "subscribe"),
    {ok, "Subscription process ~p <--> ~p initiated. Good luck!",
     [jlib:jid_to_string(XJID), jlib:jid_to_string(QJID)]}.

do_command_unbind(Server, NameStr, JIDStr, RK) ->
    XJID = jlib:make_jid(NameStr, Server, RK),
    QJID = jlib:string_to_jid(JIDStr),
    do_unsub(QJID, XJID, list_to_binary(NameStr), list_to_binary(RK), list_to_binary(JIDStr)),
    {ok, "Unsubscription process ~p <--> ~p initiated. Good luck!",
     [jlib:jid_to_string(XJID), jlib:jid_to_string(QJID)]}.
