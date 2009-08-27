%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.

-module(rabbit_xmpp).

-include_lib("exmpp/include/exmpp.hrl").
-include_lib("exmpp/include/exmpp_client.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("exmpp/include/internal/exmpp_xmpp.hrl").

-export([start_link/0, stop/1]).
-export([init/0]).

-export([consumer_init/6, consumer_main/2]).

-define(VHOST, <<"/">>).
-define(XNAME(Name), #resource{virtual_host = ?VHOST, kind = exchange, name = Name}).
-define(QNAME(Name), #resource{virtual_host = ?VHOST, kind = queue, name = Name}).

-record(state, {host}).
-record(rabbitmq_consumer_process, {queue, pid}).
-record(consumer_state, {lserver, consumer_tag, queue, priorities}).

-define(PROCNAME, ejabberd_mod_rabbitmq).
-define(TABLENAME, ?PROCNAME).

-define(DEBUG(Fmt, Vals), io:format("DEBUG: " ++ Fmt ++ "~n", Vals)).
-define(INFO_MSG(Fmt, Vals), io:format("INFO: " ++ Fmt ++ "~n", Vals)).
-define(WARNING_MSG(Fmt, Vals), io:format("WARNING: " ++ Fmt ++ "~n", Vals)).
-define(ERROR_MSG(Fmt, Vals), io:format("ERROR: " ++ Fmt ++ "~n", Vals)).

-define(HOST, application:get_env(rabbit_xmpp, component_host)).

start_link() ->
    {ok, spawn(?MODULE, init, [])}.

stop(EchoComPid) ->
    EchoComPid ! stop.

init() ->
    {ok, Server} = application:get_env(rabbit_xmpp, xmpp_server),
    {ok, Port} = application:get_env(rabbit_xmpp, component_port),
    {ok, Host} = application:get_env(rabbit_xmpp, component_host),
    {ok, Secret} = application:get_env(rabbit_xmpp, component_secret),
    
    application:start(exmpp),
    XmppCom = exmpp_component:start(),
    exmpp_component:auth(XmppCom, Host, Secret),
    _StreamId = exmpp_component:connect(XmppCom, Server, Port),
    exmpp_component:handshake(XmppCom),
    mnesia:create_table(rabbitmq_consumer_process,
            			[{attributes, record_info(fields, rabbitmq_consumer_process)}]),
    probe_queues(XmppCom, Host),
    loop(XmppCom).

loop(XmppCom) ->
    ?DEBUG("Starting loop...", []),
    
    receive
        stop ->
            exmpp_component:stop(XmppCom);
        %% If we receive a message, we reply with the same message
        Record = #received_packet{raw_packet = Packet} ->
            ?DEBUG("Got record: ~p", [Record]),
            
            From = exmpp_jid:parse(exmpp_xml:get_attribute(Packet, from, <<"unknown">>)),
            To = exmpp_jid:parse(exmpp_xml:get_attribute(Packet, to, <<"unknown">>)),
            
            do_route(XmppCom, From, To, Record),
            loop(XmppCom);
        Other ->
            ?DEBUG("Other: ~p", [Other]),
            loop(XmppCom)
    end.
    
do_route(XmppCom, From, To, 
         #received_packet{packet_type = presence, type_attr = Type,
                          raw_packet = Packet}) ->
    QNameBin = jid_to_qname(From),
    {XNameBin, RKBin} = jid_to_xname(To),
    case Type of
        "subscribe" ->
    	    case rabbit_exchange_lookup(?XNAME(XNameBin)) of
    		    {ok, _X} -> send_presence(XmppCom, To, From, "subscribe");
        		{error, not_found} -> send_presence(XmppCom, To, From, "unsubscribed")
    	    end;
	    "subscribed" ->
	        case check_and_bind(XNameBin, RKBin, QNameBin) of
    		    true ->
        		    send_presence(XmppCom, To, From, "subscribed"),
        		    send_presence(XmppCom, To, From, "");
        		false ->
        		    send_presence(XmppCom, To, From, "unsubscribed"),
        		    send_presence(XmppCom, To, From, "unsubscribe")
    	    end;
        "unsubscribe" ->
    	    maybe_unsub(XmppCom, From, To, XNameBin, RKBin, QNameBin),
    	    send_presence(XmppCom, To, From, "unsubscribed");
    	"unsubscribed" ->
    	    maybe_unsub(XmppCom, From, To, XNameBin, RKBin, QNameBin);
    	"" ->
    	    start_consumer(XmppCom, QNameBin, From, RKBin, To#jid.prep_domain, extract_priority(Packet));
    	"available" ->
    	    start_consumer(XmppCom, QNameBin, From, RKBin, To#jid.prep_domain, extract_priority(Packet));
    	"unavailable" ->
    	    stop_consumer(QNameBin, From, RKBin, false);
    	"probe" ->
    	    case is_subscribed(XNameBin, RKBin, QNameBin) of
    		    true ->
        		    send_presence(XmppCom, To, From, "");
        		false ->
        		    ?DEBUG("Not subscribed: X=~p, RK=~p, Q=~p", [XNameBin, RKBin, QNameBin]),
        		    ok
    	    end;
	    _Other ->
    	    ?INFO_MSG("Other kind of presence~n~p", [Packet])
    end;
do_route(XmppCom, From, To,
         #received_packet{packet_type = message, type_attr = Type,
                          raw_packet = Packet}) ->
    case get_subtag_cdata(Packet, body) of
  "" ->
      ?DEBUG("Ignoring message with empty body", []);
  Body ->
      {XNameBin, RKBin} = jid_to_xname(To),
      case To#jid.prep_node of
      "" ->
          send_command_reply(XmppCom, To, From, do_command(XmppCom, To, From, Body, parse_command(Body)));
      _ ->
          case Type of
              error ->
                  ?ERROR_MSG("Received error message~n~p -> ~p~n~p", [From, To, Packet]);
               _ ->
                %% FIXME: So many roundtrips!!
                Msg = rabbit_call(rabbit_basic, message,
                                  [?XNAME(XNameBin),
                                   RKBin,
                                   [{'content_type', <<"text/plain">>}],
                                   Body]),
                ?DEBUG("Sending ~p", [Msg]),
                Delivery = rabbit_call(rabbit_basic, delivery,
                                       [false, false, none, Msg]),
                rabbit_call(rabbit_basic, publish, [Delivery])
          end
      end
    end,
    ok;
do_route(XmppCom, From, To, 
         #received_packet{packet_type = iq, type_attr = Type,
                           raw_packet = Packet}) ->
    #xmlel{children = Els0} = Packet,
    Els = xml:remove_cdata(Els0),
    IqId = xml:get_tag_attr_s("id", Packet),
    case xml:get_tag_attr_s("type", Packet) of
      "get" -> reply_iq(XmppCom, To, From, IqId, do_iq(get, From, To, Els));
      "set" -> reply_iq(XmppCom, To, From, IqId, do_iq(set, From, To, Els));
      Other -> ?WARNING_MSG("Unsolicited IQ of type ~p~n~p ->~n~p~n~p",
                    [Other, From, To, Packet])
    end,
    ok;
do_route(_XmppCom, _From, _To, _Packet) ->
    ?INFO_MSG("**** DROPPED~n~p~n~p~n~p", [_From, _To, _Packet]),
    ok.
    	    
%% Packet Management
build_packet(Name, Attrs) ->
    Packet = #xmlel{name = Name},
    
    lists:foldl(fun({K, V}, Acc) -> exmpp_xml:set_attribute(Acc, K, V) end, Packet, Attrs).    	    
get_subtag_cdata(El, Name) ->
    case exmpp_xml:get_element(El, Name) of
        undefined -> "";
        Subtag    -> exmpp_xml:get_cdata(Subtag)
    end.
    	    
%% Rabbit Interface
rabbit_call(M, F, A) ->
    erlang:apply(M, F, A).
rabbit_exchange_lookup(XN) ->
    rabbit_call(rabbit_exchange, lookup, [XN]).
reply_iq(XmppCom, From, To, IqId, {OkOrError, Els}) ->
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
	%% TODO
    xmpp_component:send_packet(From, To, {xmlelement, "iq", Attrs, Els}).

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
	<<>> -> disco_info_module(To#jid.prep_domain);
	_ -> disco_info_exchange(XNameBin, RKBin)
    end;
do_iq1(get, _From, To, "http://jabber.org/protocol/disco#items",
       {xmlelement, "query", _, _}) ->
    {XNameBin, RKBin} = jid_to_xname(To),
    case XNameBin of
	<<>> -> disco_items_module(To#jid.prep_domain);
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
    Tail = case rabbit_exchange_lookup(?XNAME(XNameBin)) of
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
    case get_subtag_cdata(Packet, priority) of
	"" ->
	    0;
	S ->
	    list_to_integer(binary_to_list(S))
    end.

jid_to_qname(#jid{prep_node = U, prep_domain = S}) ->
    User = case U of undefined -> <<>>; _ -> U end,
    Server = case S of undefined -> <<>>; _ -> S end,
    list_to_binary(binary_to_list(User) ++ "@" ++ binary_to_list(Server)).

jid_to_xname(#jid{prep_node = U, prep_resource = R}) ->
    case {U, R} of
        {undefined, undefined}  -> {<<>>, <<>>};
        {undefined, R} when not(is_atom(R)) -> {<<>>, R};
        {U, undefined} -> {U, <<>>};
        {U, R} -> {U, R}
    end.

qname_to_jid(QNameBin) when is_binary(QNameBin) ->
    case exmpp_jid:parse(binary_to_list(QNameBin)) of
	error ->
	    error;
	JID ->
	    case JID#jid.prep_node of
		undefined ->
		    error;
		_ ->
		    JID
	    end
    end.

maybe_unsub(XmppCom, From, To, XNameBin, RKBin, QNameBin) ->
    case is_subscribed(XNameBin, RKBin, QNameBin) of
	true ->
	    do_unsub(XmppCom, From, To, XNameBin, RKBin, QNameBin);
	false ->
	    ok
    end,
    ok.

do_unsub(XmppCom, QJID, XJID, XNameBin, RKBin, QNameBin) ->
    send_presence(XmppCom, XJID, QJID, "unsubscribed"),
    send_presence(XmppCom, XJID, QJID, "unsubscribe"),
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
	{#resource{name = QNameBin}, RKBin, _} <-
            rabbit_call(rabbit_exchange, list_exchange_bindings, [XName])].

unsub_all(XmppCom, XNameBin, ExchangeJID) ->
    {atomic, BindingDescriptions} =
	mnesia:transaction(
	  fun () ->
		  BoundQueues = get_bound_queues(XNameBin),
		  rabbit_call(rabbit_exchange, delete, [?XNAME(XNameBin), false]),
		  BoundQueues
	  end),
    ?INFO_MSG("unsub_all~n~p~n~p~n~p", [XNameBin, ExchangeJID, BindingDescriptions]),
    lists:foreach(fun ({QNameBin, RKBin}) ->
			  case qname_to_jid(QNameBin) of
			      error ->
				  ignore;
			      QJID ->
				  do_unsub(XmppCom, QJID,
					   jlib:jid_replace_resource(ExchangeJID,
								     binary_to_list(RKBin)),
					   XNameBin,
					   RKBin,
					   QNameBin)
			  end
		  end, BindingDescriptions),
    ok.

send_presence(XmppCom, From, To, "") ->
    ?DEBUG("Sending sub reply of type ((available))~n~p -> ~p", [From, To]),
    Packet = build_packet(presence, [{from, exmpp_jid:to_binary(From)}, 
                                     {to, exmpp_jid:to_binary(To)}]),
    exmpp_component:send_packet(XmppCom, Packet);
send_presence(XmppCom, From, To, TypeStr) ->
    ?DEBUG("Sending sub reply of type ~p~n~p -> ~p", [TypeStr, From, To]),
    Packet = build_packet(presence, [{from, exmpp_jid:to_binary(From)}, 
                                     {to, exmpp_jid:to_binary(To)}, {type, TypeStr}]),
    exmpp_component:send_packet(XmppCom, Packet).

send_message(XmppCom, From, To, TypeStr, BodyStr) ->
    % build_packet(message, [{type, list_to_binary(TypeStr)},
    %                        {from, exmpp_jid:to_binary(From)},
    %                          {to, exmpp_jid:to_binary(To)}]),
    % 
    XmlBody = {xmlel, undefined, [], message,
	       [{xmlattr, undefined, type, list_to_binary(TypeStr)},
		{xmlattr, undefined, from, exmpp_jid:to_binary(From)},
		{xmlattr, undefined, to, exmpp_jid:to_binary(To)}],
	       [{xmlel, undefined, [], body, [],
		 [{xmlcdata, list_to_binary(BodyStr)}]}]},
    ?DEBUG("Delivering ~p -> ~p~n~p", [From, To, XmlBody]),
    exmpp_component:send_packet(XmppCom, XmlBody).

rabbit_exchange_list_queue_bindings(QN) ->
    rabbit_call(rabbit_exchange, list_queue_bindings, [QN]).

is_subscribed(XNameBin, RKBin, QNameBin) ->
    XName = ?XNAME(XNameBin),
    lists:any(fun 
		  ({N, R, _Arguments})
		  when N == XName andalso R == RKBin ->
		      true;
		  (_) ->
		      false
	      end, rabbit_exchange_list_queue_bindings(?QNAME(QNameBin))).

check_and_bind(XNameBin, RKBin, QNameBin) ->
    ?DEBUG("Checking ~p ~p ~p", [XNameBin, RKBin, QNameBin]),
    case rabbit_exchange_lookup(?XNAME(XNameBin)) of
	{ok, _X} ->
	    ?DEBUG("... exists", []),
	    #amqqueue{} = rabbit_call(rabbit_amqqueue, declare,
                                      [?QNAME(QNameBin), true, false, []]),
	    ok = rabbit_call(rabbit_exchange, add_binding,
                             [?XNAME(XNameBin), ?QNAME(QNameBin), RKBin, []]),
	    true;
	{error, not_found} ->
	    ?DEBUG("... not present", []),
	    false
    end.

unbind_and_delete(XNameBin, RKBin, QNameBin) ->
    ?DEBUG("Unbinding ~p ~p ~p", [XNameBin, RKBin, QNameBin]),
    QName = ?QNAME(QNameBin),
    case rabbit_call(rabbit_exchange, delete_binding,
                     [?XNAME(XNameBin), QName, RKBin, []]) of
	{error, _Reason} ->
	    ?DEBUG("... queue or exchange not found: ~p. Ignoring", [_Reason]),
	    no_subscriptions_left;
	ok ->
	    ?DEBUG("... checking count of remaining bindings ...", []),
	    %% Obvious (small) window where Problems (races) May Occur here
	    case length(rabbit_exchange_list_queue_bindings(QName)) of
		0 ->
		    ?DEBUG("... and deleting", []),
		    case rabbit_call(rabbit_amqqueue, lookup, [QName]) of
			{ok, Q} -> rabbit_call(rabbit_amqqueue, delete, [Q, false, false]);
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
	    <- rabbit_call(rabbit_exchange, list, [?VHOST]),
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
	    <- rabbit_call(rabbit_exchange, list, [?VHOST]),
	XNameBin =/= <<>>].

probe_queues(XmppCom, Server) ->
    probe_queues(XmppCom, Server, rabbit_call(rabbit_amqqueue, list, [?VHOST])).

probe_queues(XmppCom, _Server, []) ->
    ok;
probe_queues(XmppCom, Server, [#amqqueue{name = QName = #resource{name = QNameBin}} | Rest]) ->
    ?DEBUG("**** Probing ~p", [QNameBin]),
    case qname_to_jid(QNameBin) of
    	error ->
    	    probe_queues(XmppCom, Server, Rest);
    	JID ->
    	    ?DEBUG("   **** Further Probing ~p", [JID]),
    	    probe_bindings(XmppCom, Server, JID, rabbit_exchange_list_queue_bindings(QName)),
    	    probe_queues(XmppCom, Server, Rest)
    end.

probe_bindings(XmppCom, _Server, _JID, []) ->
    ok;
probe_bindings(XmppCom, Server, JID, [{#resource{name = XNameBin}, _RoutingKey, _Arguments} | Rest]) ->
    ?DEBUG("**** Probing ~p ~p ~p", [JID, XNameBin, Server]),
    SourceJID = exmpp_jid:make(binary_to_list(XNameBin), Server, ""),
    send_presence(XmppCom, SourceJID, JID, "probe"),
    send_presence(XmppCom, SourceJID, JID, ""),
    probe_bindings(XmppCom, Server, JID, Rest).

start_consumer(XmppCom, QNameBin, JID, RKBin, Server, Priority) ->
    ?DEBUG("Starting consumer...", []),
    
    mnesia:transaction(
      fun () ->
	      case mnesia:read({rabbitmq_consumer_process, QNameBin}) of
		  [#rabbitmq_consumer_process{pid = Pid}] ->
		      ?DEBUG("Consumer already exists!", []),
		      
		      Pid ! {presence, JID, RKBin, Priority},
		      ok;
		  [] ->
		      ?DEBUG("Spinning up consumer process", []),
		      
		      %% TODO: Link into supervisor
		      Pid = spawn(?MODULE, consumer_init, [XmppCom, QNameBin, JID, RKBin, Server, Priority]),
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

with_queue(QN, Fun) ->
    %% FIXME: No way of using rabbit_amqqueue:with/2, so using this awful kludge :-(
    case rabbit_call(rabbit_amqqueue, lookup, [QN]) of
        {ok, Q} ->
            Fun(Q);
        {error, Reason} ->
            {error, Reason}
    end.

%% @hidden
consumer_init(XmppCom, QNameBin, JID, RKBin, Server, Priority) ->
    ?INFO_MSG("**** starting consumer for queue ~p~njid ~p~npriority ~p rkbin ~p",
	      [QNameBin, JID, Priority, RKBin]),
    ConsumerTag = rabbit_call(rabbit_guid, binstring_guid, ["amq.xmpp"]),
    with_queue(?QNAME(QNameBin),
               fun(Q) ->
                       rabbit_call(rabbit_amqqueue, basic_consume,
                                   [Q, true, self(), self(), undefined,
                                    ConsumerTag, false, undefined])
               end),
    ?MODULE:consumer_main(XmppCom, #consumer_state{lserver = Server,
					  consumer_tag = ConsumerTag,
					  queue = QNameBin,
					  priorities = [{-Priority, {JID, RKBin}}]}).

jids_equal_upto_resource(J1, J2) ->
    jlib:jid_remove_resource(J1) == jlib:jid_remove_resource(J2).

%% @hidden
consumer_main(XmppCom, #consumer_state{priorities = Priorities} = State) ->
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
		    ?MODULE:consumer_main(XmppCom, NewState)
	    end;
	{presence, JID, RKBin, Priority} ->
	    NewPriorities = lists:keysort(1, keystore({JID, RKBin}, 2, Priorities,
						      {-Priority, {JID, RKBin}})),
	    ?MODULE:consumer_main(XmppCom, State#consumer_state{priorities = NewPriorities});
	{'$gen_cast', {deliver, _ConsumerTag, false, {_QName, QPid, _Id, _Redelivered, Msg}}} ->
	    #basic_message{exchange_name = #resource{name = XNameBin},
			   routing_key = RKBin,
			   content = #content{payload_fragments_rev = PayloadRev}} = Msg,
	    [{_, {TopPriorityJID, _}} | _] = Priorities,
	    send_message(XmppCom, exmpp_jid:make(binary_to_list(XNameBin),
				       State#consumer_state.lserver,
				       binary_to_list(RKBin)),
			 TopPriorityJID,
			 "chat",
			 binary_to_list(list_to_binary(lists:reverse(PayloadRev)))),
            rabbit_call(rabbit_amqqueue, notify_sent, [QPid, self()]),
	    ?MODULE:consumer_main(XmppCom, State);
	Other ->
	    ?INFO_MSG("Consumer main ~p got~n~p", [State#consumer_state.queue, Other]),
	    ?MODULE:consumer_main(XmppCom, State)
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
    with_queue(?QNAME(QNameBin),
               fun (Q) ->
                       rabbit_call(rabbit_amqqueue, basic_cancel,
                                   [Q, self(), ConsumerTag, undefined])
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

do_command(XmppCom, _To, _From, _RawCommand, {"help", []}) ->
    {ok,
     "Here is a list of commands. Use 'help (command)' to get details on any one.~n~p",
     [command_names()]};
do_command(XmppCom, _To, _From, _RawCommand, {"help", [Cmd | _]}) ->
    case lists:keysearch(stringprep:tolower(Cmd), 1, command_list()) of
	{value, {_, HelpText}} ->
	    {ok, HelpText};
	false ->
	    {ok, "Unknown command ~p. Try plain old 'help'.", [Cmd]}
    end;
do_command(XmppCom, _To, _From, _RawCommand, {"exchange.declare", [NameStr | Args]}) ->
    do_command_declare(NameStr, Args, []);
do_command(XmppCom, #jid{prep_domain = Server} = _To, _From, _RawCommand, {"exchange.delete", [NameStr]}) ->
    case NameStr of
	"amq." ++ _ ->
	    {ok, "You are not allowed to delete names starting with 'amq.'."};
	_ ->
	    XNameBin = list_to_binary(NameStr),
	    unsub_all(XmppCom, XNameBin, jlib:make_jid(NameStr, Server, "")),
	    %%rabbit_exchange:delete(?XNAME(XNameBin), false),
	    {ok, "Exchange ~p deleted.", [NameStr]}
    end;
do_command(XmppCom, #jid{prep_domain = Server} = _To, _From, _RawCommand, {"bind", [NameStr, JIDStr]}) ->
    do_command_bind(XmppCom, Server, NameStr, JIDStr, "");
do_command(XmppCom, #jid{prep_domain = Server} = _To, _From, _RawCommand, {"bind", [NameStr, JIDStr, RK | RKs]}) ->
    do_command_bind(XmppCom, Server, NameStr, JIDStr, check_ichat_brokenness(JIDStr, RK, RKs));
do_command(XmppCom, #jid{prep_domain = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr]}) ->
    do_command_unbind(XmppCom, Server, NameStr, JIDStr, "");
do_command(XmppCom, #jid{prep_domain = Server} = _To, _From, _RawCommand, {"unbind", [NameStr, JIDStr, RK | RKs]}) ->
    do_command_unbind(XmppCom, Server, NameStr, JIDStr, check_ichat_brokenness(JIDStr, RK, RKs));
do_command(XmppCom, _To, _From, _RawCommand, {"list", []}) ->
    {ok, "Exchanges available:~n~p",
     [all_exchanges()]};
do_command(XmppCom, _To, _From, _RawCommand, {"list", [NameStr]}) ->
    {atomic, BindingDescriptions} =
	mnesia:transaction(fun () -> get_bound_queues(list_to_binary(NameStr)) end),
    {ok, "Subscribers to ~p:~n~p",
     [NameStr, [{binary_to_list(QN), binary_to_list(RK)} || {QN, RK} <- BindingDescriptions]]};
do_command(XmppCom, _To, _From, RawCommand, _Parsed) ->
    {error,
     "I am a rabbitmq bot. Your command ~p was not understood.~n" ++
     "Here is a list of commands:~n~p",
     [RawCommand, command_names()]}.

send_command_reply(XmppCom, From, To, {Status, Fmt, Args}) ->
    send_command_reply(XmppCom, From, To, {Status, io_lib:format(Fmt, Args)});
send_command_reply(XmppCom, From, To, {ok, ResponseIoList}) ->
    send_message(XmppCom, From, To, "chat", lists:flatten(ResponseIoList));
send_command_reply(XmppCom, From, To, {error, ResponseIoList}) ->
    send_message(XmppCom, From, To, "chat", lists:flatten(ResponseIoList)).

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
	    case catch (rabbit_call(rabbit_exchange, check_type,
                                    [list_to_binary(get_arg(ParsedArgs, type, "fanout"))])) of
		{'EXIT', _} -> {error, "Bad exchange type."};
		TypeAtom ->
		    #exchange{} = rabbit_call(rabbit_exchange, declare,
                                              [?XNAME(list_to_binary(NameStr)),
                                               TypeAtom,
                                               get_arg(ParsedArgs, durable, true),
                                               false,
                                               []]),
		    {ok, "Exchange ~p of type ~p declared. Now you can subscribe to it.",
		     [NameStr, TypeAtom]}
	    end
    end.

interleave(_Spacer, []) ->
    [];
interleave(_Spacer, [E]) ->
    E;
interleave(Spacer, [E | Rest]) ->
    E ++ Spacer ++ interleave(Spacer, Rest).

check_ichat_brokenness(JIDStr, RK, RKs) ->
    IChatLink = "[mailto:" ++ JIDStr ++ "]",
    if
	RK == IChatLink ->
	    interleave(" ", RKs);
	true ->
	    interleave(" ", [RK | RKs])
    end.

do_command_bind(XmppCom, Server, NameStr, JIDStr, RK) ->
    XJID = jlib:make_jid(NameStr, Server, RK),
    QJID = jlib:string_to_jid(JIDStr),
    send_presence(XmppCom, XJID, QJID, "subscribe"),
    {ok, "Subscription process ~p <--> ~p initiated. Good luck!",
     [jlib:jid_to_string(XJID), jlib:jid_to_string(QJID)]}.

do_command_unbind(XmppCom, Server, NameStr, JIDStr, RK) ->
    XJID = jlib:make_jid(NameStr, Server, RK),
    QJID = jlib:string_to_jid(JIDStr),
    do_unsub(XmppCom, QJID, XJID, list_to_binary(NameStr), list_to_binary(RK), list_to_binary(JIDStr)),
    {ok, "Unsubscription process ~p <--> ~p initiated. Good luck!",
     [jlib:jid_to_string(XJID), jlib:jid_to_string(QJID)]}.
