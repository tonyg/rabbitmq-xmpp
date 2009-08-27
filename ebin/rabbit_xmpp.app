{application, rabbit_xmpp,
 [{description, "RabbitMQ XMPP Connector"},
  {vsn, "0.01"},
  {modules, [
  ]},
  {registered, []},
  {mod, {rabbit_xmpp_app, []}},
  {env, [{xmpp_server, "127.0.0.1"},
         {component_host, "amqp.localhost.lshift.net"},
         {component_port, 5288},
         {component_secret, "secret"}
        ]},
  {applications, [kernel, stdlib, exmpp]}]}.
