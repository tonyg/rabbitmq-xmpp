{application, rabbit_xmpp,
 [{description, "RabbitMQ XMPP Connector"},
  {vsn, "0.01"},
  {modules, [
  ]},
  {registered, []},
  {mod, {rabbit_xmpp_app, []}},
  {env, []},
  {applications, [kernel, stdlib, exmpp]}]}.
