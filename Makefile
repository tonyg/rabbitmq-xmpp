PACKAGE=rabbitmq-xmpp
DEPS=rabbitmq-erlang-client
INTERNAL_DEPS=exmpp
TEST_APPS=exmpp rabbit_xmpp
TEST_SCRIPTS=spec/run-specs.sh
START_RABBIT_IN_TESTS=true
TEST_ARGS=-rabbit_xmpp component_port 15288 -rabbit_xmpp component_host \"amqp.localhost.lshift.net\"

include ../include.mk
