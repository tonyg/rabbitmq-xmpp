PACKAGE=rabbitmq-xmpp
DEPS=rabbitmq-erlang-client
INTERNAL_DEPS=exmpp
TEST_APPS=exmpp rabbit_xmpp
START_RABBIT_IN_TESTS=true

include ../include.mk
