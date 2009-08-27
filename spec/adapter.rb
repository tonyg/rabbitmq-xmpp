require 'rubygems'
require 'timeout'
require 'bunny'
require 'xmpp4r'

describe "RabbitMQ XMPP Adapter" do
  it "should support sending a message to a known exchange" do
    against_ejabberd do |client|
      against_rabbit do |mq|
        x = mq.exchange("listener")
        q = mq.queue("listenerq")
        q.bind(x)

        msg = Jabber::Message.new("listener@amqp.localhost.lshift.net", "Hello")
        msg.set_type(:chat)
        client.send(msg)

        msg = wait_for_message(q)
        msg.should == "Hello"
      end
    end
  end
end

def against_ejabberd
  client = Jabber::Client.new("user@localhost")
  client.connect("localhost", 15222)
  client.auth("password")
  begin
    yield client
  ensure
    client.close
  end
end

def against_rabbit
  bunny = Bunny.new
  bunny.start
  begin
    yield bunny
  ensure
    bunny.stop
  end
end

def wait_for_message(q, retry_delay=0.1, retry_count=10)
  (1..retry_count).each do |i|
    msg = q.pop
    return msg if msg
    sleep retry_delay
  end
end
