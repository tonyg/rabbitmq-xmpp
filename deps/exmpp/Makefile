all: exmpp.ez

clean:
	rm -rf exmpp-0.9.1 build doc ebin include priv exmpp.ez

veryclean: clean
	rm exmpp-0.9.1.tar.gz

exmpp-0.9.1.tar.gz:
	wget -O exmpp-0.9.1.tar.gz http://download.process-one.net/exmpp/exmpp-0.9.1-r856.tar.gz

build: exmpp-0.9.1.tar.gz 
	tar -xzf exmpp-0.9.1.tar.gz
	mv exmpp-0.9.1-r856 build

build/Makefile: build
	(cd build; ./configure --prefix=`pwd`/..)

exmpp-0.9.1: build/Makefile
	make -C build
	make -C build install

ebin: exmpp-0.9.1
	mv exmpp-0.9.1/* .

exmpp.ez: ebin
	rm -rf build/exmpp
	mkdir -p build/exmpp
	cp -r ebin include build/exmpp
	(cd build; zip -r ../exmpp.ez exmpp)
