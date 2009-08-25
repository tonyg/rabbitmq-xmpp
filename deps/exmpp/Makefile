all: ebin

clean:
	rm -rf exmpp-0.9.1 build doc ebin include priv

veryclean: clean
	rm exmpp-0.9.1.tar.gz

exmpp-0.9.1.tar.gz:
	wget -O exmpp-0.9.1.tar.gz http://download.process-one.net/exmpp/exmpp-0.9.1-r851.tar.gz
exmpp_component.diff:
	wget --no-check-certificate https://support.process-one.net/secure/attachment/14854/exmpp_component.diff

build: exmpp-0.9.1.tar.gz exmpp_component.diff
	tar -xzf exmpp-0.9.1.tar.gz
	mv exmpp-0.9.1-r851 build
	(cd build; patch -p0 <../exmpp_component.diff; patch -p0 <../exmpp_component-extra.diff)

build/Makefile: build
	(cd build; ./configure --prefix=`pwd`/..)

exmpp-0.9.1: build/Makefile
	make -C build
	make -C build install

ebin: exmpp-0.9.1
	mv exmpp-0.9.1/* .
