all: var

clean:
	rm -rf build sbin var etc ejabberd.ez

veryclean: clean
	rm ejabberd-2.0.5.tar.gz

ejabberd-2.0.5.tar.gz:
	wget -O ejabberd-2.0.5.tar.gz http://www.process-one.net/downloads/ejabberd/2.0.5/ejabberd-2.0.5.tar.gz

build: ejabberd-2.0.5.tar.gz
	tar -xzf ejabberd-2.0.5.tar.gz
	mv ejabberd-2.0.5 build

build/src/Makefile: build
	(cd build/src; ./configure --prefix=`pwd`/../..)

var: build/src/Makefile
	make -C build/src
	make -C build/src install
	cp ejabberd-rabbit.cfg etc/ejabberd/ejabberd.cfg
	# Create fake files for compatibility with standard detection
	touch ejabberd.ez
	mkdir -p ebin

start-ejabberd:
	rm -rf var/lib/ejabberd/db/*
	sbin/ejabberdctl start
	sleep 2
	sbin/ejabberdctl register user localhost password

stop-ejabberd:
	sbin/ejabberdctl stop
