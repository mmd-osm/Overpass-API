# vim:set ft=dockerfile:
FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && \
    apt-get install -y g++ git make autoconf automake ca-certificates libtool \
       libfcgi-dev libxml2-dev zlib1g-dev \
       expat libexpat1-dev liblz4-dev libbz2-dev libicu-dev \
       libfmt-dev libpcre2-dev libcereal-dev libtcmalloc-minimal4 \
      --no-install-recommends && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN git clone --recurse-submodules https://github.com/mmd-osm/Overpass-API.git overpass

WORKDIR /overpass

# Compile, install and remove source
RUN cd src/ && \
    chmod u+x test-bin/*.sh && \
    autoscan && \
    aclocal && \
    autoheader && \
    libtoolize && \
    automake --add-missing && \
    autoconf && \
    cd ..&& \
    mkdir -p build
   
RUN cd build && \
   ../src/configure CXXFLAGS="-D_FORTIFY_SOURCE=2 -fexceptions -fpie -Wl,-pie -fpic -shared -fstack-protector-strong -Wl,--no-as-needed -pipe -Wl,-z,defs -Wl,-z,now -Wl,-z,relro -fno-omit-frame-pointer -flto -fwhole-program  -O2" LDFLAGS="-flto -fwhole-program -lfmt" --prefix=/root/overpass --enable-lz4 --enable-fastcgi --enable-tests

RUN cd build && \
    make V=0 -j3 && \
    make install && \
    cp -R test-bin/ bin/ cgi-bin/ ../src && \
    export PATH=$PATH:/root/overpass/bin:/root/overpass/cgi-bin:/root/overpass/test-bin && \
    cd ../osm-3s_testing && \
    ../src/test-bin/run_testsuite.sh 2000 notimes



