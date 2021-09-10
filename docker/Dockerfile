# vim:set ft=dockerfile:
FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && \
    apt-get install -y g++ git make autoconf automake ca-certificates libtool \
       libfcgi-dev libxml2-dev zlib1g-dev \
       expat libexpat1-dev liblz4-dev libbz2-dev libicu-dev \
       libfmt-dev libpcre2-dev libcereal-dev libgoogle-perftools-dev \
      --no-install-recommends && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN git clone --depth 1 https://github.com/mmd-osm/Overpass-API.git overpass && \
    cd overpass && \
    git submodule init && \
    git submodule update && \
    cd ..

WORKDIR /overpass

# Compile sources
RUN cd src/ && \
    chmod u+x test-bin/*.sh && \
    autoscan && \
    aclocal && \
    autoheader && \
    libtoolize && \
    automake --add-missing && \
    autoconf && \
    cd .. && \
    mkdir -p build
   
RUN cd build && \
   ../src/configure CXXFLAGS="-D_FORTIFY_SOURCE=2 -fexceptions -fpie -Wl,-pie -fpic -shared -fstack-protector-strong -Wl,--no-as-needed -pipe -Wl,-z,defs -Wl,-z,now -Wl,-z,relro -fno-omit-frame-pointer -flto -fwhole-program  -O2" LDFLAGS="-ltcmalloc -flto -fwhole-program -lfmt" --prefix=/root/overpass --enable-lz4 --enable-fastcgi --enable-tests

RUN cd build && \
    make V=0 -j3 && \
    make install && \
    cp -R test-bin/ bin/ cgi-bin/ ../src && \
    export PATH=$PATH:/root/overpass/bin:/root/overpass/cgi-bin:/root/overpass/test-bin && \
    cd ../osm-3s_testing && \
    ../src/test-bin/run_testsuite.sh 200 notimes

# Compile export tool for 0.7.56.9 database files
RUN tar xfz upstream/osm-3s_v0.7.56.9.patched.tar.gz && \
    cd osm-3s_v0.7.56.9 && \
    ./configure && \
    make V=0 -j3 bin/export_tables && \
    strip bin/export_tables && \
    cp bin/export_tables /root/overpass/bin/export_tables_0756 && \
    cd .. && \
    rm -rf osm-3s_v0.7.56.9/

FROM ubuntu:20.04

RUN apt-get update -qq && \
    apt-get install -y \
       libfcgi-bin zlib1g expat libicu66 liblz4-1 libpcre2-8-0 libgomp1 libgoogle-perftools4 \
       --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*  

COPY --from=builder /root/overpass/bin /opt/overpass/bin
COPY --from=builder /root/overpass/cgi-bin /opt/overpass/cgi-bin
COPY --from=builder /overpass/src/rules /opt/overpass/rules

RUN groupadd -g 62000 overpass && \
    useradd -g 62000 -l -M -s /bin/false -u 62000 overpass

USER overpass
