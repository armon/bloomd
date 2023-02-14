############
# BUILDER - Used to build the bloomd binary
############

FROM alpine:3.10.2 as BUILDER

WORKDIR /etc/bloomd

RUN apk add --no-cache build-base gcc py-pip

RUN pip install SCons

COPY deps deps

RUN cd deps/check-0.9.8 && ./configure && make && make install

RUN cd /etc/bloomd

COPY src src
COPY SConstruct SConstruct

RUN scons


############
# RUNNER - Use Bloomd binary generated in the builder above
############

FROM alpine:3.10.2 as RUNNER

WORKDIR /etc/bloomd
RUN mkdir /data

ENV BLOOMD_PORT=8673
ENV BLOOMD_LOG_LEVEL=INFO
ENV BLOOMD_DATA_DIR=/data
ENV BLOOMD_FLUSH_INTERVAL=300
ENV BLOOMD_WORKERS=2

COPY --from=BUILDER /etc/bloomd/bloomd /etc/bloomd/bloomd

RUN printf "#!/bin/sh\n\
printf \"[bloomd]\\ntcp_port = \$BLOOMD_PORT\\ndata_dir = \$BLOOMD_DATA_DIR\\nlog_level = \$BLOOMD_LOG_LEVEL\\nflush_interval = \$BLOOMD_FLUSH_INTERVAL\\nworkers = \$BLOOMD_WORKERS\" > /etc/bloomd/bloomd.conf \n \
./bloomd -f /etc/bloomd/bloomd.conf" > entry.sh

RUN chmod +x entry.sh

CMD ["./entry.sh"]
