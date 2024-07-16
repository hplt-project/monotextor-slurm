FROM ubuntu:22.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y htop curl parallel git zstd gzip \
    python3 python-is-python3 python3-dev python3-pip python3-venv \
    build-essential cmake libuchardet-dev libzip-dev \
    libboost-thread-dev libboost-regex-dev libboost-filesystem-dev \
    libboost-log-dev libboost-iostreams-dev libboost-locale-dev libboost-program-options-dev
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y --default-toolchain=1.77.2
ENV PATH="/root/.cargo/bin:${PATH}"

RUN git clone --recursive https://github.com/bitextor/warc2text /opt/warc2text \
    && mkdir /opt/warc2text/build \
    && cd /opt/warc2text/build \
    && cmake .. \
    && make -j16 \
    && ln -s /opt/warc2text/build/bin/warc2text /usr/local/bin/warc2text

COPY requirements.txt /opt/reqs.txt
RUN pip install -U pip \
    && pip install -r /opt/reqs.txt \
    && git clone -b openlid193 https://github.com/zjaume/heli-otr.git \
    && cd heli-otr \
    && pip install . \
    && heli-convert \
    && rm -fr /heli-otr

COPY utils /opt/monotextor_utils
RUN cargo install \
    --root /usr/local \
    --path /opt/monotextor_utils

RUN cargo install \
    --root /usr/local \
    fst-bin

# remove all the cargo build cache and rustup
RUN rm -r /root/.cargo \
    /root/.rustup \
    /opt/warc2text/.git \
    /opt/monotextor_utils

ENV LC_ALL C.UTF-8
