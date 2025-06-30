FROM ubuntu:22.04@sha256:67cadaff1dca187079fce41360d5a7eb6f7dcd3745e53c79ad5efd8563118240

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y htop curl parallel git zstd gzip \
    python3 python-is-python3 python3-dev python3-pip python3-venv \
    build-essential cmake libuchardet-dev libzip-dev \
    libboost-thread-dev libboost-regex-dev libboost-filesystem-dev \
    libboost-log-dev libboost-iostreams-dev libboost-locale-dev libboost-program-options-dev
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y --default-toolchain=1.86.0
RUN pip install uv
ENV PATH="/root/.cargo/bin:${PATH}"

RUN curl -Lo /trufflehog.tgz https://github.com/trufflesecurity/trufflehog/releases/download/v3.88.28/trufflehog_3.88.28_linux_amd64.tar.gz
RUN tar xvf /trufflehog.tgz -C /usr/bin trufflehog

RUN cargo install \
    --root /usr/local \
    fst-bin

RUN git clone --jobs 8 --recursive https://github.com/bitextor/warc2text /opt/warc2text \
    && mkdir /opt/warc2text/build \
    && cd /opt/warc2text/build \
    && cmake .. \
    && make -j16 \
    && ln -s /opt/warc2text/build/bin/warc2text /usr/local/bin/warc2text

COPY requirements.txt /opt/reqs.txt
RUN uv pip install --system -r /opt/reqs.txt \
    && git clone -b openlidv2 https://github.com/zjaume/heli-otr.git \
    && cd heli-otr \
    && git checkout 0988902 \
    && uv pip install --system . \
    && heli-convert \
    && rm -fr /heli-otr

COPY utils /opt/monotextor_utils
RUN cargo install \
    --locked \
    --root /usr/local \
    --path /opt/monotextor_utils

# remove all the cargo build cache and rustup
RUN rm -r /root/.cargo \
    /root/.rustup \
    /opt/warc2text/.git \
    /opt/monotextor_utils

ENV LC_ALL C.UTF-8
