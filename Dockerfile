FROM ubuntu:22.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y curl parallel git python3 python-is-python3 python3-dev python3-pip python3-venv
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

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

# remove all the cargo build cache and rustup
RUN rm -r /root/.cargo \
    /root/.rustup \
    /opt/monotextor_utils

ENV LC_ALL C.UTF-8
