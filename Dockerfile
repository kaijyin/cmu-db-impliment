FROM ubuntu:20.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      clang-8 \
      clang-format-8 \
      clang-tidy-8 \
      cmake \
      doxygen \
      git \
      g++-7 \
      pkg-config \
      valgrind \
      zlib1g-dev \
      # s.0681
      gdb-multiarch \
      qemu-system-misc \
      gcc-riscv64-linux-gnu \
      binutils-riscv64-linux-gnu \
      # cs144
      graphviz \
      bash \
      coreutils\
      libpcap-dev \
      iptables \
      mininet \
      tcpdump \
      telnet \
      wireshark \
      socat \
      netcat-openbsd \