# syntax=docker/dockerfile:1.4
FROM ubuntu:jammy

RUN apt-get update && \
	DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y build-essential lsof lshw sysstat net-tools numactl bzip2 runit ca-certificates gpg wget curl git locales locales-all vim gdb nano

RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
	echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null && \
	apt-get update &&  rm /usr/share/keyrings/kitware-archive-keyring.gpg && \
	apt-get install kitware-archive-keyring && \
	DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install cmake -y

RUN ln -s /usr/bin/python3 /usr/bin/python && \
	export REPO=$(mktemp /tmp/repo.XXXXXXXXX) && \
	curl -o ${REPO} https://storage.googleapis.com/git-repo-downloads/repo && install -m 755 ${REPO} /usr/bin/repo

ARG USERNAME=cbci

RUN useradd -rm -d /home/bot -s /bin/bash -g root -G root bot

RUN mkdir /home/bot/.ssh /home/bot/.cbdepscache /home/bot/.cbdepcache
RUN mkdir /var/www

RUN cat > /home/bot/.ssh/config <<EOF
Host github.com
	User git
	Hostname github.com
	PreferredAuthentications publickey
	IdentityFile /home/bot/.ssh/github
	StrictHostKeyChecking no

Host review.couchbase.org
	User ${USERNAME}
	Port 29418
	IdentityFile /home/bot/.ssh/gerrit
	StrictHostKeyChecking no
EOF

RUN --mount=type=secret,id=github_key,required=true \
	cp /run/secrets/github_key /home/bot/.ssh/github

RUN --mount=type=secret,id=gerrit_key,required=true \
	cp /run/secrets/gerrit_key /home/bot/.ssh/gerrit

ARG ciscripts_dir=/home/bot/.ciscripts
ENV ciscripts_dir=${ciscripts_dir}

RUN chown -R bot:root /home/bot/.ssh /home/bot/.cbdepscache /home/bot/.cbdepcache
RUN chown -R bot:root /var/www

ENV LANG=en_US.UTF-8
ENV LANGUAGE=${LANG}
ENV LC_ALL=${LANG}

RUN locale-gen ${LANG} && update-locale LANG=${LANG}

USER bot

EXPOSE 8091-8095

EXPOSE 11200-11220

# cluster run ports
EXPOSE 9000-9120

EXPOSE 2222:22

# EXPOSE 80

# mount static dirs so that we don't have to re-download lot of things
VOLUME [ "/home/bot/build", "/home/bot/.cbdepscache", "/home/bot/.cbdepcache" ]

# mount the /var/www/ so that we have all the output html files intact
VOLUME [ "/var/www/" ]

# default env variables - once an image is built with these, we need to rebuild the image
# to run a new PEGGED version
ARG WORKSPACE=/home/bot/build
ARG CINAME=ci2i-unstable
ARG CIBOT=false
ARG RELEASE=8.0.0
ARG MANIFEST="couchbase-server/morpheus/8.0.0.xml"
ARG MODE="sanity,unit,functional,integration"
ARG BRANCH="unstable"
ARG STORAGE="plasma"
ARG PEGGED

# do this to add basic ciscripts to the image so that we can run something in entrypoint
RUN git clone -q https://github.com/couchbase/indexing.git ${ciscripts_dir} && \
	cd ${ciscripts_dir} && \
	git checkout ${BRANCH} && \
	git pull -q

# setup .cienv file on the basis of ARGS
COPY <<EOF /home/bot/.cienv
export WORKSPACE=${WORKSPACE}
export CINAME=${CINAME}
export CIBOT=${CIBOT}
export RELEASE=${RELEASE}
export MANIFEST=${MANIFEST}
export MODE=${MODE}
export BRANCH=${BRANCH}
export STORAGE=${STORAGE}
export PEGGED=${PEGGED}
export PATH=$PATH:${ciscripts_dir}/secondary/tests/ci/scripts/
alias repo='repo --color=never'
EOF

COPY <<EOF /home/bot/.gitconfig
[user]
	name = CB Robot
	email = build@northscale
[ui]
	color = auto
EOF

RUN mkdir ${WORKSPACE}

RUN mkdir /home/bot/bin

WORKDIR ${WORKSPACE}

# ENTRYPOINT [ "/bin/bash", "-c" ]

# default CMD is domain to run the script only once
# set this to ${ciscripts_dir}/build for running in CI
CMD ${ciscripts_dir}/secondary/tests/ci/scripts/domain
