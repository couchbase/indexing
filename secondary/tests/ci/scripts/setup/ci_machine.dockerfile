# syntax=docker/dockerfile:1.4
FROM ubuntu:noble

RUN apt-get update && \
	DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y build-essential lsof lshw sysstat \
	net-tools numactl bzip2 runit ca-certificates gpg wget curl git locales locales-all vim gdb \
	nano jq fuse3 libfuse3-dev

RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
	echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ noble main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null && \
	apt-get update &&  rm /usr/share/keyrings/kitware-archive-keyring.gpg && \
	apt-get install kitware-archive-keyring && \
	DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install cmake -y

RUN ln -s /usr/bin/python3 /usr/bin/python && \
	export REPO=$(mktemp /tmp/repo.XXXXXXXXX) && \
	curl -o ${REPO} https://storage.googleapis.com/git-repo-downloads/repo && install -m 755 ${REPO} /usr/bin/repo

ARG USERNAME=cbci

RUN useradd -rm -d /home/bot -s /bin/bash -u 1300 -g root -G root bot

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

# ---------------------------------------------------------------------------
# Group 1: args that affect the workspace sync + build.
# These are declared BEFORE setup-workspace so BuildKit caches the expensive
# build layer. CINAME/MODE are NOT in this group — changing them (e.g. between
# the ci and linter services) does not invalidate the build cache.
# ---------------------------------------------------------------------------
ARG WORKSPACE=/home/bot/build
ARG RELEASE=8.1.0
ARG MANIFEST="couchbase-server/totoro/8.1.0.xml"
ARG BRANCH="unstable"
ARG STORAGE="plasma"
ARG PEGGED

# Write a minimal .cienv used by setup-workspace and the scripts it calls
# (builder, repo, etc). This will be overwritten by the full .cienv in Group 2.
COPY <<EOF /home/bot/.cienv
export WORKSPACE=${WORKSPACE}
export RELEASE=${RELEASE}
export MANIFEST=${MANIFEST}
export BRANCH=${BRANCH}
export STORAGE=${STORAGE}
export PEGGED=${PEGGED}
export PATH=$PATH:${ciscripts_dir}/secondary/tests/ci/scripts/:/home/bot/bin/
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
RUN mkdir /home/bot/tmp
RUN chmod -R 0777 /home/bot/tmp
RUN chown -R bot:root /home/bot/tmp

ENV TMPDIR=/home/bot/tmp

WORKDIR ${WORKSPACE}

# Clone ciscripts so setup-workspace and container entrypoints are available.
# if you want to try local scripts, run `git daemon --base-path=<server-root>/goproj/src/github.com/couchbase --export-all`
# RUN git clone -q git://host.docker.internal/indexing/ ${ciscripts_dir} && \
RUN git clone -q https://github.com/couchbase/indexing.git ${ciscripts_dir} && \
	cd ${ciscripts_dir} && \
	git checkout ${BRANCH} && \
	git pull -q

# Populate ~/bin so setup-workspace can call builder, dolint, etc.
RUN find ${ciscripts_dir}/secondary/tests/ci/scripts \
		-not -name 'build' -not -name '.*' -type f \
		-exec cp {} /home/bot/bin/ \; && \
	chmod +x /home/bot/bin/*

# Pre-sync the full monorepo and run an EE build. This bakes the compiled
# workspace into the image so:
#   - linter containers never need to run a build
#   - ci containers only need a fast incremental rebuild for gerrit patches
#
# CINAME/MODE are not ARGs yet at this layer — BuildKit cache is shared
# between the ci and linter services (one build, two services).
#
# Run under bash with pipefail: a pipeline's exit status is its LAST command,
# so `setup-workspace ... | tee` would otherwise report tee's status (always 0)
# and mask a setup-workspace failure. That produced a cached layer with no real
# workspace/deps (e.g. grpc), silently reused by every later build. pipefail
# makes the RUN — and thus the build — fail loudly instead of caching garbage.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN export PATH=$PATH:/home/bot/bin && \
	/home/bot/bin/setup-workspace 2>&1 | tee ${WORKSPACE}/setup-workspace.log

# ---------------------------------------------------------------------------
# Group 2: final runtime config. Declared AFTER the expensive build step.
#
# The baked .cienv is ROLE-NEUTRAL: it carries only build-time facts and
# deliberately omits MODE / CINAME / CIBOT / TEST_NAME. Those are injected at
# container start by the entrypoint scripts (container-runner.sh /
# linter-runner.sh) from the compose `environment:` block. This makes the image
# a single artifact shared by both the test and lint services — one build, no
# tag collision — instead of two near-identical images racing for the same tag.
# ---------------------------------------------------------------------------

# Overwrite the minimal Group 1 .cienv with the final (role-neutral) runtime config.
COPY <<EOF /home/bot/.cienv
export WORKSPACE=${WORKSPACE}
export RELEASE=${RELEASE}
export MANIFEST=${MANIFEST}
export BRANCH=${BRANCH}
export STORAGE=${STORAGE}
export PEGGED=${PEGGED}
export PATH=$PATH:${ciscripts_dir}/secondary/tests/ci/scripts/
alias repo='repo --color=never'
EOF

# VOLUME declarations intentionally removed. They were placed before the build
# step in the original Dockerfile, which caused Docker to discard build artifacts
# written to those paths. Volumes are declared in docker-compose instead.

# default CMD is domain to run the script only once
# set this to ${ciscripts_dir}/build for running in CI
CMD ${ciscripts_dir}/secondary/tests/ci/scripts/domain
