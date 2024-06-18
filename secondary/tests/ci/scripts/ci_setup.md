## Docker based CI setup

To avoid a lot of setup issues, indexing CI is configured to be run in a docker container. This can also be used
as a test setup environment for local runs. Follow the document to setup a fully functioning test env for indexing CI.

### Prerequisites

Docker only provides the runtime environment but we still need some artifacts to create the runtime.

-   Functioning docker environment with support for docker-compose
-   valid github and gerrit keys
-   Testdata

### Running CI

Once we have the above files, running is a few configurations away. By default, a lot of these configurations
are set to default values so no need to change them -

-   WORKSPACE: the dir to pull and sync couchbase-server into (default: /home/bot/build)
-   MANIFEST: path to manifest file under github.com/couchbase/build-manifests (default: couchbase-server/morpheus/8.0.0.xml)
-   CINAME: name of the CI server. (default: ci2i-unstable)
-   CIBOT: true
-   RELEASE: release version (default: 8.0.0)
-   MODE: indexing tests to run (default: "sanity,unit,functional,integration")
-   BRANCH: branch to checkout for indexing and dependent repositories (default: "unstable")
-   STORAGE: storage mode to run the tests on (default: "plasma")
-   USERNAME: gerrit username (default: cbci)
-   PEGGED: the build number of a release to checkout, eg: 1644. This config has no default value but we can use an env var to set this.

From the above configs, we mainly need to configure the `MANIFEST`, `RELEASE` and `PEGGED`.
Others can continue using the default values.

The above configs need to be changed in the file `setup/run_ci_dc.yaml` in "services.ci.build.args" object before running
the docker commands.

Before running the docker commands, we need to make sure that we have the `testdata` unzipped in `$HOME/testdata`

Now to run the long running CI, run the following command -

```sh
cd setup
export PEGGED=1644 # (sample pegged version)

# use docker-compose or docker compose on what is available on the system
docker-compose -f run_ci_dc.yaml build && \
docker-compose -f run_ci_dc.yaml up -d
# --abort-on-container-exit --exit-code-from ci
```

This also sets up an apache web server server on port 80 so you can view the previous CI run and
current run progress on http://localhost/gsi-current.html

The compose file also specifies certain volumes which are persisted across runs. These volumes are
mainly for build dependency cache and html files.

> **ℹ️** We use some custom paths in all places because the volume mounts attached for persistance
> means the `bot` user has limited permissions to read write to them and we cannot run with `root`
> user as that provides too many permissions and certain tests fail in that scenario

#### Running with different parameters

The way docker works, we can treat the container as a machine itself and change
any CI configurations. Although functionally correct, this may not be the best
approach as these base images are made in such a way that future upgrades (like
OS upgrades, new machine setup, etc) is not manual and configurable. So to run,
next `PEGGED` version or change any manifest, we can simply rebuild the docker
images and recreate the containers.

```sh
cd setup
echo "change any of the CI parameters and save the file"
$EDITOR run_ci_dc.yaml

read -p "New pegged version:" new_pegged
export PEGGED=$new_pegged

docker-compose -f run_ci_dc.yaml build && \
docker-compose -f run_ci_dc.yaml down --remove-orphans && \
docker-compose -f run_ci_dc.yaml up -d
```

#### Debugging

Debugging can be broadly divided into 2 sections: Live and Dead.

- **Live Debugging**
Live Debugging requires that the container is running. To live debug any issue,
we can exec into the container and treat it as a machine/VM itself. It can be
done easily using the following command

```sh
read -p "Enter current pegged version:" new_pegged
export PEGGED=$new_pegged

docker-compose -f run_ci_dc.yaml exec -u root ci /bin/bash
```

- **Dead Debugging**
The container is supposed to be a long running process. If the long running,
process has died for some reason, that is when we can do dead debugging. This
usually indicates an issue with running the CI scripts itself. To look at the
output of the CI scripts, we do the following -

```sh
read -p "Enter current pegged version:" new_pegged
export PEGGED=$new_pegged

docker-compose -f run_ci_dc.yaml logs
```
The logs out there could indicate what the issue is.

	- Permission Issues
	The most common issue is permission denied on volume paths. If this happens.
	we can delete the volumes and restart the CI commands. To delete volumes -
	```sh
	read -p "Enter current pegged version:" new_pegged
	export PEGGED=$new_pegged

	docker-compose -f run_ci_dc.yaml down --rmi local --volumes
	```

### Running standalone tests

To run tests on a local setup, we need to change some configs as described from the setup above
but in a different file `setup/run_standalone_dc.yaml`. The configs remain the same as before -

-   WORKSPACE: the dir to pull and sync couchbase-server into (default: /home/bot/build)
-   MANIFEST: path to manifest file under github.com/couchbase/build-manifests (default: couchbase-server/morpheus/8.0.0.xml)
-   CINAME: name of the CI server. (default: ci2i-unstable)
-   CIBOT: false
-   RELEASE: release version (default: 8.0.0)
-   MODE: indexing tests to run (default: "sanity,unit,functional,integration")
-   BRANCH: branch to checkout for indexing and dependent repositories (default: "unstable")
-   STORAGE: storage mode to run the tests on (default: "plasma")
-   USERNAME: gerrit username (default: $GITHUB_USERNAME env var)
-   PEGGED: the build number of a release to checkout, eg: 1644. This config has no default value but we can use an env var to set this.

From the above configs, we need configure `MANIFEST`, `RELEASE`, `PEGGED` and `USERNAME`.
Others can continue with default values.

Before running the docker commands, we need to make sure that we have the `testdata` unzipped in `$HOME/testdata`.
Also the standalone test runs some basic volume mappings to directories on local system -
- $HOME/tmp:/home/bot/build - repo cache
- $PWD:/var/www - run html artifacts
- $HOME/container_build_cache/cbdepscache:/home/bot/.cbdepscache:rw - build cache
- $HOME/container_build_cache/cbdepcache:/home/bot/.cbdepcache:rw - build cache

These directories may not exist before hand and docker compose will create them. But it is important
to note their existence as their cleanup is going to user driven and not by docker compose lifecycle.

To run the tests, we still need to configure the github_key and gerrit_key so that ci machine can
pull/sync the repo. One thing to note is these keys should not be password protected unlike how we
usually store them on our systems. This is because we cannot forward the ssh-agent to docker on MacOS
and windows. To reuse the existing private keys, we will create password less versions of the same keys -
```sh
cd $HOME/.ssh

cp <github private key> github_password_less
# when asking for new password, leave it blank
ssh-keygen -p -f github_password_less

cp <gerrit private key> gerrit_password_less
# when asking for new password, leave it blank
ssh-keygen -p -f gerrit_password_less
```

Now to run the standalone tests -
```sh
cd setup
export PEGGED=1644 # (sample pegged version)
export GITHUB_USERNAME=<github username>

# use docker-compose or docker compose on what is available on the system
docker-compose -f run_standalone_dc.yaml build && \
docker-compose -f run_standalone_dc.yaml up \
	--abort-on-container-exit --exit-code-from ci
```

This will run one iteration of the CI tests and exit. We can look at the current progress and artifacts
on an apache2 web server on url http://localhost:8080/

> **ℹ️** If the docker commands fail with permission errors on `/var/www` then it is probably a side
> effect of rootless docker installation. To fix this, replace all the system volume paths with
> docker managed volumes. Sample for the same can be found in `run_ci_dc.yaml`

### Understanding the Dockerfile

There are 2 dockerfile in our setup: `ci_machine.dockerfile` and `apache_server.dockerfile`

#### apache2_server

This is simply the httpd docker images extended with our custom apache2 conf to serve the html artifacts
generated from CI runs. Config for the apache server can be found in `setup/apache2.conf`

#### ci_machine

This file extends the ~build teams's base docker images for linux-cv~ ubuntu:jammy image. It sets up
some basic tools like make, cmake, git, repo, etc which are useful for building the couchbase-server.

<br>
Let's dive deeper into some steps it performs -

* Setting up dependencies:
	```sh
	# system dependencies for debugging
	RUN apt-get update && \
		DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y build-essential lsof lshw sysstat net-tools numactl bzip2 runit ca-certificates gpg wget curl git locales locales-all vim gdb nano

	# dependencies to sync and build cluster
	RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
		echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null && \
		apt-get update &&  rm /usr/share/keyrings/kitware-archive-keyring.gpg && \
		apt-get install silversearcher-ag && \
		apt-get install kitware-archive-keyring && \
		DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install cmake -y && \
		ln -s /usr/bin/python3 /usr/bin/python && \
		export REPO=$(mktemp /tmp/repo.XXXXXXXXX) && \
		curl -o ${REPO} https://storage.googleapis.com/git-repo-downloads/repo && install -m 755 ${REPO} /usr/bin/repo
	```
* Creating a user for running tests and setting up basic folders (root is not allowed as root as too many permissions and plasma tests will fail on root):
	```sh
	RUN useradd -rm -d /home/bot -s /bin/bash -g root -G root bot
	RUN mkdir /home/bot/.ssh
	RUN mkdir /var/www

	...

	RUN mkdir ${WORKSPACE}
	RUN mkdir /home/bot/bin
	WORKDIR ${WORKSPACE}
	```
* Git and ssh configuration for repo sync and gerrit pulls -
	```sh
	# ssh config
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

	# git config
	COPY <<EOF /home/bot/.gitconfig
	[user]
		name = CB Robot
		email = build@northscale
	[ui]
		color = auto
	EOF
	```
* Bundling CI scripts in image -
	```sh
	ARG ciscripts_dir=/home/bot/.ciscripts
	ENV ciscripts_dir=${ciscripts_dir}

	RUN git clone -q https://github.com/couchbase/indexing.git ${ciscripts_dir} && \
		cd ${ciscripts_dir} && \
		git checkout ${BRANCH} && \
		git pull -q
	```
* Configuring default run on container start:
	```sh
	CMD ${ciscripts_dir}/secondary/tests/ci/scripts/domain
	```
