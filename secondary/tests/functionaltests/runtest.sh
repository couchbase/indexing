export NS_SERVER_CBAUTH_URL="http://127.0.0.1:9000/_cbauth"
export NS_SERVER_CBAUTH_USER="Administrator"
export NS_SERVER_CBAUTH_PWD="asdasd"
export NS_SERVER_CBAUTH_RPC_URL="http://127.0.0.1:9000/cbauth-demo"

export CBAUTH_REVRPC_URL="http://Administrator:asdasd@127.0.0.1:9000/query"

go "$@"
