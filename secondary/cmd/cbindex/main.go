package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	c "github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/security"
)

func doRequest(method, url string) (body []byte, err error) {

	r, err := security.GetWithAuthNonTLS(url, nil)
	if err != nil {
		errMsg := "Error while getting with auth, err: " + err.Error()
		fmt.Fprintln(os.Stderr, errMsg)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		err := url + " returned: " + r.Status
		return nil, errors.New(err)
	}

	return ioutil.ReadAll(r.Body)
}

func get(url string) (value []byte, err error) {

	config, err := doRequest("GET", url)
	if err != nil {
		errMsg := "Cbindex get returned err: " + err.Error()
		fmt.Fprintln(os.Stderr, errMsg)
		return nil, err
	}

	return config, nil
}

func updateConfig(value []byte, config c.Config) {

	newConfig, err := c.NewConfig(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cbindex updateConfig err: "+err.Error())
	}
	// queryport.client configs used with & without prefix trim because
	// metadataClient refers 'servicesNotifierRetryTm'
	// but ClientSettings refers 'queryport.client.servicesNotifierRetryTm'
	config.Update(newConfig.Json())
}

func usage(fset *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "Usage: cbindex [options]")
	fset.PrintDefaults()
	fmt.Fprintln(os.Stderr, `Examples:

- Scan
    cbindex -auth user:pass -type=scanAll -bucket default -index abcd
    cbindex -auth user:pass -type=scanAll -index abcd -limit 0
    cbindex -auth user:pass -type=scanAll -index abcd -limit 0 -consistency true
    cbindex -auth user:pass -type=scan -index state -low='["Ar"]' -high='["Co"]' -buffersz=300
    cbindex -auth user:pass -type=scan -index name_state_age -low='["Ar"]' -high='["Arlette", "N"]'
    cbindex -auth user:pass -type scan -index '#primary' -equal='["Adena_54605074"]'

- Create/Drop
    cbindex -auth user:pass -type create -bucket default -using memdb -index first_name -fields=first_name,last_name
    cbindex -auth user:pass -type create -bucket default -primary=true -index primary
    cbindex -auth user:pass -type drop -bucket default -index first_name 

- N1QL support (Only create index is supported currently)
	cbindex -auth Administrator:pass -type n1ql -index_ddl "CREATE INDEX idx_source_schedule ON travel-sample.inventory.route(sourceairport,  DISTINCT ARRAY v.flight FOR v IN schedule END );" -server 127.0.0.1:9000
	cbindex -auth Administrator:pass -type n1ql -index_ddl "CREATE PRIMARY INDEX def_inventory_airport_primary ON travel-sample.inventory.airport WITH { \"defer_build\":true };" -server 127.0.0.1:9000
    cbindex -auth Administrator:pass -type n1ql -index_ddl "CREATE INDEX ixf_sched ON travel-sample.inventory.route (ALL ARRAY FLATTEN_KEYS(s.day DESC, s.flight) FOR s IN schedule END, sourceairport, destinationairport, stops);" -server 127.0.0.1:9000

- List
    cbindex -auth user:pass -type list
    cbindex -auth user:pass -type nodes

- Move
    Single Index:
    cbindex -auth user:pass -type move -index 'def_airportname' -bucket default -with '{"nodes":"10.17.6.32:8091"}'

    Index And 1 Replica:
    cbindex -auth user:pass -type move -index 'def_airportname' -bucket default -with '{"nodes":["10.17.6.32:8091","10.17.6.33:8091"]}'
    (Move Index supports moving only 1 index (and its replicas) at a time)

- Alter
	Single Index:
	cbindex -auth user:pass -type alter -bucket default -index index1 -with '{"action":"replica_count","num_replica":2}'
	cbindex -auth user:pass -type alter -bucket default -index index1 -with '{"action":"drop_replica","replicaId":1}'
	(Alter Index supports changing only 1 index (and its replicas) at a time)
	`)
}

func main() {
	logging.SetLogLevel(logging.Warn)
	runtime.GOMAXPROCS(runtime.NumCPU())

	cmdOptions, _, fset, err := querycmd.ParseArgs(os.Args[1:])
	if err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	} else if cmdOptions.Help || len(cmdOptions.OpType) < 1 {
		usage(fset)
		os.Exit(0)
	}

	logLevel := strings.ToUpper(cmdOptions.UseLogLevel)
	logging.SetLogLevel(logging.Level(logLevel))
	if cmdOptions.UseTools {
		creds := strings.Split(cmdOptions.Auth, ":")
		if len(creds) < 2 || creds[0] == "" || creds[1] == "" {
			logging.Errorf("Error in setting config: use format -auth user:password with non-empty creds")
			return
		}
		var insecureSkipVerify bool
		if cmdOptions.CACert == "" {
			insecureSkipVerify = true
		}
		err := security.SetToolsConfig(creds[0], creds[1], cmdOptions.CACert, insecureSkipVerify, true)
		if err != nil {
			logging.Errorf("Error in setting config: %v", err)
			return
		}
	} else if cmdOptions.UseTLS {
		querycmd.InitToolsSecurityContext(cmdOptions.Server, "", "", "", cmdOptions.CACert, true)
	}

	if os.Getenv("CBAUTH_REVRPC_URL") == "" && cmdOptions.Auth != "" && !cmdOptions.UseTools {
		// unfortunately, above is read at init, so we have to respawn
		revrpc := fmt.Sprintf("http://%v@%v/query2", cmdOptions.Auth, cmdOptions.Server)
		os.Setenv("CBAUTH_REVRPC_URL", revrpc)
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		exitcode := 0
		if err := cmd.Run(); err != nil {
			if status, ok := err.(*exec.ExitError); ok {
				exitcode = status.Sys().(syscall.WaitStatus).ExitStatus()
			}
		}
		os.Exit(exitcode)
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)

	if cmdOptions.RefreshSettings && !cmdOptions.UseTools {
		config.Set("needsRefresh", c.ConfigValue{true, "read upto date settings from metakv", true, false, false})
	} else if cmdOptions.RefreshSettings && cmdOptions.UseTools {
		//get indexer config from server to avoid metakv

		server := cmdOptions.Server
		server, _, _ = net.SplitHostPort(server)
		if !strings.HasPrefix(server, "https://") {
			server = "https://" + server
		}

		current, err := get(server + ":" + cmdOptions.IndexerPort + "/settings?internal=ok")
		if err != nil {
			msg := "Cbindex Config skipping update to config: " + config.String()
			fmt.Fprintln(os.Stderr, msg)
		} else {
			msg := "Cbindex Config before update: " + config.String()
			fmt.Fprintln(os.Stderr, msg)

			updateConfig(current, config)

			msg = "Cbindex Config after update: " + config.String()
			fmt.Fprintln(os.Stderr, msg)
		}
	} else if cmdOptions.UseTools {
		// GsiClient.config is set from config passed here
		// - ClientSettings.handleSettings read "queryport.client.*"						from GsiClient.config
		// - GsiScanClient reads "readDeadline" instead of "queryport.client.readDeadline"	from GsiClient.config
		// Use SystemConfig for ClientSettings
		// Before MB-59788, ClientSettings.config was nil & overridden by common.SystemConfig.Clone()
		// For useTools, config needs to be passed explicitly
		sysConfig := c.SystemConfig.Clone()
		config.Update(sysConfig.Json())
	}

	client, err := qclient.NewGsiClient(cmdOptions.Server, config)
	if err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	}

	if err = querycmd.HandleCommand(client, cmdOptions, false, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
	}
	client.Close()
}
