
To run test, you will need to run an instance of gmeta in the same host.  To run gometa, you need a config file as follows:

{
    "Host" : {
	        "ElectionAddr" : "localhost:9883",
		    "MessageAddr"  : "localhost:9884",
		    "RequestAddr"  : "localhost:9885"
    }
}

To run gometa

<gometa-binary> -config="<name of config file>"

