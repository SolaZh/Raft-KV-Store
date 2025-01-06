You need to develop all your code for this lab in the nim-client.go file.

After you have made your modifications, build the nim client as follows:

    go build nim-client.go

Then, run the nim client:

    ./nim-client seed

The seed should be an integer value between `-128` to `127`.

The client configuration data is in the `./config/nim-client-config.json` file. The client and tracing server IP addresses are local addresses and should not need to be changed. However, you may need to change their port numbers if you see any conflicts (when other students run their client code on the same machine).

The server IP address and port number will be provided on Piazza.

The nim client code should start the tracing server. The tracing server configuration file should be located in `./config/tracing-server-config.json`. Read the tracing documentation as mentioned in the lab instructions.

Suppose the tracing server output is located in `./output.log`. Then you can check whether the tracing output meets the requirements specified in the lab instructions by running:

    /cad2/ece419/bin/ece419-lab1-check seed output.log

The seed should be the same value as specified on the command line for the `./nim-client` program.
