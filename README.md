# hazelcast-cloud-csharp-sample-client

Sample C# Client For Hazelcast Cloud Community

Requires a cluster on Hazelcast Cloud, with a name and a discovery token. In addition, in order to support SSL,
a PFX certificate file is required along with the corresponding password.

Run the client with 100 iterations and no SSL with:
```
Client/bin/Debug/netcoreapp3.1/client.exe \
    hazelcast.networking.cloud.discoveryToken=<token> \
    hazelcast.clusterName=<name> \
    -n100
```

Run the client with 100 iterations *and* SSL with:
```
Client/bin/Debug/netcoreapp3.1/client.exe \
    hazelcast.networking.cloud.discoveryToken=<token> \
    hazelcast.clusterName=<name> \
    hazelcast.networking.ssl.enabled=true \
    hazelcast.networking.ssl.certificatePath=<path/to/pfx> \
    hazelcast.networking.ssl.certificatePassword=<password> \
    -n100
```

Note that these two commands use the command-line configuration style for Hazelcast. It is also possible to
configure Hazelcast via a JSON configuration file, or directly in the code. See details in the `Program.cs`
file, and documentation on the [documentation pages](https://hazelcast.github.io/hazelcast-csharp-client/).