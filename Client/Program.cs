// Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.ComponentModel;
using System.Linq;
using System.Security.Authentication;
using System.Threading.Tasks;
using Hazelcast;
using Microsoft.Extensions.Logging;

namespace ClientWithSsl
{
    /*
     * A sample application that configures a client to connect to an Hazelcast Cloud cluster
     * with SSL, and to then put and get random values in/from a map, thus testing that the
     * connection to the Hazelcast Cloud cluster is successful.
     *
     * see: https://hazelcast.github.io/hazelcast-csharp-client/
     */
    internal static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine();
            Console.WriteLine("Hazelcast Cloud Client");

            Console.Write("Build options...");
            var options = new HazelcastOptionsBuilder()
                .With(args)
                .WithConsoleLogger()
                //.With("Logging:LogLevel:Hazelcast", "Information")
                .Build();

            // options can be provided via the command-line.
            //
            // without ssl:
            //
            // > client.exe hazelcast.networking.cloud.discoveryToken=<token> \
            //              hazelcast.clusterName=<name> \
            //              logging:logLevel:hazelcast=<level>
            //
            // with ssl:
            //
            // > client.exe hazelcast.networking.cloud.discoveryToken=<token> \
            //              hazelcast.clusterName=<name> \
            //              hazelcast.networking.ssl.enabled=true \
            //              hazelcast.networking.ssl.certificatePath=<path> \
            //              hazelcast.networking.ssl.certificatePassword=<password> \
            //              logging:logLevel:hazelcast=<level>
            //
            // or via an hazelcast.json configuration file,
            // or directly in the code by uncommenting lines below (and above for log level).
            // (see configuration docs at https://hazelcast.github.io/hazelcast-csharp-client/)
            //
            // <level> must be a valid Microsoft.Extensions.Logging.LogLevel value
            //   Trace | Debug | Information | Warning | Error | Critical | None
            // also pay attention to the options format: the default format uses ':' separators
            // and is required for non-hazelcast options, only hazelcast options support the
            // '.' separator.

            // set the cluster name
            //options.ClusterName = "YOUR_CLUSTER_NAME";

            // set the discovery token
            //options.Networking.Cloud.DiscoveryToken = "YOUR_CLUSTER_DISCOVERY_TOKEN";

            // the the cloud url base
            // note: in v4 this is not an option anymore
            //options.Networking.Cloud.UrlBase = "YOUR_DISCOVERY_URL";

            if (options.Networking.Ssl.Enabled)
            {
                // configure ssl
                options.Networking.Ssl.ValidateCertificateChain = false;
                options.Networking.Ssl.Protocol = SslProtocols.Tls12;

                // configure the ssl certificate
                //options.Networking.Ssl.CertificatePath = "client.pfx";
                //options.Networking.Ssl.CertificatePassword = "YOUR_SSL_PASSWORD";
            }

            // use the -n100 argument for 100 iterations
            var narg = args.FirstOrDefault(x => x.StartsWith("-n"));
            var total = narg != null && narg.Length > 2 && int.TryParse(narg.Substring(2), out var totalArg) && totalArg > 0 ? totalArg : 100;

            Console.WriteLine(" ok.");

            Console.WriteLine($"Ssl is {(options.Networking.Ssl.Enabled ? "" : "not ")}enabled.");

            Console.Write("Get and connect client...");
            await using var client = await HazelcastClientFactory.StartNewClientAsync(options);
            Console.WriteLine(" ok.");

            Console.Write("Get map...");
            await using var map = await client.GetMapAsync<string, string>("map");
            Console.WriteLine(" ok.");

            Console.Write("Put value into map...");
            await map.PutAsync("key", "value");
            Console.WriteLine(" ok.");

            Console.Write("Get value from map...");
            var value = await map.GetAsync("key");
            Console.WriteLine(" ok");

            Console.Write("Validate value...");
            if (value.Equals("value"))
            {
                Console.WriteLine("ok.");
            }
            else
            {
                Console.WriteLine("Error.");
                Console.WriteLine("Check your configuration.");
                return;
            }

            Console.WriteLine($"Put/Get values in/from map with random values...");
            var random = new Random();
            var step = total / 10;
            for (var i = 0; i < total; i++)
            {
                var randomValue = random.Next(100_000);
                await map.PutAsync("key_" + randomValue, "value_" + randomValue);

                randomValue = random.Next(100_000);
                await map.GetAsync("key" + randomValue);

                if (i % step == 0)
                {
                    Console.WriteLine($"[{i:D3}] map size: {await map.GetSizeAsync()}");
                }
            }

            Console.WriteLine("Done.");
        }

        public static HazelcastOptionsBuilder WithConsoleLogger(this HazelcastOptionsBuilder builder)
        {
            return builder
                .With("Logging:LogLevel:Default", "None")
                .With("Logging:LogLevel:System", "None")
                .With("Logging:LogLevel:Microsoft", "None")
                .With((configuration, options) =>
                {
                    // configure logging factory and add the console provider
                    options.LoggerFactory.Creator = () => LoggerFactory.Create(loggingBuilder =>
                        loggingBuilder
                            .AddConfiguration(configuration.GetSection("logging"))
                            .AddConsole());
                });
        }
    }
}
