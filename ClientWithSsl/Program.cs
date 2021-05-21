using System;
using System.Security.Authentication;
using System.Threading.Tasks;
using Hazelcast;
using Hazelcast.Networking;
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
        private const int IterationCount = 100;

        public static async Task Main(string[] args)
        {
            Console.WriteLine();
            Console.WriteLine("Hazelcast Cloud Client with SSL");

            Console.Write("Build options...");
            var options = new HazelcastOptionsBuilder()
                .With(args)
                .WithConsoleLogger()
                .With("Logging:LogLevel:Hazelcast", "Information")
                .Build();

            // log level must be a valid Microsoft.Extensions.Logging.LogLevel value
            //   Trace | Debug | Information | Warning | Error | Critical | None

            if (args.Length == 0)
            {
                // it is OK to pass the arguments in the command line
                // otherwise, they must be specified here

                // set the cluster name
                options.ClusterName = "YOUR_CLUSTER_NAME";

                // set the cloud discovery token and url
                options.Networking.Cloud.DiscoveryToken = "YOUR_CLUSTER_DISCOVERY_TOKEN";
                options.Networking.Cloud.Url = new Uri("YOUR_DISCOVERY_URL");
            }

            // make sure the client stays connected
            options.Networking.ReconnectMode = ReconnectMode.ReconnectAsync;
            
            // enable metrics
            options.Metrics.Enabled = true;

            // set ssl
            options.Networking.Ssl.Enabled = true;
            options.Networking.Ssl.ValidateCertificateChain = false;
            options.Networking.Ssl.Protocol = SslProtocols.Tls12;
            options.Networking.Ssl.CertificatePath = "client.pfx";
            options.Networking.Ssl.CertificatePassword = "YOUR_SSL_PASSWORD";

            Console.WriteLine(" ok.");

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

            Console.WriteLine("Put/Get values in/from map with random values...");
            var random = new Random();
            var step = IterationCount / 10;
            for (var i = 0; i < IterationCount; i++)
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
