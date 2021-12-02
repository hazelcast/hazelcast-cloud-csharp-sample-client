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

            Console.WriteLine("Get and connect client...");
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

            // the 'MapExample' is an example with an infinite loop inside, so if you'd like to try other examples,
            // don't forget to comment out the following line
            await MapExample(map);

            // await SqlExample(client);

            Console.WriteLine("Done.");
        }

        public static async Task MapExample(IHMap<string, string> map)
        {
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
        }

        public static async Task SqlExample(IHazelcastClient hzClient)
        {
            Console.WriteLine("Creating a mapping...");
            // See: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps
            var mappingQuery = "CREATE OR REPLACE MAPPING cities TYPE IMap OPTIONS " +
                "('keyFormat'='varchar','valueFormat'='varchar')";
            await hzClient.Sql.ExecuteQueryAsync(mappingQuery);
            Console.WriteLine("The mapping has been created successfully.");
            Console.WriteLine("--------------------");

            Console.WriteLine("Deleting data via SQL...");
            var deleteQuery = "DELETE FROM cities";
            await hzClient.Sql.ExecuteQueryAsync(deleteQuery);
            Console.WriteLine("The data has been deleted successfully.");
            Console.WriteLine("--------------------");

            Console.WriteLine("Inserting data via SQL...");
            var insertQuery = "INSERT INTO cities VALUES " +
                "('Australia','Canberra')," +
                "('Croatia','Zagreb')," +
                "('Czech Republic','Prague')," +
                "('England','London')," +
                "('Turkey','Ankara')," +
                "('United States','Washington, DC');";
            await hzClient.Sql.ExecuteQueryAsync(insertQuery);
            Console.WriteLine("The data has been inserted successfully.");
            Console.WriteLine("--------------------");

            Console.WriteLine("Retrieving all the data via SQL...");
            var sqlResultAll = await hzClient.Sql.ExecuteQueryAsync("SELECT * FROM cities");
            await foreach (var row in sqlResultAll)
            {
                var country = row.GetKey<string>();
                var city = row.GetValue<string>();
                Console.WriteLine("%s - %s", country, city);
            }
            Console.WriteLine("--------------------");

            Console.WriteLine("Retrieving a city name via SQL...");
            var sqlResultRecord = await hzClient.Sql
                .ExecuteQueryAsync("SELECT __key AS country, this AS city FROM cities WHERE __key = ?",
                new[]{"United States"});
            await foreach (var row in sqlResultRecord)
            {
                var country = row.GetColumn<string>("country");
                var city = row.GetColumn<string>("city");
                Console.WriteLine("Country name: %s; City name: %s", country, city);
            }
            Console.WriteLine("--------------------");
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
