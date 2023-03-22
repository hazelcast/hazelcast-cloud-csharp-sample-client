using System;
using System.Security.Authentication;
using System.Threading.Tasks;
using Hazelcast;
using Microsoft.Extensions.Logging;

namespace ClientWithSsl
{
    /*
     * A sample application that configures a client to connect to an Hazelcast Viridian cluster
     * over TLS, and to then insert and fetch data with SQL, thus testing that the connection to 
     * the Hazelcast Viridian cluster is successful.
     * 
     * Hazelcast .Net Client: https://hazelcast.github.io/hazelcast-csharp-client/
     */
    internal static class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Connect Hazelcast Viridian with TLS");

            Console.Write("Build options...");

            var options = new HazelcastOptionsBuilder()
                .WithConsoleLogger()
                // Log level must be a valid Microsoft.Extensions.Logging.LogLevel value.
                // Trace | Debug | Information | Warning | Error | Critical | None
                .With("Logging:LogLevel:Hazelcast", "Information")
                .Build();

            // Your Viridian cluster name.
            options.ClusterName = "YOUR_CLUSTER_NAME";

            // Your discovery token to connect Viridian cluster.
            options.Networking.Cloud.DiscoveryToken = "YOUR_CLUSTER_DISCOVERY_TOKEN";

            // Enable metrics to see on Management Center.
            options.Metrics.Enabled = true;

            // Configure SSL
            options.Networking.Ssl.Enabled = true;
            options.Networking.Ssl.ValidateCertificateChain = false;
            options.Networking.Ssl.Protocol = SslProtocols.Tls12;
            options.Networking.Ssl.CertificatePath = "client.pfx";
            options.Networking.Ssl.CertificatePassword = "YOUR_SSL_PASSWORD";

            Console.WriteLine("Get and connect client...");
            await using var client = await HazelcastClientFactory.StartNewClientAsync(options);
            Console.WriteLine("Connection Sucessful!");

            // Arrange data with SQL.
            await CreateMapping(client);
            await AddUsersWithSQL(client);
            await FetchUsersWithSQL(client);

            Console.WriteLine("Done.");
        }

        private static async Task CreateMapping(IHazelcastClient client)
        {
            // Mapping is required for your distributed map to be queried over SQL.
            // See: https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps

            Console.Write("\nCreating the mapping...");

            var mappingCommand = @"CREATE OR REPLACE MAPPING 
                                    users (
                                        __key INT,
                                        name VARCHAR,
                                        country VARCHAR
                                    ) TYPE IMAP
                                    OPTIONS ( 'keyFormat' = 'int',
                                              'valueFormat' = 'json-flat' )";

            await client.Sql.ExecuteCommandAsync(mappingCommand);

            Console.Write("OK.");
        }

        private static async Task AddUsersWithSQL(IHazelcastClient client)
        {
            Console.Write("\nInserting users into 'users' map via SQL...");

            var insertQuery = @"INSERT INTO users 
                                (__key, name, country) VALUES
                                (1, 'Emre', 'Türkiye'),
                                (2, 'Aika', 'Japan'),
                                (3, 'John', 'United States'),
                                (4, 'Olivia', 'United Kingdom'),
                                (5, 'Jonas', 'Germany')";

            await client.Sql.ExecuteCommandAsync(insertQuery);

            Console.Write("OK.");
        }

        private static async Task FetchUsersWithSQL(IHazelcastClient client)
        {
            Console.WriteLine("\nFetching users via SQL...");

            await using var result = await client.Sql.ExecuteQueryAsync("SELECT * FROM users");

            Console.WriteLine("--Results of 'SELECT * FROM users'");

            await foreach (var row in result)
            {
                Console.WriteLine($"Id:{row.GetKey<int>()}\tName:{row.GetColumn<string>("name")}\tCountry:{row.GetColumn<string>("country")}");
            }

            Console.WriteLine("\n!! Hint !! You can execute your SQL queries on your Viridian cluster over the management center. Go to 'Management Center' of your Hazelcast Viridian cluster, then open the 'SQL Browser' and try to execute 'SELECT * FROM users'.\n");
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
