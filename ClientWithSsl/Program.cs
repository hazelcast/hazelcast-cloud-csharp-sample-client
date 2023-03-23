using System;
using System.IO;
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
                .With(config =>
                {
                    // Your Viridian cluster name.
                    config.ClusterName = "YOUR_CLUSTER_NAME";
                    // Your discovery token to connect Viridian cluster.
                    config.Networking.Cloud.DiscoveryToken = "YOUR_CLUSTER_DISCOVERY_TOKEN";
                    // Enable metrics to see on Management Center.
                    config.Metrics.Enabled = true;
                    // Configure SSL.
                    config.Networking.Ssl.Enabled = true;
                    config.Networking.Ssl.ValidateCertificateChain = false;
                    config.Networking.Ssl.Protocol = SslProtocols.Tls12;
                    config.Networking.Ssl.CertificatePath = "client.pfx";
                    config.Networking.Ssl.CertificatePassword = "YOUR_SSL_PASSWORD";

                    // Register Compact serializer of User class.
                    config.Serialization.Compact.AddSerializer(new UserSerializer());
                })
                .With(args)
                // Log level must be a valid Microsoft.Extensions.Logging.LogLevel value.
                // Trace | Debug | Information | Warning | Error | Critical | None
                .With("Logging:LogLevel:Hazelcast", "Information")
                .Build();

            Console.WriteLine("Get and connect client...");
            await using var client = await HazelcastClientFactory.StartNewClientAsync(options);
            Console.WriteLine("Connection Sucessful!");

            // Arrange data.
            await CreateMapping(client);
            await AddUsers(client);
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
                                        country VARCHAR) TYPE IMAP
                                    OPTIONS ( 
                                        'keyFormat' = 'int',
                                        'valueFormat' = 'compact',
                                        'valueCompactTypeName' = 'user')";

            await client.Sql.ExecuteCommandAsync(mappingCommand);

            Console.Write("OK.");
        }

        private static async Task AddUsers(IHazelcastClient client)
        {
            Console.Write("\nInserting users into 'users' map...");

            var insertQuery = @"INSERT INTO users 
                                (__key, name, country) VALUES
                                (1, 'Emre', 'Türkiye'),
                                (2, 'Aika', 'Japan'),
                                (3, 'John', 'United States'),
                                (4, 'Olivia', 'United Kingdom'),
                                (5, 'Jonas', 'Germany')";

            try
            {
                await client.Sql.ExecuteCommandAsync(insertQuery);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            // Let's also add a user as object.
            var map = await client.GetMapAsync<int, User>("users");
            await map.PutAsync(6, new User { Name = "Alice", Country = "Brazil" });

            Console.Write("OK.");
        }

        private static async Task FetchUsersWithSQL(IHazelcastClient client)
        {
            Console.WriteLine("\nFetching users via SQL...");

            await using var result = await client.Sql.ExecuteQueryAsync("SELECT __key, this FROM users");

            Console.WriteLine("--Results of 'SELECT __key, this FROM users'");

            await foreach (var row in result)
            {
                var id = row.GetKey<int>(); // Corresponds to '__key'
                var user = row.GetValue<User>(); // Corresponds to 'this'

                Console.WriteLine($"Id:{id}\tName:{user.Name}\tCountry:{user.Country}");
            }

            Console.WriteLine("\n!! Hint !! You can execute your SQL queries on your Viridian cluster over the management center. \n 1. Go to 'Management Center' of your Hazelcast Viridian cluster. \n 2. Open the 'SQL Browser'. \n 3. Try to execute 'SELECT * FROM users'.\n");
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
