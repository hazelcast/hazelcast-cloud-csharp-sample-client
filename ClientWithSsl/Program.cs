// Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
     * See: https://docs.hazelcast.com/cloud/get-started
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

                    // Register Compact serializer of City class.
                    config.Serialization.Compact.AddSerializer(new CitySerializer());
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
            await PopulateCities(client);
            await FetchCitiesWithSQL(client);

            Console.WriteLine("Done.");
        }

        private static async Task CreateMapping(IHazelcastClient client)
        {
            // Mapping is required for your distributed map to be queried over SQL.
            // See: https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps

            Console.Write("\nCreating the mapping...");

            var mappingCommand = @"CREATE OR REPLACE MAPPING 
                                    cities (
                                        __key INT,                                        
                                        country VARCHAR,
                                        city VARCHAR,
                                        population INT) TYPE IMAP
                                    OPTIONS ( 
                                        'keyFormat' = 'int',
                                        'valueFormat' = 'compact',
                                        'valueCompactTypeName' = 'city')";

            await client.Sql.ExecuteCommandAsync(mappingCommand);

            Console.Write("OK.");
        }

        private static async Task PopulateCities(IHazelcastClient client)
        {
            Console.Write("\nInserting cities into 'cities' map...");

            var insertQuery = @"INSERT INTO cities 
                                (__key, city, country, population) VALUES
                                (1, 'London', 'United Kingdom', 9540576),
                                (2, 'Manchester', 'United Kingdom', 2770434),
                                (3, 'New York', 'United States', 19223191),
                                (4, 'Los Angeles', 'United States', 3985520),
                                (5, 'Istanbul', 'Türkiye', 15636243),
                                (6, 'Ankara', 'Türkiye', 5309690),
                                (7, 'Sao Paulo ', 'Brazil', 22429800)";

            try
            {
                await client.Sql.ExecuteCommandAsync(insertQuery);
            }
            catch (Exception ex)
            {
                Console.WriteLine("FAILED. "+ex.ToString());
            }

            Console.Write("\nPutting a city into 'cities' map...");
            // Let's also add a city as object.
            var map = await client.GetMapAsync<int, CityDTO>("cities");
            await map.PutAsync(8, new CityDTO { City = "Rio de Janeiro", Country = "Brazil", Population = 13634274 });

            Console.Write("OK.");
        }

        private static async Task FetchCitiesWithSQL(IHazelcastClient client)
        {
            Console.Write("\nFetching cities via SQL...");

            await using var result = await client.Sql.ExecuteQueryAsync("SELECT __key, this FROM cities");
            Console.Write("OK.");
            Console.WriteLine("\n--Results of 'SELECT __key, this FROM cities'");
            Console.WriteLine(String.Format("| {0,4} | {1,20} | {2,20} | {3,15} |","id", "country", "city", "population"));

            await foreach (var row in result)
            {
                var id = row.GetKey<int>(); // Corresponds to '__key'
                var c = row.GetValue<CityDTO>(); // Corresponds to 'this'

                Console.WriteLine(string.Format("| {0,4} | {1,20} | {2,20} | {3,15} |",
                                    id,
                                    c.Country,
                                    c.City,
                                    c.Population));
            }

            Console.WriteLine("\n!! Hint !! You can execute your SQL queries on your Viridian cluster over the management center. \n 1. Go to 'Management Center' of your Hazelcast Viridian cluster. \n 2. Open the 'SQL Browser'. \n 3. Try to execute 'SELECT * FROM cities'.\n");
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
