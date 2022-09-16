using System;
using System.Security.Authentication;
using System.Text.Json;
using System.Threading.Tasks;
using Hazelcast;
using Hazelcast.Core;
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

            Console.WriteLine("Get and connect client...");
            await using var client = await HazelcastClientFactory.StartNewClientAsync(options);
            Console.WriteLine("Connection Sucessful!");

            await MapExample(client);

            // await SqlExample(client);

            // await JsonSerializationExample(client);

            // await NonStopMapExample(client);

            Console.WriteLine("Done.");
        }

        /// <summary>
        /// This example shows how to work with Hazelcast maps, where the map is updated continuously.
        /// </summary>
        private static async Task NonStopMapExample(IHazelcastClient client)
        {
            Console.WriteLine("Now the map named 'map' will be filled with random entries.");

            var map = await client.GetMapAsync<string, string>("map");
            var rnd = new Random();
            var iterator = 0;

            while (true)
            {
                var randomKey = rnd.Next(100_00);
                await map.PutAsync("key-" + randomKey, "value-" + randomKey);
                var _ = await map.GetAsync("key-" + rnd.Next(100_000));

                if (++iterator % 10 == 0)
                {
                    Console.WriteLine($"Current Map Size :{ await map.GetSizeAsync()}");
                }
            }
        }

        #region JsonExample
        private static async Task JsonSerializationExample(IHazelcastClient client)
        {
            await CreateMappingForContries(client);

            await PopulateCountriesWithMap(client);

            await SelectAllCountries(client);

            await CreateMappingForCities(client);

            await PopulateCities(client);

            await SelectCitiesByCountries(client, "AU");

            await SelectCountriesAndCities(client);
        }

        private static async Task SelectCountriesAndCities(IHazelcastClient client)
        {
            var query = "SELECT c.isoCode, c.country, t.city, t.population"
                + "  FROM country c"
                + "  JOIN city t ON c.isoCode = t.country";

            Console.WriteLine("Select country and city data in query that joins tables.");
            Console.WriteLine($"| {"ISO",-10} | {"Country",-20} | {"City",-20} | {"Population",15} |");

            await using var result = await client.Sql.ExecuteQueryAsync(query);

            await foreach (var item in result)
            {
                Console.WriteLine($"| {item.GetColumn<string>("isoCode"),-10} | {item.GetColumn<string>("country"),-20} | {item.GetColumn<string>("city"),-20} | {item.GetColumn<long>("population"),15} |");
            }

            Console.WriteLine("------------------------------------------");
        }

        private static async Task SelectCitiesByCountries(IHazelcastClient client, string country)
        {
            var query = "SELECT city, population FROM city where country=?";
            Console.WriteLine($"Select city and population with sql = {query}");

            await using var result = await client.Sql.ExecuteQueryAsync(query, new object[] { country });

            await foreach (var item in result)
            {
                Console.WriteLine($"City: {item.GetColumn<string>("city")}, Population: {item.GetColumn<long>("population")}");
            }
            Console.WriteLine("------------------------------------------");
        }

        private static async Task PopulateCities(IHazelcastClient client)
        {
            // see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
            Console.WriteLine("Populating 'city' map with JSON values...");

            var map = await client.GetMapAsync<string, HazelcastJsonValue>("city");

            await map.PutAsync("1", new CityDTO { City = "Canberra", Population = 467_194, Country = "AU" }.AsJson());
            await map.PutAsync("2", new CityDTO { City = "Prague", Population = 1_318_085, Country = "CZ" }.AsJson());
            await map.PutAsync("3", new CityDTO { City = "London", Population = 9_540_576, Country = "EN" }.AsJson());
            await map.PutAsync("4", new CityDTO { City = "Washington, DC", Population = 7_887_965, Country = "US" }.AsJson());

            Console.WriteLine("The 'city' map has been populated.");
            Console.WriteLine("------------------------------------------");
        }

        private static async Task CreateMappingForCities(IHazelcastClient client)
        {
            //see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
            Console.WriteLine("Creating mapping for cities...");

            string mappingSql = "CREATE OR REPLACE MAPPING city("
                 + " __key INT ,"
                 + " country VARCHAR ,"
                 + " city VARCHAR,"
                 + " population BIGINT)"
                 + " TYPE IMap"
                 + " OPTIONS ("
                 + "     'keyFormat' = 'int',"
                 + "     'valueFormat' = 'json-flat'"
                 + " )";

            if (await client.Sql.ExecuteCommandAsync(mappingSql) > 0)
                Console.WriteLine("The mapping has been created successfully.");

            Console.WriteLine("------------------------------------------");
        }

        private static async Task SelectAllCountries(IHazelcastClient client)
        {
            var query = "SELECT c.country from country c";
            Console.WriteLine($"Select all countries with sql = {query}");

            await using var result = await client.Sql.ExecuteQueryAsync(query);

            await foreach (var item in result)
            {
                Console.WriteLine($"Country: {item.GetColumn<object>("country")}");
            }
            Console.WriteLine("------------------------------------------");
        }

        private static async Task PopulateCountriesWithMap(IHazelcastClient client)
        {
            Console.WriteLine("Populating 'country' map with JSON values...");
            // see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map

            var map = await client.GetMapAsync<string, HazelcastJsonValue>("country");
            await map.PutAsync("AU", new CountryDTO { ISOCode = "AU", Country = "Australia" }.AsJson());
            await map.PutAsync("EN", new CountryDTO { ISOCode = "EN", Country = "England" }.AsJson());
            await map.PutAsync("US", new CountryDTO { ISOCode = "US", Country = "United States" }.AsJson());
            await map.PutAsync("CZ", new CountryDTO { ISOCode = "CZ", Country = "Czech Republic" }.AsJson());

            Console.WriteLine("The 'countries' map has been populated.");
            Console.WriteLine("------------------------------------------");
        }

        private static async Task CreateMappingForContries(IHazelcastClient client)
        {
            Console.WriteLine("Creating a mapping for 'country'...");
            // See: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps

            var mappingString = "CREATE OR REPLACE MAPPING country("
                                + "     __key VARCHAR,"
                                + "     isoCode VARCHAR,"
                                + "     country VARCHAR"
                                + ") TYPE IMap"
                                + " OPTIONS ("
                                + "     'keyFormat' = 'varchar',"
                                + "     'valueFormat' = 'json-flat'"
                                + " )";

            if (await client.Sql.ExecuteCommandAsync(mappingString) > 0)
                Console.WriteLine("Mapping for countries has been created.");

            Console.WriteLine("------------------------------------------");
        }
        #endregion

        #region SqlExample
        private static async Task SqlExample(IHazelcastClient client)
        {
            await CreateMappingForCapitals(client);

            await ClearCapitals(client);

            await PopulateCapitals(client);

            await SelectAllCapitals(client);

            await SelectAllCapitalNames(client);

        }

        private static async Task SelectAllCapitalNames(IHazelcastClient client)
        {
            Console.WriteLine("Retrieving the capital name via SQL...");

            await using var result = await client.Sql.ExecuteQueryAsync("SELECT * FROM capitals WHERE __key=?", new object[] { "United States" });

            await foreach (var item in result)
            {
                var country = item.GetColumn<string>(0);
                var city = item.GetColumn<string>(1);

                Console.WriteLine($"Country: {country}; Capital: {city}");
            }
        }

        private static async Task SelectAllCapitals(IHazelcastClient client)
        {
            Console.WriteLine("Retrieving all the data via SQL...");

            await using var result = await client.Sql.ExecuteQueryAsync("SELECT * FROM capitals");

            await foreach (var item in result)
            {
                var country = item.GetColumn<string>(0);
                var city = item.GetColumn<string>(1);

                Console.WriteLine($"Country:{country}; City:{city}");
            }

            Console.WriteLine("------------------------------------------");
        }

        private static async Task PopulateCapitals(IHazelcastClient client)
        {
            Console.WriteLine("Inserting data via SQL...");

            var insertQuery = "INSERT INTO capitals VALUES"
                             + "('Australia','Canberra'),"
                             + "('Croatia','Zagreb'),"
                             + "('Czech Republic','Prague'),"
                             + "('England','London'),"
                             + "('Turkey','Ankara'),"
                             + "('United States','Washington, DC');";

            if (await client.Sql.ExecuteCommandAsync(insertQuery) > 0)
                Console.WriteLine("The data has been inserted successfully.");

            Console.WriteLine("------------------------------------------");
        }

        private static async Task ClearCapitals(IHazelcastClient client)
        {
            Console.WriteLine("Deleting data via SQL...");

            if (await client.Sql.ExecuteCommandAsync("DELETE FROM capitals") > 0)
                Console.WriteLine("The data has been deleted successfully.");

            Console.WriteLine("------------------------------------------");
        }

        private static async Task CreateMappingForCapitals(IHazelcastClient client)
        {
            Console.WriteLine("Creating a mapping...");
            // See: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps

            var mappingString = "CREATE OR REPLACE MAPPING capitals TYPE IMap"
                                + " OPTIONS ("
                                + "   'keyFormat' = 'varchar',"
                                + "   'valueFormat' = 'varchar'"
                                + ")";

            if (await client.Sql.ExecuteCommandAsync(mappingString) > 0)
                Console.WriteLine("The mapping has been created successfully.");

            Console.WriteLine("------------------------------------------");
        }
        #endregion

        public static async Task MapExample(IHazelcastClient client)
        {
            var map = await client.GetMapAsync<string, HazelcastJsonValue>("cities");

            await map.PutAsync("1", new CityDTO { City = "London", Population = 9_540_576, Country = "United Kingdom" }.AsJson());
            await map.PutAsync("2", new CityDTO { City = "Manchester", Population = 2_770_434, Country = "United Kingdom" }.AsJson());
            await map.PutAsync("3", new CityDTO { City = "New York", Population = 19_223_191, Country = "United States" }.AsJson());
            await map.PutAsync("4", new CityDTO { City = "Los Angeles", Population = 3_985_520, Country = "United States" }.AsJson());
            await map.PutAsync("5", new CityDTO { City = "Ankara", Population = 5_309_690, Country = "Turkey" }.AsJson());
            await map.PutAsync("6", new CityDTO { City = "Istanbul", Population = 15_636_243, Country = "Turkey" }.AsJson());
            await map.PutAsync("7", new CityDTO { City = "Sao Paulo", Population = 22_429_800, Country = "Brazil" }.AsJson());
            await map.PutAsync("8", new CityDTO { City = "Rio de Janeiro", Population = 13_634_274, Country = "Brazil" }.AsJson());

            var size = await map.GetSizeAsync();

            Console.WriteLine($"'cities' contains {size} entries.");
            Console.WriteLine("----------------------------------");
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
