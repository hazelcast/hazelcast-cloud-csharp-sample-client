using System;
using System.Threading;
using Hazelcast.Client;
using Hazelcast.Config;

/**
 *
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * After successful connection, it puts random entries into the map.
 *
 * See: https://docs.hazelcast.cloud/docs/csharp-client
 *
 */
namespace Client
{
    class Program
    {
        public static void Main(string[] args)
        {
            Environment.SetEnvironmentVariable("hazelcast.client.statistics.enabled", "true");
            Environment.SetEnvironmentVariable("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
            var config = new ClientConfig();
            config.GetGroupConfig()
                .SetName("YOUR_CLUSTER_NAME")
                .SetPassword("YOUR_CLUSTER_PASSWORD");

            config.GetNetworkConfig().GetCloudConfig()
                .SetEnabled(true)
                .SetDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN");

            config.GetNetworkConfig().GetSSLConfig()
                .SetEnabled(true)
                .SetProperty(SSLConfig.ValidateCertificateChain, "false")
                .SetProperty(SSLConfig.CertificateFilePath, "client.pfx")
                .SetProperty(SSLConfig.CertificatePassword, "YOUR_SSL_PASSWORD");

            var client = HazelcastClient.NewHazelcastClient(config);
            var map = client.GetMap<string, string>("map");
            map.Put("key", "value");
            if(map.Get("key").Equals("value"))
            {
                Console.WriteLine("Connection Successful!");
                Console.WriteLine("Now, `map` will be filled with random entries.");
            }
            else {
                throw new Exception("Connection failed, check your configuration.");
            }
            var random = new Random();
            while (true) {
                var randomKey = random.Next(100_000);
                map.Put("key" + randomKey, "value" + randomKey);
                map.Get("key" + random.Next(100_000));
                if(randomKey % 10 == 0 )
                {
                    Console.WriteLine("map size: {0}", map.Size());
                }
            }
        }
    }
}
