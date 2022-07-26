using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Hazelcast.Core;

namespace ClientWithSsl
{
    internal static class SerializationExtensions
    {
        public static HazelcastJsonValue AsJson(this CityDTO city)
        {
            return new HazelcastJsonValue(JsonSerializer.Serialize(city));
        }

        public static HazelcastJsonValue AsJson(this CountryDTO country)
        {
            return new HazelcastJsonValue(JsonSerializer.Serialize(country));
        }
    }
}
