using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace ClientWithSsl
{
    internal class CityDTO
    {
        [JsonPropertyName("country")]
        public string Country { get; set; }
        [JsonPropertyName("city")]
        public string City { get; set; }
        [JsonPropertyName("population")]
        public long Population { get; set; }
    }
}
