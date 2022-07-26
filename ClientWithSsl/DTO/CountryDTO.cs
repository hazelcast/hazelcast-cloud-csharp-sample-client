using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace ClientWithSsl
{
    internal class CountryDTO
    {
        [JsonPropertyName("isoCode")]
        public string ISOCode { get; set; }
        [JsonPropertyName("country")]
        public string Country { get; set; }
    }
}
