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

using Hazelcast.Serialization.Compact;

namespace ClientWithSsl
{
    internal class CitySerializer : ICompactSerializer<CityDTO>
    {
        public string TypeName => "city";

        public CityDTO Read(ICompactReader reader)
        {
            return new CityDTO()
            {
                City = reader.ReadString("city"),
                Country = reader.ReadString("country"),
                Population = reader.ReadInt32("population")
            };
        }

        public void Write(ICompactWriter writer, CityDTO value)
        {
            writer.WriteString("city", value.City);
            writer.WriteString("country", value.Country);
            writer.WriteInt32("population", value.Population);
        }
    }
}
