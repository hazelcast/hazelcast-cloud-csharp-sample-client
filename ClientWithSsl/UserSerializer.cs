using Hazelcast.Serialization.Compact;

namespace ClientWithSsl
{
    internal class UserSerializer : ICompactSerializer<User>
    {
        public string TypeName => "user";

        public User Read(ICompactReader reader)
        {
            return new User()
            {
                Name = reader.ReadString("name"),
                Country = reader.ReadString("country")
            };
        }

        public void Write(ICompactWriter writer, User value)
        {
            writer.WriteString("name", value.Name);
            writer.WriteString("country", value.Country);
        }
    }
}
