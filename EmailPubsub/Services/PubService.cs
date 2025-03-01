using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using System.Threading.Tasks;
using EmailPubsub.Models;
namespace EmailPubsub.Services
{
    public class PubService
    {
        private readonly IConfiguration _config;
        private readonly ProducerConfig _producerConfig;
        private readonly string _topic;

        public PubService(IConfiguration config)
        {
            _config = config;
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = _config["Kafka:SaslUsername"],
                SaslPassword = _config["Kafka:SaslPassword"]
            };
            _topic = _config["Kafka:Topic"];
        }

        public async Task SendEmailMessage(Email emailRequest)
        {
            using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
            string message = JsonSerializer.Serialize(emailRequest);

            try
            {
                var result = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
                Console.WriteLine($"Đã gửi thông điệp tới {_topic}: {result.TopicPartitionOffset}");
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"Lỗi khi gửi: {ex.Error.Reason}");
                throw;
            }

            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
