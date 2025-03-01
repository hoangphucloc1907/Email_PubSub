using Confluent.Kafka;
using EmailPubsub.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Net.Mail;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
namespace EmailPubsub.Services
{
    public class SubService : BackgroundService
    {
        private readonly IConfiguration _config;

        public SubService(IConfiguration config)
        {
            _config = config;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = _config["Kafka:SaslUsername"],
                SaslPassword = _config["Kafka:SaslPassword"],
                GroupId = "email_consumer_group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_config["Kafka:Topic"]);

            Console.WriteLine("Consumer đang lắng nghe...");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(stoppingToken);
                    if (consumeResult?.Message != null)
                    {
                        string message = consumeResult.Message.Value;
                        Console.WriteLine($"Nhận được thông điệp: {message}");

                        var emailData = JsonSerializer.Deserialize<Email>(message);
                        SendEmail(emailData.To, emailData.Subject, emailData.Body);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi: {ex.Message}");
            }
        }

        private void SendEmail(string to, string subject, string body)
        {
            var smtpClient = new SmtpClient(_config["Smtp:Host"])
            {
                Port = int.Parse(_config["Smtp:Port"]),
                Credentials = new System.Net.NetworkCredential(_config["Smtp:Username"], _config["Smtp:Password"]),
                EnableSsl = true,
            };

            var mailMessage = new MailMessage
            {
                From = new MailAddress(_config["Smtp:Username"]),
                Subject = subject,
                Body = body,
                IsBodyHtml = false,
            };
            mailMessage.To.Add(to);

            try
            {
                smtpClient.Send(mailMessage);
                Console.WriteLine($"Đã gửi email tới {to}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi gửi email: {ex.Message}");
            }
        }
    }
}
