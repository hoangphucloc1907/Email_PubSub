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
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Thêm timeout để tránh treo vô hạn
                SessionTimeoutMs = 10000, // 10 giây
                MaxPollIntervalMs = 300000 // 5 phút
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_config["Kafka:Topic"]);

            Console.WriteLine("Consumer đang lắng nghe...");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1)); // Timeout 1 giây
                        if (consumeResult?.Message != null)
                        {
                            string message = consumeResult.Message.Value;
                            Console.WriteLine($"Nhận được thông điệp: {message}");

                            var emailData = JsonSerializer.Deserialize<Email>(message);
                            if (emailData != null)
                            {
                                // Gửi email không chặn luồng chính
                                await Task.Run(() => SendEmail(emailData.To, emailData.Subject, emailData.Body), stoppingToken);
                            }
                        }
                        else
                        {
                            // Nghỉ ngắn để tránh CPU usage cao
                            await Task.Delay(100, stoppingToken);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Lỗi Kafka: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Dịch vụ bị hủy.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi tổng quát: {ex.Message}");
            }
            finally
            {
                consumer.Close();
            }
        }

        private void SendEmail(string to, string subject, string body)
        {
            using var smtpClient = new SmtpClient(_config["Smtp:Host"])
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
                smtpClient.Send(mailMessage); // Có thể thay bằng SendAsync để bất đồng bộ
                Console.WriteLine($"Đã gửi email tới {to}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Lỗi gửi email: {ex.Message}");
            }
        }
    }
}