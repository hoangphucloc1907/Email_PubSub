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
                SessionTimeoutMs = 10000, // 10 seconds
                MaxPollIntervalMs = 300000 // 5 minutes
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_config["Kafka:Topic"]);

            Console.WriteLine("Consumer is listening...");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1)); // Use stoppingToken for cancellation
                        if (consumeResult?.Message != null)
                        {
                            string message = consumeResult.Message.Value;
                            Console.WriteLine($"Received message: {message}");

                            var emailData = JsonSerializer.Deserialize<Email>(message);
                            if (emailData != null)
                            {
                                // Send email asynchronously
                                _ = SendEmailAsync(emailData.To, emailData.Subject, emailData.Body, stoppingToken);
                            }
                        }
                        else
                        {
                            // Short delay to avoid high CPU usage
                            await Task.Delay(100, stoppingToken);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Kafka error: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Service canceled.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"General error: {ex.Message}");
            }
            finally
            {
                consumer.Close();
            }
        }

        private async Task SendEmailAsync(string to, string subject, string body, CancellationToken cancellationToken)
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
                await smtpClient.SendMailAsync(mailMessage, cancellationToken); 
                Console.WriteLine($"Email sent to {to}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Email sending error: {ex.Message}");
            }
        }
    }
}