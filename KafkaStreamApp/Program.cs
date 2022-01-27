using Confluent.Kafka;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaStreamApp
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, collection) => {

                    collection.AddHostedService<KafkaProducerHostedService>()
                              .AddHostedService<KafkaConsumerHostedService>();
                    
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });

    }

    public class KafkaProducerHostedService : IHostedService
    {
        private const string topic = "quickstart-events";
        private const string broker = "localhost:9092";

        private readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;
        
        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger) {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = broker
            };

            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {

            for (var i = 0; i < 100; i++) {
                var value = $"Hello world{i}";
                _logger.LogInformation(value);

                await _producer.ProduceAsync(topic, new Message<Null, string>()
                {
                    Value = value
                }, cancellationToken);
            }

            _producer.Flush(TimeSpan.FromSeconds(10));
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();

            return Task.CompletedTask;
        }
    }

    public class KafkaConsumerHostedService : IHostedService
    {
        private const string topic = "quickstart-events";
        private const string broker = "localhost:9092";

        private readonly ILogger<KafkaConsumerHostedService> _logger;        
        private IConsumer<string, string> _consumer;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            _logger = logger;
            var config = new ConsumerConfig()
            {
                BootstrapServers = broker,
                GroupId = "kafka-dotnet-getting-started",
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            _consumer = new ConsumerBuilder<string, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {

            Task.Run(() =>
            {
                _consumer.Subscribe(topic);
                
                try
                {

                    while (true)
                    {
                         var cr = _consumer.Consume(cancellationToken);
                        _logger.LogInformation($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value}");
                    }

                }
                catch (OperationCanceledException)
                {
                    // Ctrl-C was pressed.
                }
                finally
                {
                    _consumer.Close();
                }
            }, cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _consumer.Close();
            _consumer.Dispose();

            return Task.CompletedTask;
        }
    }
}
