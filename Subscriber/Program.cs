using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Subscriber
{
    public class AppConfig
    {
        public string BrokerList { get; set; }
        public List<string> Topics { get; set; }
    }
    class Program
    {
        public static IConfigurationRoot Configuration { get; private set; }

        static void Main(string[] args)
        {
            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            Configure(args);
            var svcProvider = ServiceProvider();
            using (var consumer = svcProvider.GetRequiredService<IConsumer<Ignore, string>>())
            {
                var logger = svcProvider
                                .GetRequiredService<ILoggerFactory>()
                                .CreateLogger<Program>();
                var config = svcProvider.GetRequiredService<IOptions<AppConfig>>().Value;
                consumer.Assign(config.Topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        // Note: End of partition notification has not been enabled, so
                        // it is guaranteed that the ConsumeResult instance corresponds
                        // to a Message, and not a PartitionEOF event.
                        Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "'while' exception");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        private static ServiceProvider ServiceProvider()
        {
            var services = new ServiceCollection();

            services.AddLogging(logging =>
            {
                logging.AddConfiguration(Configuration.GetSection("Logging"));
                logging.AddConsole();
            });
            services.Configure<AppConfig>(Configuration.GetSection("AppConfig"));
            services.AddSingleton(ConfigureConsumer);
            var svcProvider = services.BuildServiceProvider();
            return svcProvider;
        }

        private static void Configure(string[] args)
        {
            var config = new ConfigurationBuilder();
            config.AddJsonFile("appsettings.json", true);
            config.AddEnvironmentVariables();

            if (args != null)
                config.AddCommandLine(args);
            Configuration = config.Build();
        }

        static IConsumer<Ignore, string> ConfigureConsumer(IServiceProvider provider)
        {
            var options = provider.GetRequiredService<IOptions<AppConfig>>().Value;
            var config = new ConsumerConfig
            {
                BootstrapServers = options.BrokerList,
                GroupId = "consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };
            var logging = provider.GetRequiredService<ILoggerFactory>().CreateLogger<IConsumer<Ignore, string>>();

            // Note: If a key or value deserializer is not set (as is the case below), the 
            // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
            // will be used automatically (where available). The default deserializer for string
            // is UTF8. The default deserializer for Ignore returns null for all input data
            // (including non-null data).
            return new ConsumerBuilder<Ignore, string>(config)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => logging.LogError($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => logging.LogInformation($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    logging.LogInformation($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    // possibly manually specify start offsets or override the partition assignment provided by
                    // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                    // 
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    logging.LogInformation($"Revoking assignment: [{string.Join(", ", partitions)}]");
                })
                .Build();
        }
    }

}
