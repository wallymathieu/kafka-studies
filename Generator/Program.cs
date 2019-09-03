using System;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
namespace Generator
{
    public class AppConfig
    {
        public string BrokerList { get; set; }
        public string TopicName { get; set; }
    }
    class Program
    {
        public static IConfigurationRoot Configuration { get; private set; }

        static async Task Main(string[] args)
        {
            Configure(args);
            var svcProvider = ServiceProvider();
            using (var producer = svcProvider.GetRequiredService<IProducer<string, string>>())
            {
                var logger = svcProvider
                                .GetRequiredService<ILoggerFactory>()
                                .CreateLogger<Program>();
                var config = svcProvider.GetRequiredService<IOptions<AppConfig>>().Value;
                Console.WriteLine("Enter message (or ctrl+c to exit)");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("> ");

                    string text;
                    try
                    {
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }

                    string key = null;
                    string val = text;

                    // split line if both key and value specified.
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                        val = text.Substring(index + 1);
                    }

                    try
                    {
                        // Note: Awaiting the asynchronous produce request below prevents flow of execution
                        // from proceeding until the acknowledgement from the broker is received (at the 
                        // expense of low throughput).
                        var deliveryReport = await producer.ProduceAsync(
                            config.TopicName, new Message<string, string> { Key = key, Value = val });

                        Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }

                // Since we are producing synchronously, at this point there will be no messages
                // in-flight and no delivery reports waiting to be acknowledged, so there is no
                // need to call producer.Flush before disposing the producer.
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
            services.AddSingleton(ConfigureProducer);
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

        static IProducer<string, string> ConfigureProducer(IServiceProvider provider)
        {
            var options = provider.GetRequiredService<IOptions<AppConfig>>().Value;
            var config = new ProducerConfig { BootstrapServers = options.BrokerList };

            return new ProducerBuilder<string, string>(config).Build();
        }
    }
}