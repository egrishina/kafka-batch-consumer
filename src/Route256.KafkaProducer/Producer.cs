using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Route256.KafkaProducer;

public class Producer : IDisposable
{
    private const string KafkaHost = "localhost:9092";
    private const string Topic = "temperature";

    private readonly ILogger<Producer> _logger;
    private readonly IProducer<string, string> _producer;

    public Producer(ILogger<Producer> logger)
    {
        var producerBuilder = new ProducerBuilder<string, string>(new Dictionary<string, string>
        {
            ["bootstrap.servers"] = KafkaHost,
            ["enable.auto.commit"] = "true",
            ["enable.auto.offset.store"] = "true",
            ["fetch.message.max.bytes"] = "10240",
            ["fetch.min.bytes"] = "10000"
        });

        _producer = producerBuilder.Build();
        _logger = logger;
    }

    public void Dispose()
    {
        try
        {
            _producer.Flush(TimeSpan.FromSeconds(10.0));
            _logger.LogInformation("Kafka producer flushed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing producer");
        }

        _producer.Dispose();
    }

    public async Task StartProducing()
    {
        var cities = new[]
        {
            "moscow", "kazan", "samara", "krasnodar", "omsk", "ufa", "perm", "novosibirsk", "voronezh",
            "saint petersburg", "yekaterinburg", "volgograd", "kaliningrad", "tomsk"
        };

        var numProduced = 0;
        var rnd = new Random();
        for (int i = 0; i < 1000; i++)
        {
            var key = cities[rnd.Next(cities.Length)];
            var value = (rnd.NextDouble() * 40 - 10).ToString();

            var msg = new Message<string, string>
            {
                Key = key,
                Value = value
            };

            _producer.Produce(Topic, msg,
                deliveryReport =>
                {
                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                    {
                        _logger.LogWarning("Failed to deliver message: {@Reason}", deliveryReport.Error.Reason);
                    }
                    else
                    {
                        _logger.LogInformation("Produced event to topic {@Topic}: key = {@Key} value = {@Value}", Topic, key, value);
                        numProduced += 1;
                    }
                });
        }

        _producer.Flush();
        _logger.LogInformation("{@Number} messages were produced to topic {@Topic}", numProduced, Topic);
    }
}