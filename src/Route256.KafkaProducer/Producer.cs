using Confluent.Kafka;

namespace Route256.KafkaProducer;

public class Producer
{
    public const string KafkaHost = "localhost:9092";
    public const string Topic = "temperature";

    public async Task StartProducer()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = KafkaHost
        };

        var cities = new[]
        {
            "moscow", "kazan", "samara", "krasnodar", "omsk", "ufa", "perm", "novosibirsk", "voronezh",
            "saint petersburg", "yekaterinburg", "volgograd", "kaliningrad", "tomsk"
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
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

            producer.Produce(Topic, msg,
                deliveryReport =>
                {
                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                    {
                        Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                    }
                    else
                    {
                        Console.WriteLine($"Produced event to topic {Topic}: key = {key,-20} value = {value}");
                        numProduced += 1;
                    }
                });
        }

        producer.Flush();
        Console.WriteLine($"{numProduced} messages were produced to topic {Topic}");
    }
}