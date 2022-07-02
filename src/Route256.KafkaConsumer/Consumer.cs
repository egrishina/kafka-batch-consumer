using Confluent.Kafka;

namespace Route256.KafkaConsumer;

public class Consumer
{
    private const string KafkaHost = "localhost:9092";
    private const string Topic = "temperature";
    private const int BatchSize = 10;

    private readonly List<Action<IConsumer<string, string>, Error>> _errorsHandlers = new();

    public Consumer()
    {
        RegisterLogErrorHandler();
    }

    public void OnError(Action<IConsumer<string, string>, Error> handler) => _errorsHandlers.Add(handler);

    public async Task StartConsumer()
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaHost,
            GroupId = "group_1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        await ConsumeMessages(consumerConfig, cts.Token);
    }

    private async Task ConsumeMessages(ConsumerConfig config, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((cons, error) => _errorsHandlers.ForEach(x => x(cons, error)))
            .Build();
        consumer.Subscribe(Topic);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                while (true)
                {
                    var cr = ConsumeBatch(consumer, TimeSpan.FromMilliseconds(100), BatchSize);
                    ProcessMessages(cr.Select(x => x.Message).ToArray());
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Consumer Error: {e.Message}");
            }
            finally
            {
                consumer.Close();
            }
        }
    }

    private IReadOnlyCollection<ConsumeResult<TKey, TValue>> ConsumeBatch<TKey, TValue>(
        IConsumer<TKey, TValue> consumer, TimeSpan timeout, int batchSize)
    {
        int latestPartition = -1;
        var messageBatch = new List<ConsumeResult<TKey, TValue>>();
        while (messageBatch.Count < batchSize)
        {
            var message = consumer.Consume(timeout);
            if (message?.Message is null)
                break;

            if (latestPartition != -1 && message.Partition.Value != latestPartition)
            {
                consumer.Commit(messageBatch.Last());
            }

            Console.WriteLine(
                $"{message.Topic}:{message.Partition}:{message.Offset} => Consumed event with " +
                $"key = {message.Message.Key,-20} value = {message.Message.Value}");
            messageBatch.Add(message);
            latestPartition = message.Partition.Value;
        }

        if (messageBatch.Any())
        {
            consumer.Commit(messageBatch.Last());
        }

        return messageBatch;
    }

    private void ProcessMessages<TKey, TValue>(IReadOnlyCollection<Message<TKey, TValue>> messages)
    {
        if (messages.Any())
        {
            Console.WriteLine("- - - - - Processing batch messages - - - - -");
        }
        else
        {
            Console.WriteLine("- - - - - Waiting for new messages - - - - -");
        }
    }

    private void RegisterLogErrorHandler()
    {
        OnError((_, error) =>
        {
            if (error.IsFatal)
            {
                Console.WriteLine($"Kafka Consumer Internal Error: Code {error.Code}, Reason {error.Reason}");
            }
            else
            {
                Console.WriteLine($"Kafka Consumer Internal Warning: Code {error.Code}, Reason {error.Reason}");
            }
        });
    }
}