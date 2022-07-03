using Confluent.Kafka;
using Route256.Redis;

namespace Route256.KafkaConsumer;

public class Consumer
{
    private const string KafkaHost = "localhost:9092";
    private const string Topic = "temperature";
    private const string ConsumerGroupId = "group_1";
    private const int BatchSize = 10;

    private readonly RedisCache _redis;
    private readonly List<Action<IConsumer<string, string>, Error>> _errorsHandlers = new();

    public Consumer()
    {
        RegisterLogErrorHandler();
        _redis = new RedisCache();
    }

    public async Task StartConsuming()
    {
        await _redis.ConnectToDatabase();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaHost,
            GroupId = ConsumerGroupId,
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
                    ConsumeBatch(consumer, TimeSpan.FromMilliseconds(100), BatchSize);
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

    private void ConsumeBatch<TKey, TValue>(IConsumer<TKey, TValue> consumer, TimeSpan timeout, int batchSize)
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
                ProcessBatch(consumer, messageBatch);
                return;
            }

            Console.WriteLine(
                $"{message.Topic}:{message.Partition}:{message.Offset} => Consumed event with " +
                $"key = {message.Message.Key,-20} value = {message.Message.Value}");
            messageBatch.Add(message);
            latestPartition = message.Partition.Value;
        }

        if (messageBatch.Any())
        {
            ProcessBatch(consumer, messageBatch);
        }
        else
        {
            Console.WriteLine("- - - - - Waiting for new messages - - - - -");
        }
    }

    private void ProcessBatch<TKey, TValue>(IConsumer<TKey, TValue> consumer,
        IReadOnlyCollection<ConsumeResult<TKey, TValue>> messageBatch)
    {
        try
        {
            consumer.Commit(messageBatch.Last());
            ProcessMessages(messageBatch.Select(x => x.Message).ToArray());
            StoreCurrentOffset(messageBatch.Last().TopicPartitionOffset);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Unexpected error occured during processing: {e.Message}");
            var topicPartition = messageBatch.Last().TopicPartition;
            var offset = RestoreLastOffset(topicPartition);
            consumer.Seek(new TopicPartitionOffset(topicPartition, new Offset(offset)));
        }
    }

    private void ProcessMessages<TKey, TValue>(IReadOnlyCollection<Message<TKey, TValue>> messages)
    {
        Console.WriteLine("- - - - - Processing batch messages - - - - -");
    }

    private void StoreCurrentOffset(TopicPartitionOffset tpo)
    {
        _redis.StoreValue($"{tpo.Topic}:{tpo.Partition.Value}", tpo.Offset.Value.ToString());
        Console.WriteLine(
            $"Stored offset {tpo.Offset.Value} for topic = {tpo.Topic} partition = {tpo.Partition.Value}");
    }

    private long RestoreLastOffset(TopicPartition tp)
    {
        var offsetString = _redis.RetrieveValue($"{tp.Topic}:{tp.Partition.Value}");
        if (long.TryParse(offsetString, out var offset))
        {
            Console.WriteLine($"Restored offset {offset} for topic = {tp.Topic} partition = {tp.Partition.Value}");
            return offset;
        }
        else
        {
            Console.WriteLine($"No offset for topic = {tp.Topic} partition = {tp.Partition.Value}");
            return 0;
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

    private void OnError(Action<IConsumer<string, string>, Error> handler) => _errorsHandlers.Add(handler);
}