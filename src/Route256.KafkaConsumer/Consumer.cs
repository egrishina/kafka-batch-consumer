using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Route256.Redis;

namespace Route256.KafkaConsumer;

public class Consumer
{
    private const string KafkaHost = "localhost:9092";
    private const string Topic = "temperature";
    private const string ConsumerGroupId = "group_1";
    private const int BatchSize = 10;

    private readonly ILogger<Consumer> _logger;
    private readonly RedisCache _redis;
    private readonly List<Action<IConsumer<string, string>, Error>> _errorsHandlers = new();

    public Consumer(ILogger<Consumer> logger)
    {
        RegisterLogErrorHandler();

        _logger = logger;
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
                _logger.LogError("Consumer Error: {@Message}", e.Message);
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

            _logger.LogInformation(
                "{@Topic}:{@Partition}:{@Offset} => Consumed event with key = {@Key} value = {@Value}",
                message.Topic, message.Partition, message.Offset, message.Message.Key, message.Message.Value);

            messageBatch.Add(message);
            latestPartition = message.Partition.Value;
        }

        if (messageBatch.Any())
        {
            ProcessBatch(consumer, messageBatch);
        }
        else
        {
            _logger.LogDebug("- - - - - Waiting for new messages - - - - -");
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
            _logger.LogError("Unexpected error occured during processing: {@Message}", e.Message);
            var topicPartition = messageBatch.Last().TopicPartition;
            var offset = RestoreLastOffset(topicPartition);
            consumer.Seek(new TopicPartitionOffset(topicPartition, new Offset(offset)));
        }
    }

    private void ProcessMessages<TKey, TValue>(IReadOnlyCollection<Message<TKey, TValue>> messages)
    {
        _logger.LogDebug("- - - - - Processing batch messages - - - - -");
    }

    private void StoreCurrentOffset(TopicPartitionOffset tpo)
    {
        _redis.StoreValue($"{tpo.Topic}:{tpo.Partition.Value}", tpo.Offset.Value.ToString());
        _logger.LogInformation("Stored offset {@Offset} for topic = {@Topic} partition = {@Partition}", tpo.Offset.Value, tpo.Topic, tpo.Partition.Value);
    }

    private long RestoreLastOffset(TopicPartition tp)
    {
        var offsetString = _redis.RetrieveValue($"{tp.Topic}:{tp.Partition.Value}");
        if (long.TryParse(offsetString, out var offset))
        {
            _logger.LogInformation("Restored offset {@Offset} for topic = {@Topic} partition = {@Partition}", offset, tp.Topic, tp.Partition.Value);
            return offset;
        }
        else
        {
            _logger.LogInformation("No offset for topic = {@Topic} partition = {@Partition}", tp.Topic, tp.Partition.Value);
            return 0;
        }
    }

    private void RegisterLogErrorHandler()
    {
        OnError((_, error) =>
        {
            if (error.IsFatal)
            {
                _logger.LogError("Kafka Consumer Internal Error: Code {@Code}, Reason {@Reason}", error.Code, error.Reason);
            }
            else
            {
                _logger.LogWarning("Kafka Consumer Internal Warning: Code {@Code}, Reason {@Reason}", error.Code, error.Reason);
            }
        });
    }

    private void OnError(Action<IConsumer<string, string>, Error> handler) => _errorsHandlers.Add(handler);
}