namespace Route256.KafkaConsumer;

public static class Program
{
    public static async Task Main()
    {
        var consumer = new Consumer();
        await consumer.StartConsumer();
    }
}