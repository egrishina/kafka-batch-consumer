namespace Route256.KafkaProducer;

public static class Program
{
    public static async Task Main()
    {
        var producer = new Producer();
        await producer.StartProducing();
    }
}