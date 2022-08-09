using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Route256.KafkaConsumer;

public static class Program
{
    public static async Task Main()
    {
        var serviceCollection = new ServiceCollection();
        ConfigureServices(serviceCollection);

        var serviceProvider = serviceCollection.BuildServiceProvider();

        var consumer = serviceProvider.GetService<Consumer>();
        await consumer.StartConsuming();
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        services
            .AddLogging(configure => configure.AddConsole())
            .AddTransient<Consumer>();
    }
}