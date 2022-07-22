using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Route256.KafkaProducer;

public static class Program
{
    public static async Task Main()
    {
        var serviceCollection = new ServiceCollection();
        ConfigureServices(serviceCollection);

        var serviceProvider = serviceCollection.BuildServiceProvider();

        var producer = serviceProvider.GetService<Producer>();
        await producer.StartProducing();
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        services
            .AddLogging(configure => configure.AddConsole())
            .AddTransient<Producer>();
    }
}