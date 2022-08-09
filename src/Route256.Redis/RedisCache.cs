using StackExchange.Redis;

namespace Route256.Redis;

public class RedisCache
{
    private IDatabase _db;
    
    public async Task ConnectToDatabase()
    {
        var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
        _db = redis.GetDatabase();
    }

    public void StoreValue(string key, string value)
    {
        _db.StringSet(key, value);
    }

    public string RetrieveValue(string key)
    {
        return _db.StringGet(key).ToString();
    }
}
