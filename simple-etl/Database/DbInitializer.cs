using DbUp;

namespace simple_etl.Database;

public static class DbInitializer
{
    public static async Task InitiliazeDb(string connectionString)
    {
        EnsureDatabase.For.SqlDatabase(connectionString);
        
        var upgrader = DeployChanges.To.SqlDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(typeof(Program).Assembly)
            .LogToConsole()
            .Build();

        if (upgrader.IsUpgradeRequired())
        {
            upgrader.PerformUpgrade();
        }
    }
}