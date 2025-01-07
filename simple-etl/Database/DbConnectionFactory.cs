using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace simple_etl.Database;

public class DbConnectionFactory
{
    private readonly string _connectionString;
    
    public DbConnectionFactory(string connectionString)
    {
        _connectionString = connectionString;
    }

    public DbConnection GetConnection()
    {
        return new SqlConnection(_connectionString);
    }
}