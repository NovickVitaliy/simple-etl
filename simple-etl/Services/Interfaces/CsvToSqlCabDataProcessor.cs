using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using CsvHelper;
using Microsoft.Data.SqlClient;
using simple_etl.Database;
using simple_etl.ErrorHandling;
using simple_etl.Services.Implementations;

namespace simple_etl.Services.Interfaces;

public class CsvToSqlCabDataProcessor : ICsvCabDataProcessor
{
    private readonly DbConnectionFactory _dbConnectionFactory;

    public CsvToSqlCabDataProcessor(DbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task<ErrorOr<bool>> InsertIntoDb(string csvFilePath)
    {
        if (string.IsNullOrWhiteSpace(csvFilePath))
        {
            return ErrorOr<bool>.Failure("Path to file cannot be empty");
        }

        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        var dataTable = new DataTable();

        DataColumn column = new DataColumn();
        column.DataType = System.Type.GetType("System.Int32");
        column.AutoIncrement = true;
        column.AutoIncrementSeed = 1;
        column.AutoIncrementStep = 1;
        
        
        dataTable.Columns.Add(column);
        dataTable.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        dataTable.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        dataTable.Columns.Add("passenger_count", typeof(int));
        dataTable.Columns.Add("trip_distance", typeof(double));
        dataTable.Columns.Add("store_and_fwd_flag", typeof(string));
        dataTable.Columns.Add("PULocationID", typeof(int));
        dataTable.Columns.Add("DOLocationID", typeof(int));
        dataTable.Columns.Add("fare_amount", typeof(double));
        dataTable.Columns.Add("tip_amount", typeof(double));

        using var reader = new StreamReader(csvFilePath);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        var records = csv.GetRecords<dynamic>();
        foreach (var record in records)
        {
            try
            {
                var tpepPickupDatetime = DateTime.Parse(record.tpep_pickup_datetime);
                var tpepDropoffDatetime = DateTime.Parse(record.tpep_dropoff_datetime);
                var passengerCount = 0;
                int.TryParse(record.passenger_count as string, out passengerCount);
                var tripDistance = double.Parse(record.trip_distance as string);
                var storeAndFwdFlag = (record.store_and_fwd_flag as string) == "Y" ? "Yes" : "No";
                var pulocationId = int.Parse(record.PULocationID as string);
                var dolocationId = int.Parse(record.DOLocationID as string);
                var fareAmount = double.Parse(record.fare_amount as string);
                var tipAmount = double.Parse(record.tip_amount as string);
                dataTable.Rows.Add(
                    DBNull.Value,
                    tpepPickupDatetime,
                    tpepDropoffDatetime,
                    passengerCount,
                    tripDistance,
                    storeAndFwdFlag,
                    pulocationId,
                    dolocationId,
                    fareAmount,
                    tipAmount);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        try
        {
            using var sqlBulkCopy = new SqlBulkCopy((SqlConnection)connection);
            sqlBulkCopy.DestinationTableName = "cab_data";
            await sqlBulkCopy.WriteToServerAsync(dataTable);
        }
        catch (Exception e)
        {
            return ErrorOr<bool>.Failure(e.Message);
        }

        return ErrorOr<bool>.Success(true);
    }
}