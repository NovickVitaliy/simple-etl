using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using CsvHelper;
using Microsoft.Data.SqlClient;
using simple_etl.Database;
using simple_etl.ErrorHandling;
using simple_etl.Models;
using simple_etl.Services.Implementations;

namespace simple_etl.Services.Interfaces;

public class CabDataProcessor : ICabDataProcessor
{
    private readonly DbConnectionFactory _dbConnectionFactory;

    public CabDataProcessor(DbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task<ErrorOr<bool>> InsertIntoDbFromCsv(string csvFilePath)
    {
        if (string.IsNullOrWhiteSpace(csvFilePath))
        {
            return ErrorOr<bool>.Failure("Path to file cannot be empty");
        }
        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        var dataTable = new DataTable();
        DataColumn column = new DataColumn();
        column.DataType = typeof(int);
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
            var tpepPickupDatetime = DateTime.Parse(record.tpep_pickup_datetime);
            var tpepDropoffDatetime = DateTime.Parse(record.tpep_dropoff_datetime);
            int.TryParse(record.passenger_count as string, out int passengerCount);
            var tripDistance = double.Parse(record.trip_distance.ToString());
            var storeAndFwdFlag = record.store_and_fwd_flag.ToString().Trim() == "Y" ? "Yes" : "No";
            var pulocationId = int.Parse(record.PULocationID.ToString());
            var dolocationId = int.Parse(record.DOLocationID.ToString());
            var fareAmount = double.Parse(record.fare_amount.ToString());
            var tipAmount = double.Parse(record.tip_amount.ToString());
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

        using var sqlBulkCopy = new SqlBulkCopy((SqlConnection)connection);
        sqlBulkCopy.DestinationTableName = "cab_data";
        await sqlBulkCopy.WriteToServerAsync(dataTable);

        return ErrorOr<bool>.Success(true);
    }

    public async Task<ErrorOr<bool>> RemoveDuplicatesAndMoveToCsv(string csvFilePath)
    {
        await using var streamWriter = new StreamWriter(csvFilePath);
        await using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);
        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
                          WITH DuplicateCTE AS (
                              SELECT
                                  tpep_pickup_datetime,
                                  tpep_dropoff_datetime,
                                  passenger_count,
                                  COUNT(*) AS DuplicateCount
                              FROM cab_data 
                              GROUP BY
                                  tpep_pickup_datetime,
                                  tpep_dropoff_datetime,
                                  passenger_count
                              HAVING COUNT(*) > 1 
                          )
                          SELECT *
                          FROM cab_data t
                                   JOIN DuplicateCTE d
                                        ON t.tpep_pickup_datetime = d.tpep_pickup_datetime
                                            AND t.tpep_dropoff_datetime = d.tpep_dropoff_datetime
                                            AND t.passenger_count = d.passenger_count;
                          """;

        var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var tpepPickupDatetime = reader.GetDateTime("tpep_pickup_datetime");
            var tpepDropoffDatetime = reader.GetDateTime("tpep_dropoff_datetime");
            var passengerCount = reader.GetInt32("passenger_count");
            var tripDistance = reader.GetDouble("trip_distance");
            var storeAndFwdFlag = reader.GetString("store_and_fwd_flag");
            var pulocationId = reader.GetInt32("PULocationID");
            var dolocationId = reader.GetInt32("DOLocationID");
            var fareAmount = reader.GetDouble("fare_amount");
            var tipAmount = reader.GetDouble("tip_amount");
            csvWriter.WriteRecord(new
            {
                tpepPickupDatetime, tpepDropoffDatetime, passengerCount, tripDistance, storeAndFwdFlag,
                pulocationId, dolocationId, fareAmount, tipAmount
            });
            await csvWriter.NextRecordAsync();
        }

        await reader.CloseAsync();

        cmd.CommandText = """
                          WITH DuplicateCTE AS (
                              SELECT
                                  id,
                                  ROW_NUMBER() OVER (PARTITION BY
                                      tpep_pickup_datetime,
                                      tpep_dropoff_datetime,
                                      passenger_count
                                      ORDER BY id) AS rn
                              FROM cab_data
                          )
                          DELETE FROM cab_data
                          WHERE id IN (
                              SELECT id
                              FROM DuplicateCTE
                              WHERE rn > 1
                          );
                          """;
        await cmd.ExecuteNonQueryAsync();

        return ErrorOr<bool>.Success(true);
    }

    public async Task<long> FindLocationWithTheHighestTipAmountOnAverage()
    {
        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
                                SELECT TOP 1 PULocationID FROM cab_data
                                GROUP BY PULocationID 
                                ORDER BY AVG(tip_amount) DESC 
                                """;
        var id = (int?)await dbCommand.ExecuteScalarAsync();
        return id.Value;
    }

    public async Task<List<Fare>> Top100LongestFaresByDistance()
    {
        List<Fare> fares = [];
        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
                                SELECT TOP 100 * FROM cab_data
                                ORDER BY trip_distance DESC
                                """;
        await using var reader = await dbCommand.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            fares.Add(ReadFare(reader));
        }

        return fares;
    }

    public async Task<List<Fare>> Top100LongestFaresByTime()
    {
        List<Fare> fares = [];
        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
                                SELECT TOP 100 * FROM cab_data
                                ORDER BY tpep_dropoff_datetime - tpep_pickup_datetime DESC
                                """;
        await using var reader = await dbCommand.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            fares.Add(ReadFare(reader));
        }

        return fares;
    }

    public async Task<List<Fare>> SearchByPickupLocation(int pickupLocationId)
    {
        List<Fare> fares = [];
        await using var connection = _dbConnectionFactory.GetConnection();
        await connection.OpenAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = """
                                SELECT * FROM cab_data
                                WHERE PULocationID = @pickupLocationId
                                """;
        dbCommand.Parameters.Add(new SqlParameter("pickupLocationId", SqlDbType.Int)
        {
            Value = pickupLocationId
        });
        await using var reader = await dbCommand.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            fares.Add(ReadFare(reader));
        }

        return fares;
    }

    private Fare ReadFare(DbDataReader reader)
    {
        return new Fare
        {
            TpepPickupDatetime = reader.GetDateTime(reader.GetOrdinal("tpep_pickup_datetime")),
            TpepDropoffDateTime = reader.GetDateTime(reader.GetOrdinal("tpep_dropoff_datetime")),
            PassengerCount = reader.GetInt32(reader.GetOrdinal("passenger_count")),
            TripDistance = reader.GetDouble(reader.GetOrdinal("trip_distance")),
            StoreAndFwdFlag = reader.GetString(reader.GetOrdinal("store_and_fwd_flag")),
            PULocationId = reader.GetInt32(reader.GetOrdinal("PULocationID")),
            DOLocationId = reader.GetInt32(reader.GetOrdinal("DOLocationID")),
            FareAmount = reader.GetDouble(reader.GetOrdinal("fare_amount")),
            TipAmount = reader.GetDouble(reader.GetOrdinal("tip_amount"))
        };
    }
}