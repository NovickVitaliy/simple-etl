using simple_etl.ErrorHandling;
using simple_etl.Models;

namespace simple_etl.Services.Implementations;

public interface ICabDataProcessor
{
    Task<ErrorOr<bool>> InsertIntoDbFromCsv(string csvFilePath);
    Task<ErrorOr<bool>> RemoveDuplicatesAndMoveToCsv(string csvFilePath);
    Task<long> FindLocationWithTheHighestTipAmountOnAverage();
    Task<List<Fare>> Top100LongestFaresByDistance();
    Task<List<Fare>> Top100LongestFaresByTime();
    Task<List<Fare>> SearchByPickupLocation(int pickupLocationId);
}