using simple_etl.ErrorHandling;

namespace simple_etl.Services.Implementations;

public interface ICsvCabDataProcessor
{
    Task<ErrorOr<bool>> InsertIntoDb(string csvFilePath);
    Task<ErrorOr<bool>> RemoveDuplicates();
}