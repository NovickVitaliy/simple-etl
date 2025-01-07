using simple_etl.Database;
using simple_etl.Services.Interfaces;

const string connectionString = "Server=localhost;Database=cab_db;User Id=sa;Password=SwN12345678;Encrypt=False;TrustServerCertificate=True";

await DbInitializer.InitiliazeDb(connectionString);

var dbConnectionFactory = new DbConnectionFactory(connectionString);
var csvCabDataProcessor = new CsvToSqlCabDataProcessor(dbConnectionFactory);
Console.WriteLine("Welcome to the simple ETL system");
while (true)
{
    Console.WriteLine("Choose your next option:");
    Console.WriteLine("1 - Insert data from CSV file");
    Console.WriteLine("2 - Queries");
    Console.WriteLine("3 - Remove duplicates and write to duplicates.csv");
    Console.WriteLine("q - To exit the app");

    var input = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(input))
    {
        Console.WriteLine("Input cannot be empty");
        continue;
    }

    if (input == "q")
    {
        Console.WriteLine("Goodbye!");
        break;
    }

    if (!int.TryParse(input, out int option))
    {
        Console.WriteLine("Input is not a valid option choice");
        continue;
    }

    switch (option)
    {
        case 1:
            //insert data from csv
            Console.WriteLine("Input file path");
            var filePath = Console.ReadLine();
            var result = await csvCabDataProcessor.InsertIntoDb(filePath!);
            if (!string.IsNullOrWhiteSpace(result.ErrorMessage))
            {
                Console.WriteLine("Error occured");
                Console.WriteLine(result.ErrorMessage);
                continue;
            }

            Console.WriteLine("Records have been successfully written to the database");
            continue;
        case 2:
            //perform queries
            continue;
        case 3:
            await csvCabDataProcessor.RemoveDuplicates();
            Console.WriteLine("Duplicates were succesffully removed");
            continue;
        default:
            Console.WriteLine("Non existent option");
            break;
    }
}