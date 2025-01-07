using simple_etl.Database;
using simple_etl.Models;
using simple_etl.Services.Interfaces;

const string connectionString = "Server=localhost;Database=cab_db;User Id=sa;Password=SwN12345678;Encrypt=False;TrustServerCertificate=True";

await DbInitializer.InitiliazeDb(connectionString);

var dbConnectionFactory = new DbConnectionFactory(connectionString);
var cabDataProcessor = new CabDataProcessor(dbConnectionFactory);
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
            var result = await cabDataProcessor.InsertIntoDbFromCsv(filePath!);
            if (!string.IsNullOrWhiteSpace(result.ErrorMessage))
            {
                Console.WriteLine("Error occured");
                Console.WriteLine(result.ErrorMessage);
                continue;
            }

            Console.WriteLine("Records have been successfully written to the database");
            continue;
        case 2:
            while (true)
            {
                Console.WriteLine("1 - Find out which `PULocationId` (Pick-up location ID) has the highest tip_amount on average.");
                Console.WriteLine("2 - Find the top 100 longest fares in terms of `trip_distance`.");
                Console.WriteLine("3 - Find the top 100 longest fares in terms of time spent traveling.");
                Console.WriteLine("4 - Search by pickup location id");
                var queryInput = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(input))
                {
                    Console.WriteLine("Input cannot be empty");
                    continue;
                }

                if (!int.TryParse(queryInput, out int queryOption))
                {
                    Console.WriteLine("Input is not a valid option choice");
                    continue;
                }

                switch (queryOption)
                {
                    case 1:
                        long id = await cabDataProcessor.FindLocationWithTheHighestTipAmountOnAverage();
                        Console.WriteLine($"PULocatioId with the highest tip amount on average: {id}");
                        break;
                    case 2:
                        List<Fare> longestByDistance = await cabDataProcessor.Top100LongestFaresByDistance();
                        Console.WriteLine("Longest fares by distance traveled:");
                        foreach (var fare in longestByDistance)
                        {
                            Console.WriteLine(fare);
                        }
                        break;
                    case 3:
                        List<Fare> longestByTimeSpent = await cabDataProcessor.Top100LongestFaresByTime();
                        Console.WriteLine("Longest fares by time spent travelling:");
                        foreach (var fare in longestByTimeSpent)
                        {
                            Console.WriteLine(fare);
                        }
                        break;
                    case 4:
                        Console.WriteLine("Enter pickup location id:");
                        var pickupLocationInput = Console.ReadLine();
                        if (string.IsNullOrWhiteSpace(pickupLocationInput))
                        {
                            Console.WriteLine("Input cannot be empty");
                            continue;
                        }

                        if (!int.TryParse(pickupLocationInput, out int pickupLocationId))
                        {
                            Console.WriteLine("Input is not a valid number");
                            continue;
                        }
                        List<Fare> faresByPickupLocationId = await cabDataProcessor.SearchByPickupLocation(pickupLocationId);
                        Console.WriteLine("Fares by pickup location:");
                        foreach (var fare in faresByPickupLocationId)
                        {
                            Console.WriteLine(fare);
                        }
                        break;
                }

                break;
            }
            continue;
        case 3:
            Console.WriteLine("Input file path to put duplicates");
            var csvFile = Console.ReadLine();
            await cabDataProcessor.RemoveDuplicatesAndMoveToCsv(csvFile);
            Console.WriteLine("Duplicates were succesffully removed");
            continue;
        default:
            Console.WriteLine("Non existent option");
            break;
    }
}