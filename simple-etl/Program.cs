Console.WriteLine("Welcome to the simple ETL system");
while (true)
{
    Console.WriteLine("Choose your next option:");
    Console.WriteLine("1 - Insert data from CSV file");
    Console.WriteLine("2 - Queries");
    Console.WriteLine("q - To exit the app");

    var input = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(input))
    {
        Console.WriteLine("Input cannot be empty");
        continue;
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
            continue;
        case 2:
            //perform queries
            continue;
        default:
            Console.WriteLine("Non existent option");
            break;
    }
}