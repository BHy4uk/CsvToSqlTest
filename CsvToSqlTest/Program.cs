using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Configuration;

namespace CsvToSqlETL
{
    class Program
    {
        private static string connectionString;

        static void Main(string[] args)
        {
            // Load configuration
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables();
            IConfiguration configuration = builder.Build();

            connectionString = configuration.GetConnectionString("DefaultConnection");

            string csvFilePath = configuration["CsvFilePath"];
            string duplicatesFilePath = configuration["DuplicatesFilePath"];

            DataTable dataTable = CreateDataTable();

            using (var reader = new StreamReader(csvFilePath))
            using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { TrimOptions = TrimOptions.Trim }))
            {
                csv.Context.RegisterClassMap<TaxiTripMap>();
                var records = csv.GetRecords<TaxiTrip>().ToList();

                foreach (var record in records)
                {
                    DataRow row = dataTable.NewRow();
                    row["tpep_pickup_datetime"] = ConvertToUtc(record.tpep_pickup_datetime);
                    row["tpep_dropoff_datetime"] = ConvertToUtc(record.tpep_dropoff_datetime);
                    row["passenger_count"] = record.passenger_count;
                    row["trip_distance"] = record.trip_distance;
                    row["store_and_fwd_flag"] = record.store_and_fwd_flag == "Y" ? "Yes" : "No";
                    row["PULocationID"] = record.PULocationID;
                    row["DOLocationID"] = record.DOLocationID;
                    row["fare_amount"] = record.fare_amount;
                    row["tip_amount"] = record.tip_amount;

                    dataTable.Rows.Add(row);
                }
            }

            // Remove duplicates
            var duplicateRows = dataTable.AsEnumerable()
                .GroupBy(r => new { Pickup = r["tpep_pickup_datetime"], Dropoff = r["tpep_dropoff_datetime"], Passenger = r["passenger_count"] })
                .Where(g => g.Count() > 1)
                .SelectMany(g => g.Skip(1))
                .ToList();

            // Write duplicates to CSV
            using (var writer = new StreamWriter(duplicatesFilePath))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteRecords(duplicateRows.Select(row => new TaxiTrip
                {
                    tpep_pickup_datetime = (DateTime) row["tpep_pickup_datetime"],
                    tpep_dropoff_datetime = (DateTime) row["tpep_dropoff_datetime"],
                    passenger_count = (int) row["passenger_count"],
                    trip_distance = (double) row["trip_distance"],
                    store_and_fwd_flag = (string) row["store_and_fwd_flag"],
                    PULocationID = (int) row["PULocationID"],
                    DOLocationID = (int) row["DOLocationID"],
                    fare_amount = (decimal) row["fare_amount"],
                    tip_amount = (decimal) row["tip_amount"]
                }));
            }

            foreach (var duplicateRow in duplicateRows)
            {
                dataTable.Rows.Remove(duplicateRow);
            }

            // Bulk insert into SQL Server
            BulkInsert(dataTable);

            Console.WriteLine("ETL process completed successfully.");
        }

        private static DataTable CreateDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
            table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
            table.Columns.Add("passenger_count", typeof(int));
            table.Columns.Add("trip_distance", typeof(double));
            table.Columns.Add("store_and_fwd_flag", typeof(string));
            table.Columns.Add("PULocationID", typeof(int));
            table.Columns.Add("DOLocationID", typeof(int));
            table.Columns.Add("fare_amount", typeof(decimal));
            table.Columns.Add("tip_amount", typeof(decimal));
            return table;
        }

        private static DateTime ConvertToUtc(DateTime estDateTime)
        {
            TimeZoneInfo estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            return TimeZoneInfo.ConvertTimeToUtc(estDateTime, estZone);
        }

        private static void BulkInsert(DataTable dataTable)
        {
            //To prevent SQL injection attacks SQL code was separated from user input.
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                string insertQuery = @"INSERT INTO TaxiTrips 
                               (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, 
                               store_and_fwd_flag, PULocationID, DOLocationID, fare_amount, tip_amount) 
                               VALUES 
                               (@PickupDateTime, @DropoffDateTime, @PassengerCount, @TripDistance, 
                               @StoreAndFwdFlag, @PULocationID, @DOLocationID, @FareAmount, @TipAmount)";

                using (SqlCommand command = new SqlCommand(insertQuery, connection))
                {
                    command.Parameters.Add("@PickupDateTime", SqlDbType.DateTime);
                    command.Parameters.Add("@DropoffDateTime", SqlDbType.DateTime);
                    command.Parameters.Add("@PassengerCount", SqlDbType.Int);
                    command.Parameters.Add("@TripDistance", SqlDbType.Float);
                    command.Parameters.Add("@StoreAndFwdFlag", SqlDbType.NVarChar);
                    command.Parameters.Add("@PULocationID", SqlDbType.Int);
                    command.Parameters.Add("@DOLocationID", SqlDbType.Int);
                    command.Parameters.Add("@FareAmount", SqlDbType.Decimal);
                    command.Parameters.Add("@TipAmount", SqlDbType.Decimal);

                    foreach (DataRow row in dataTable.Rows)
                    {
                        command.Parameters["@PickupDateTime"].Value = row["tpep_pickup_datetime"];
                        command.Parameters["@DropoffDateTime"].Value = row["tpep_dropoff_datetime"];
                        command.Parameters["@PassengerCount"].Value = row["passenger_count"];
                        command.Parameters["@TripDistance"].Value = row["trip_distance"];
                        command.Parameters["@StoreAndFwdFlag"].Value = row["store_and_fwd_flag"];
                        command.Parameters["@PULocationID"].Value = row["PULocationID"];
                        command.Parameters["@DOLocationID"].Value = row["DOLocationID"];
                        command.Parameters["@FareAmount"].Value = row["fare_amount"];
                        command.Parameters["@TipAmount"].Value = row["tip_amount"];

                        command.ExecuteNonQuery();
                    }
                }
            }
        }

    }
}