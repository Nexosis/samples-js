using Nexosis.Api.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Nexosis.Api.Samples
{
    class Program
    {
        /// <summary>
        /// The name by which you can refer to the saved dataset
        /// </summary>
        private const string DatasetName = "GeneratedJson100K";

        /// <summary>
        /// A simple program that serves two purposes: One - how to upload a large dataset in batches before running a forecast. Two - allows you to upload generated data to play around with the features of the Nexosis API.
        /// </summary>
        static void Main(string[] args)
        {
            //enter your own api key here, or pass on the command line:
            var apiKey = "your api key";
            if (args != null && args.Count() == 1)
                apiKey = args[0];
            var client = new ApiClient(apiKey);
            //should not change...
            client.BaseUrl = "https://ml.nexosis.com/api";
            //Will create a new named dataset on which you can run forecast operations.  Note that the generated data is mostly random and will usually just return naive forecasts.
            var dataTask = SendLotsOfData(client);
            dataTask.Wait();
            //Set a start and end date that make sense for the generated dataset.  These dates match up to a 100K hourly observation set.
            var newSession = client.ForecastFromSavedDataSetAsync(new SessionData { DataSetName = DatasetName, StartDate = DateTime.Parse("1960-07-29"), EndDate = DateTime.Parse("1960-11-29") }, "Column_2").Result;
            Console.WriteLine($"Your forecast session has been submitted: {newSession.Status}");

            var sessionReturned = false;
            while (!sessionReturned)
            {
                var sessionStatus = client.GetSessionStatusAsync(newSession.SessionId).Result;
                if (sessionStatus.Status == SessionResponseDtoStatus.Completed)
                {
                    sessionReturned = true;
                    var sessionResults = client.GetSessionResultsAsync(newSession.SessionId).Result;
                    var json = AsJson(sessionResults);
                    Console.WriteLine($"Results ready. Here's a preview... {json.Substring(0,300)}");
                }
                else
                {
                    Console.WriteLine("Waiting for completed results...");
                    Thread.Sleep(3000);
                }
            }
            Console.WriteLine("Job completed. Press enter to quit.");
            Console.ReadLine();
        }

        /// <summary>
        /// How many total rows to enter into the database as observations - total dataset size. 
        /// </summary>
        /// <remarks>
        /// Given current cost structure, an upload of 100K records will charge the associated account $0.01.
        /// </remarks>
        const int BATCH_TOTAL = 100000;
        /// <summary>
        /// Keeps the request size below maximums. If you're getting a 413 response, you need to decrease this number.
        /// </summary>
        const int BATCH_SIZE = 5000;
        private static async Task SendLotsOfData(ApiClient client)
        {
            for (var batchCount = 0; batchCount < (BATCH_TOTAL / BATCH_SIZE); batchCount++)
            {
                Console.WriteLine($"Sending {BATCH_SIZE} more rows to the API...");
                var jsonSession = await client.SaveDataSetAsync(DatasetName, GeneratedData(BATCH_SIZE));
            }
            Console.Write("All data uploaded. Ready to run forecast");
        }

        private static DataSet GeneratedData(int rows)
        {
            var generator = new DataGenerator();
            var dataSet = generator.CreateDataSet(6, rows);
            return dataSet;
        }

        /// <summary>
        /// Utility method to save off a dataset as Json
        /// </summary>
        /// <param name="dataSet">The filled dataset object</param>
        /// <returns>A JSON string representing a time series dataset</returns>
        private static string AsJson(DataSet dataSet)
        {
            var stream = new System.IO.MemoryStream(1024);
            using (var writer = new StreamWriter(stream))
            {
                new Newtonsoft.Json.JsonSerializer().Serialize(writer, dataSet, typeof(DataSet));
                writer.Flush();
                stream.Position = 0;
                return new StreamReader(stream).ReadToEnd();
            }
        }

        class DataGenerator
        {
            public DataSet CreateDataSet(int columns, int lines)
            {
                var dataSet = new DataSet() { Data = new List<DataSetRow>() };
                var headers = CreateColumns(columns);
                for (var rowCount = 0; rowCount < lines; rowCount++)
                    dataSet.Data.Add(CreateRow(headers));
                return dataSet;
            }

            const string ROW_PREFIX = "Column_";
            /// <summary>
            /// Generate a given number of dummy column. There is always a timestamp column.
            /// </summary>
            /// <param name="colCount">How many columns to create for this dataset</param>
            /// <returns>An array of column names</returns>
            public string[] CreateColumns(int colCount)
            {
                var columns = new List<string>();
                columns.Add("timestamp");
                for (var count = 1; count < colCount; count++)
                {
                    columns.Add($"{ROW_PREFIX}{count.ToString()}");
                }
                return columns.ToArray();
            }

            /// <summary>
            /// Enough time slots for 500000 hourly observations
            /// </summary>
            static DateTime _lastTime = new DateTime((DateTime.Now - TimeSpan.FromDays(365 * 58)).Ticks);
            static int _totalObservationsCreated = 1;
            /// <summary>
            /// Creates values in a DataSetRow object for the given columns along with a timestamp entry for each row.
            /// </summary>
            /// <param name="columns">The array of column names which each non-timestamp column will fill</param>
            /// <returns>A DataSetRow object for use in a DataSet</returns>
            public DataSetRow CreateRow(string[] columns)
            {
                _totalObservationsCreated++;
                //creates observations which are seperated by the following change
                var rowTime = _lastTime + TimeSpan.FromHours(1);

                var row = new DataSetRow { Timestamp = rowTime, Values = new Dictionary<string, double>() };
                _lastTime = rowTime;

                for (var index = 1; index < columns.Length; index++)
                {
                    row.Values[columns[index]] = GetRandomDouble();
                }
                return row;
            }

            Random _random;
            private Random RandomGen { get { if (_random == null) _random = new Random(); return _random; } }

            public double GetRandomDouble()
            {
                //Synthetic growth (yearly if daily observations, more quickly for hourly, etc.)
                var start = RandomGen.NextDouble();
                var min = _totalObservationsCreated / 365;
                var max = min + 30;
                var multiple = RandomGen.Next(min, max);
                return start * (double)multiple;
            }
        }
    }
}