namespace demoedgev2
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Loader;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Reactive;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Azure.Devices.Shared;
    using Newtonsoft.Json;
    using System.Reactive.Linq;
    using System.Linq;

    class Program
    {
        static readonly Random rand = new Random();
        static void Main(string[] args)
        {
            // Initialize Edge Module
            InitEdgeModule().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }
        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the Azure IoT Client for the Edge Module
        /// </summary>
        static async Task<IDisposable> InitEdgeModule()
        {
            // Open a connection to the Edge runtime using MQTT transport and
            // the connection string provided as an environment variable
            ITransportSettings[] settings =
            {
                    new MqttTransportSettings(TransportType.Mqtt_Tcp_Only)
                    { RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true }
                };

            DeviceClient IoTHubModuleClient = DeviceClient.CreateFromConnectionString(Environment.GetEnvironmentVariable("EdgeHubConnectionString"), settings);
            await IoTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            return
                 Observable.Interval(new TimeSpan(0, 0, 1))
                     .Select(i =>
                                 new MessageBody
                                 {
                                     Values =
                                         Enumerable.Range(0, 12)
                                             .Select(item => new MessageBody.MessageValue { DataPoint = $"Some{item}_Value", Value = rand.Next(0, 1001) })
                                             .ToList()
                                 }
                     )
                     .Select(JsonConvert.SerializeObject)
                     .Select(messageString => Encoding.UTF8.GetBytes(messageString))
                     .Select(messageBytes => new Message(messageBytes))
                     .SelectMany(async message =>
                     {
                         await IoTHubModuleClient.SendEventAsync("DataOutput", message);
                         return Unit.Default;
                     })
                     .Subscribe(_ => Console.WriteLine($"{DateTime.Now} - Sent message"),
                                ex =>
                                {
                                    Console.WriteLine(DateTime.Now + " - Error " + ex.Message);
                                    Console.WriteLine(ex.ToString());
                                    Console.WriteLine();
                                });
        }

    }

    public class MessageBody
    {
        public IReadOnlyList<MessageValue> Values { get; set; }

        public class MessageValue
        {
            public string DataPoint { get; set; }
            public int Value { get; set; }
        }
    }
}