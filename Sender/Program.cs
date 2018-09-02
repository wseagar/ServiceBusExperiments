namespace ServiceBusExperiment1
{
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class Program
    {
        const string ServiceBusConnectionString = "";
        const string TopicName = "topic1";
        const string SubscriptionName = "subscription1";

        static async Task MainAsync()
        {
            // 1. In the exclusive queue there was ~18 sessions
            // 2. One of the sessions had 45000 messages queued
            // 3. The topic was backed up by ~80,000 messages
            // 4. The service was at 100% CPU usage
            // 5. Users were reporting that they couldn't use exporting functionality

            // The user uploaded 150,000 products and triggered 150,000 entity changed events
            // MaxConcurrentSessions = 800 would of spawned 800 processes to work through these entity changes
            // This would of starved processsing from other users

            // Solution: use Session based subsciptions to Topic where the SessionId = OrgId
            // This means that the subscriptions would only process one EntityChange from one Org at a time.


            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to START sending messages.");
            Console.WriteLine("======================================================");
            Console.ReadKey();

            // Lets put 200 messages from ORG A onto the topic FIRST before any from B, C, D, E
            // OnMessage / OnMessageAync will process ORG A's messages first and will starve the messages from B C D or E

            // However using a SessionHandler we should see A B C D E all process simultaneously.

            await SendMessagesAsync("A", 100);

            await Task.WhenAll(
                SendMessagesAsync("B", 10),
                SendMessagesAsync("C", 10),
                SendMessagesAsync("D", 10),
                SendMessagesAsync("E", 10)
            );

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to START receiving messages then ENTER key again to stop");
            Console.WriteLine("======================================================");
            Console.ReadKey();

            // Generate a cancellation token 
            var cts = new CancellationTokenSource();

            var doneReceiving = InitializeReceiver(cts.Token);
            Console.ReadKey();

            cts.Cancel();
            await doneReceiving;
        }

# region TOPIC_SENDER

        static async Task SendMessagesAsync(string OrgId, int numberOfMessagesToSend)
        {
            try
            {
                var senderFactory = MessagingFactory.CreateFromConnectionString(ServiceBusConnectionString);
                var sender = await senderFactory.CreateMessageSenderAsync(TopicName);

                // Lets simulate entity change events from the same org
                var data = new List<dynamic>();
                for (var i = 0; i < numberOfMessagesToSend; ++i)
                {
                    data.Add(new {EntityType = "Product", Id = i});
                }

                for (var i = 0; i < data.Count; i++)
                {
                    var json = JsonConvert.SerializeObject(data[i]);
                    
                    // Create a new message to send to the topic.
                    var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(json)));

                    // IMPORTANT: Set the session id to the ORG ID
                    message.SessionId = OrgId;
                    message.ContentType = "application/json";
                    message.MessageId = i.ToString();

                    // Send the message to the topic.
                    await sender.SendAsync(message);

                    // Write the message to the console.
                    lock (Console.Out)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"{OrgId} - Sending message: {i}");
                        Console.ResetColor();
                    }
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        #endregion

#region TOPIC_RECEIVER
        static async Task InitializeReceiver(CancellationToken ct)
        {
            var receiverFactory = MessagingFactory.CreateFromConnectionString(ServiceBusConnectionString);
            var receiver = receiverFactory.CreateSubscriptionClient(TopicName, SubscriptionName);

            var doneReceiving = new TaskCompletionSource<bool>();

            ct.Register(async () => {
                await receiver.CloseAsync();
                await receiverFactory.CloseAsync();
                doneReceiving.SetResult(true);
            });

            // register the OnMessageAsync callback
            await receiver.RegisterSessionHandlerAsync(typeof(SessionHandler),
                new SessionHandlerOptions
                {
                    MaxConcurrentSessions = 5,
                    AutoComplete = false
                });
                

            await doneReceiving.Task;
        }

        class SessionHandler : IMessageSessionAsyncHandler
        {
            public async Task OnCloseSessionAsync(MessageSession session)
            {

            }

            public async Task OnMessageAsync(MessageSession session, BrokeredMessage message)
            {
                if (message.ContentType != null && message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                {
                    var body = message.GetBody<Stream>();

                    dynamic entityChange = JsonConvert.DeserializeObject(new StreamReader(body, true).ReadToEnd());

                    ConsoleColor color;
                    switch (session.SessionId)
                    {
                        case "A":
                            color = ConsoleColor.DarkBlue;
                            break;
                        case "B":
                            color = ConsoleColor.DarkCyan;
                            break;
                        case "C":
                            color = ConsoleColor.DarkGray;
                            break;
                        case "D":
                            color = ConsoleColor.DarkGreen;
                            break;
                        case "E":
                            color = ConsoleColor.DarkMagenta;
                            break;
                        default:
                            color = ConsoleColor.White;
                            break;
                    }

                    lock (Console.Out)
                    {
                        Console.ForegroundColor = color;
                        Console.WriteLine($"{session.SessionId} - {entityChange.Id}");
                    }

                    await message.CompleteAsync();
                }
            }

            public async Task OnSessionLostAsync(Exception exception)
            { 

            }
        }
        #endregion

        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }
    }
}
