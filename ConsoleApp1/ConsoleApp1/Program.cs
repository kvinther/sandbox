using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ConsoleApp1
{
    class Program
    {

        const string QueueConnectionString =        "Endpoint=sb://shuffle.servicebus.windows.net/;SharedAccessKeyName=SendListen;SharedAccessKey=Ms2rgPQekVZjx9W9rEsro/qY5JjS5i4EsOq02sdZFsQ=";
        const string QueueName = "testqueue";
        const string SubscriptionConnectionString = "Endpoint=sb://shuffle.servicebus.windows.net/;SharedAccessKeyName=LogTopicSendListen;SharedAccessKey=AHPlQrVOf5+haJfmrw6VA5o2caDIMOK77B+3EoF7y8U=";
        const string TopicName = "log";
        const string SubscriptionName1 = "subscription01";
        const string SubscriptionName2 = "subscription02";

        static IQueueClient _queueClient;
        static ITopicClient _topicClient;
        static ISubscriptionClient _subscriptionClient01;
        static ISubscriptionClient _subscriptionClient02;

        static int _messageCounter = 1;

        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            const int numberOfMessages = 10;
            _queueClient = new QueueClient(QueueConnectionString, QueueName);
            _topicClient = new TopicClient(SubscriptionConnectionString, TopicName);
            _subscriptionClient01 = new SubscriptionClient(SubscriptionConnectionString, TopicName, SubscriptionName1);
            _subscriptionClient02 = new SubscriptionClient(SubscriptionConnectionString, TopicName, SubscriptionName2);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press Q to send queue messages.");
            Console.WriteLine("Press T to send topic messages.");
            Console.WriteLine("Press any other key to exit.");
            Console.WriteLine("======================================================");

            // Register the queue message handler and receive messages in a loop
            RegisterMessageHandlers();
            
            var exit = false;
            do
            {
                var s = Console.ReadKey(true);

                switch (s.Key)
                {
                    case ConsoleKey.Q:
                        await SendQueueMessages(numberOfMessages);
                        break;
                    case ConsoleKey.T:
                        await SendTopicMessages(numberOfMessages);
                        break;
                    default:
                        exit = true;
                        break;
                }

            } while (exit == false);

            await _queueClient.CloseAsync();
            await _topicClient.CloseAsync();
            await _subscriptionClient01.CloseAsync();
        }

        static void RegisterMessageHandlers()
        {
            _subscriptionClient01.RegisterMessageHandler(async (message, c) =>
            {
                HandleMessage(message, "sub01");
                await _subscriptionClient01.CompleteAsync(message.SystemProperties.LockToken);

            }, GetMessageHandlerOptions());

            _subscriptionClient02.RegisterMessageHandler(async (message, c) =>
            {
                HandleMessage(message, "sub02");
                await _subscriptionClient02.CompleteAsync(message.SystemProperties.LockToken);

            }, GetMessageHandlerOptions());

            _queueClient.RegisterMessageHandler(async (message, c) =>
            {
                HandleMessage(message, "queue");
                await _queueClient.CompleteAsync(message.SystemProperties.LockToken);

            }, GetMessageHandlerOptions());
        }

        static void HandleMessage(Message message, string prefix = null)
        {
            prefix = prefix == null ? string.Empty : prefix + ":";
            var body = Encoding.UTF8.GetString(message.Body);
            Console.WriteLine($"message:{prefix}{message.SystemProperties.SequenceNumber}:{body}");
        }

        // Use this handler to examine the exceptions received on the message pump.
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        static async Task SendTopicMessages(int numberOfMessagesToSend)
        {
            try
            {
                Enumerable.Range(0, numberOfMessagesToSend)
                    .ToList()
                    .ForEach(async x =>
                    {
                        await _topicClient.SendAsync(new Message(Encoding.UTF8.GetBytes($"{_messageCounter++}")));
                    });
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static async Task SendQueueMessages(int numberOfMessagesToSend)
        {
            try
            {
                Enumerable.Range(0, numberOfMessagesToSend)
                    .ToList()
                    .ForEach(async x =>
                    {
                        await _queueClient.SendAsync(new Message(Encoding.UTF8.GetBytes($"{_messageCounter++}")));
                    });
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static MessageHandlerOptions GetMessageHandlerOptions()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            return new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };
        }
    }
}
