using System;
using System.Diagnostics.Tracing;
using System.Threading.Tasks;
using Azure.Core.Diagnostics;
using Azure.Messaging.ServiceBus;

namespace ServiceBusErrorLogging
{
    class Program
    {
        private const string QueueName = "<insert>";
        private const string ConnectionString = "<insert>";

        static void Main(string[] args) => Task.Run(async () =>
        {
            using var listener = CreateLogListener();
            await MainAsync();
        }).Wait();
        
        private static async Task MainAsync()
        {
            await using var client = new ServiceBusClient(ConnectionString, new ServiceBusClientOptions
            {
                RetryOptions = new ServiceBusRetryOptions()
                {
                    Mode = ServiceBusRetryMode.Exponential,
                    MaxRetries = 3,
                    Delay = TimeSpan.FromMilliseconds(100),
                    MaxDelay = TimeSpan.FromMilliseconds(500),
                    TryTimeout = TimeSpan.FromMilliseconds(1000)
                }
            });
            
            await using var receiver = client.CreateSessionProcessor(QueueName, new ServiceBusSessionProcessorOptions()
            {
                PrefetchCount = 0,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                AutoCompleteMessages = false,
                MaxAutoLockRenewalDuration = TimeSpan.FromMilliseconds(14400000),
                MaxConcurrentSessions = 1,
                MaxConcurrentCallsPerSession = 1
            });

            receiver.SessionInitializingAsync += eventArgs =>
            {
                Log($"Session({eventArgs.SessionId}) initialized and locked until {eventArgs.SessionLockedUntil:O}.");
                return Task.CompletedTask;
            };
            
            receiver.SessionClosingAsync += eventArgs =>
            {
                Log($"Session({eventArgs.SessionId}) closing.");
                return Task.CompletedTask;
            };
            
            receiver.ProcessMessageAsync += async eventArgs =>
            {
                Log($"Received message({eventArgs.Message.MessageId}).");
                await eventArgs.CompleteMessageAsync(eventArgs.Message);
            };
            
            receiver.ProcessErrorAsync += eventArgs =>
            {
                Log($"Received error({eventArgs.ErrorSource}). Exception: {eventArgs.Exception}");
                return Task.CompletedTask;
            };

            await receiver.StartProcessingAsync();
            
            while (true) { await Task.Delay(100); }
        }
        
        private static IDisposable CreateLogListener()
        {
            return new AzureEventSourceListener(
                (eventData, text) =>
                {
                    Log($"ASB: [{eventData.Level}] {eventData.EventSource.Name}: {text}");
                    Console.WriteLine();
                }, EventLevel.LogAlways);
        }

        private static void Log(string message)
        {
            Console.WriteLine($"time={DateTime.Now:O} message={message}");
        }
    }
}