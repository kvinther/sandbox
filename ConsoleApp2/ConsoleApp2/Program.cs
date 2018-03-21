using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using StackExchange;
using StackExchange.Redis;

namespace ConsoleApp2
{
    class Program
    {
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            var options = ConfigurationOptions.Parse("localhost:6379,allowAdmin=true");
            return ConnectionMultiplexer.Connect(options);
        });

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }
        static void Main(string[] args)
        {
            var cache = Connection.GetDatabase();
            var server = Connection.GetServer("localhost:6379");
            var exit = false;
            do
            {
                Console.Clear();
                var keys = server.Keys();
                foreach (var key in keys)
                {
                    var value = cache.StringGet(key);
                    Console.WriteLine($"kv:{key}:{value}");
                }

                var consoleKey = Console.ReadKey(true);
                if (consoleKey.Key != ConsoleKey.Spacebar)
                    exit = true;
            } while (!exit);


        }
    }
}
