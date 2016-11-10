using Newtonsoft.Json;
using RethinkDb.Driver;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace doc_stack_persistor
{
    public class Program
    {
        public static RethinkDB R = RethinkDB.R;

        public static int Main(string[] args)
        {
            const string rethinkHost = "localhost";
            const string redisHost = "localhost";
            const string dbName = "doc_stack_db";
            const string tableName = "documents";

            //indexes
            const string name_client_user_index = "name_client_user";

            var ip = GetIp(rethinkHost);

            var c = R.Connection()
             .Hostname(ip)
             .Port(RethinkDBConstants.DefaultPort)
             .Timeout(60)
             .Connect();

            List<string> dbs = R.DbList().Run<List<string>>(c);
            if (!dbs.Contains(dbName))
            {
                R.DbCreate(dbName).Run(c);
            }
            var db = R.Db(dbName);

            List<string> tables = db.TableList().Run<List<string>>(c);
            if (!tables.Contains(tableName))
            {
                db.TableCreate(tableName).Run(c);
            }
            var documentTable = db.Table(tableName);

            List<string> indexes = documentTable.IndexList().Run(c);
            if (!indexes.Contains(name_client_user_index))
            {
                RethinkDb.Driver.Ast.ReqlFunction1 pathIx = row => { return R.Array(row["name"], row["client"], row["user"]); };
                documentTable.IndexCreate("name_client_user", pathIx).Run(c);
                documentTable.IndexWait("name_client_user").Run(c);
            }

            try
            {
                var redis = OpenRedisConnection(redisHost).GetDatabase();

                var definition = new { id = "", name = "", size = 0, user = "", client = "", content = "" };

                while (true)
                {
                    
                    string json = redis.ListLeftPopAsync("documents:process:0").Result;
                    if (json != null)
                    {
                        var document = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Document '{document.name}/{document.id}/{document.user}/{document.client}' will be persisted");
                        var now = DateTime.UtcNow;
                        var toSave = new { id = document.id, user=document.user, client=document.client, name=document.name, document = document, inserted = now, updated = now, state = new[] { "persisted" } };

                        //first check if the document maybe already exists - right now we will just override
                        var x = new[] { "ocr-test2.png", "dummyClient", "dummyUser" };
                        //documentTable.GetAll(x).OptArg("index", "name_client_user");
                        //
                        documentTable.Insert(toSave).Run(c);
                    }
                    else
                    {
                        Thread.Sleep(500);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }

        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // Use IP address to workaround hhttps://github.com/StackExchange/StackExchange.Redis/issues/410
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connected to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();
    }
}
