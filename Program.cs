using Microsoft.Extensions.Configuration;
using Serilog;
using System;

namespace ZMQDDS
{
    class Program
    {
        static void Main(string[] args)
        {
            // create config
            var config = new ConfigurationBuilder()
                .AddJsonFile("config.json")
                .Build();

            // Create logger
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(config)
                .CreateLogger();

            // Testing 
            Log.Information("Testing");
            Log.Verbose("DDS start");
            Log.Debug("DDS start debug");
            /*
            var log = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File(config["Log"])
                .CreateLogger();
            log.Information("Hello, testing serilog");           
            */
            bool connectToRoot = bool.Parse(config["DDS:ConnectToRoot"]);

            DDSIP_Port pub = new DDSIP_Port(config["DDS:Pub:IP"], int.Parse(config["DDS:Pub:Port"]), int.Parse(config["DDS:Pub:HWM"]));
            DDSIP_Port sub = new DDSIP_Port(config["DDS:Sub:IP"], int.Parse(config["DDS:Sub:Port"]), int.Parse(config["DDS:Sub:HWM"]));
            DDSIP_Port router = new DDSIP_Port(config["DDS:Router:IP"], int.Parse(config["DDS:Router:Port"]), int.Parse(config["DDS:Router:HWM"]));
            DDSIP_Port root = new DDSIP_Port(config["DDS:Root:IP"], int.Parse(config["DDS:Root:Port"]), int.Parse(config["DDS:Root:HWM"]));

            if (connectToRoot)
            {
                // start DDS with connection to root DDS
                DDS dds = new DDS();
                dds.Start(sub, router, pub, root);
            }
            else
            {
                DDS dds = new DDS();
                dds.Start(sub, router, pub);
            }

            Console.ReadLine();
        }
    }
}
