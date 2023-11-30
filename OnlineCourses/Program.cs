using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace Hillsdale.OnlineCourses
{
    class Program
	{
		static void Main(string[] args)
		{

			WebHost.CreateDefaultBuilder(args)
				.ConfigureAppConfiguration((hosting, conf) => { conf.AddEnvironmentVariables(); })
				.UseStartup<Startup>().Build().Run();
		}
	}
}