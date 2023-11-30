using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Amazon.Lambda;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Logging;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses
{


    /// <summary>
    /// Base for any API startup class. This class handles setting up JWT authentication, CORS, and ConnectionString parsing.
    /// </summary>
    public class Startup
    {
        internal static string DataRoot = "";
        internal static string DataAccessKey = "";
        internal static string DiscourseSsoKey = "";
        internal static string DiscourseSsoUrl = "";
        internal static string NotesKey = "";
        internal static string EarlyAccessToken = "";
        internal static string MergeAccountFunction = "";
        internal static string VimeoAccessToken = "";

		internal static class CachePrefix
		{
			internal static string VIMEO_VIDEO = "VimeoVideo_";
		}

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
            DataRoot = configuration["Kentico:Root"];
            DataAccessKey = configuration["Kentico:Key"];
            DiscourseSsoKey = configuration["Discourse:SsoKey"];
            DiscourseSsoUrl = configuration["Discourse:SsoUrl"];

            VimeoAccessToken = configuration["Vimeo:AccessToken"];

            NotesKey = configuration["Notes:Key"];
            if (string.IsNullOrWhiteSpace(NotesKey)) Console.WriteLine("\n\n\nERROR: Must supply Notes Key\n\n\n");

            EarlyAccessToken = configuration["EarlyAccessToken"] ?? "jr2XL52sIZ8bRR6GGYAP";
            MergeAccountFunction = configuration["MergeAccountFunctionName"];
        }

        protected IConfiguration Configuration { get; }

        public virtual void ConfigureServices(IServiceCollection services)
        {
            IdentityModelEventSource.ShowPII = true;

            services.AddCors();

            services.Configure<ConnectionStringConfig>(
                this.Configuration.GetSection("connectionStrings")
            );

            services.Configure<HubspotCourseEnrollment>(Configuration.GetSection("Hubspot"));

            services.AddDefaultAWSOptions(Configuration.GetAWSOptions());
            services.AddAWSService<IAmazonLambda>();

            var validIssuer =
                $"https://cognito-idp.{Configuration["Cognito:Region"]}.amazonaws.com/{Configuration["Cognito:PoolId"]}";

            CognitoKeyRetriever.Start(validIssuer, null);

            //add Cognito keys
            services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
                .AddJwtBearer(options =>
                {
                    options.TokenValidationParameters = new TokenValidationParameters
                    {
                        IssuerSigningKeyResolver = (s, securityToken, identifier, parameters) => CognitoKeyRetriever.GetKeys(),

                        ValidIssuer = validIssuer,
                        ValidateIssuerSigningKey = true,
                        ValidateIssuer = true,
                        ValidateLifetime = true,
                        //TODO update to validate client_id claim
//                        AudienceValidator = (audiences, token, parameters) => audiences.
//                        ValidAudience = Configuration["Cognito:ClientId"],
                        ValidateAudience = false
                    };
                });

			services.AddMemoryCache();
			services.AddControllers().AddNewtonsoftJson();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseMiddleware<RequestLoggingMiddleware>();

            var allowedOrigins =
                Configuration.GetSection("AllowedOrigins").GetChildren().Select(c => c.Value).ToArray();

            app.UseRouting();
            app.UseCors(options => options.WithOrigins(allowedOrigins)
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials()
            );

            app.UseAuthentication();
			app.UseAuthorization();

			app.UseEndpoints(endpoints => {
				endpoints.MapControllers();
			});
        }
    }

    //based on https://stackoverflow.com/a/49195664
    internal class CognitoKeyRetriever
    {
        private const int ReloadTimerPeriodMinutes = 10 * 60 * 1000; //set reload for every 10 minutes

        private static IList<JsonWebKey> _keys = new List<JsonWebKey>();
        private static Timer _reloadTimer = null;

        private static readonly object KeySync = new object();
        public static void Start(string issuerUri, ILogger logger = null)
        {
            logger?.LogInformation("Started CognitoKeyRetriever");
            // Start a new timer to reload all the security keys every RELOAD_TIMER_PERIOD_MINUTES.
            if (_reloadTimer == null)
            {
                _reloadTimer = new Timer( (t) =>
                {
                    try
                    {
                        logger?.LogInformation("Pulling latest approved client symmetric keys for JWT signature validation");

                        lock (KeySync)
                        {
                            logger?.LogInformation("downloading Cognito signing keys...");
                            // get JsonWebKeySet from AWS
                            var json = new WebClient().DownloadString(issuerUri + "/.well-known/jwks.json");
                            // deserialize the result
                            var dlKeys = JsonConvert.DeserializeObject<JsonWebKeySet>(json).Keys;
                            _keys = dlKeys;
                        }

                        logger?.LogInformation("Reloaded security keys");
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Error getting current security keys");
                    }
                }, null, 0, ReloadTimerPeriodMinutes);
            }
        }

        public static void Stop()
        {
            if (_reloadTimer != null)
            {
                _reloadTimer.Dispose();
                _reloadTimer = null;
            }
        }

        public static IEnumerable<JsonWebKey> GetKeys()
        {
            lock (KeySync)
            {
                return _keys;
            }
        }
    }

   ///
   /// Represents the available connection strings from appsettings.json.
   /// Not all properties need to be present in the json file. The API will simply fill out the ones that are present.
   ///
    public class ConnectionStringConfig
    {
        public string OnlineCourse { get; set; }
    }
}
