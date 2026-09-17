using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.CL.RelatedVirtuoso;
using Es.Riam.Gnoss.HealthChecks;
using Es.Riam.Gnoss.RabbitMQ;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Util.Seguridad;
using Es.Riam.Interfaces.InterfacesOpen;
using Es.Riam.Open;
using Es.Riam.OpenReplication;
using Es.Riam.Util;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using System;
using System.Collections;

namespace Gnoss.BackgroundTask.VisitCluster
{
    public class Program
    {
        private static Serilog.ILogger _startupLogger;
        public static void Main(string[] args)
        {
            _startupLogger = LoggingService.ConfigurarBasicStartupSerilog().CreateBootstrapLogger().ForContext<Program>();
            try
            {
                CreateHostBuilder(args).Build().Run();
            }
            catch (Exception ex)
            {
                _startupLogger.Fatal(ex, "Error fatal durante el arranque");
            }
            finally
            {
                (_startupLogger as IDisposable)?.Dispose();
                Log.CloseAndFlush(); // asegura que se escriben todos los logs pendientes
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .UseWindowsService() //Windows
                .UseSystemd() //Linux
                .ConfigureAppConfiguration((hostContext, config) =>
                {
                    LoggingService.ConfigurarSeguimientoFicheros(hostContext, config, _startupLogger);
                })
                .UseSerilog((context, services, configuration) => LoggingService.ConfigurarSerilog(context.Configuration, services, configuration))
                .ConfigureServices((hostContext, services) =>
                {
                    LoggingService.SuscribirCambios(hostContext, _startupLogger);
                    _startupLogger.Information("Suscripción a cambios de configuración registrada");

                    IConfiguration configuration = hostContext.Configuration;

                    AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
                    services.AddScoped(typeof(Usuario));
                    services.AddScoped(typeof(UtilPeticion));

                    services.AddSingleton(typeof(RedisCacheWrapper));
                    services.AddScoped(typeof(UtilidadesVirtuoso));
                    services.AddScoped(typeof(VirtuosoAD));
                    services.AddScoped(typeof(LoggingService));
                    services.AddScoped(typeof(GnossCache));
                    services.AddScoped<IServicesUtilVirtuosoAndReplication, ServicesVirtuosoAndBidirectionalReplicationOpen>();
                    services.AddScoped(typeof(RelatedVirtuosoCL));
                    services.AddScoped<IAvailableServices, AvailableServicesOpen>();
                    string bdType = "";
                    IDictionary environmentVariables = Environment.GetEnvironmentVariables();
                    if (environmentVariables.Contains("connectionType"))
                    {
                        bdType = environmentVariables["connectionType"] as string;
                    }
                    else
                    {
                        bdType = configuration.GetConnectionString("connectionType");
                    }
                    if (bdType.Equals("2") || bdType.Equals("1"))
                    {
                        services.AddScoped(typeof(DbContextOptions<EntityContext>));
                        services.AddScoped(typeof(DbContextOptions<EntityContextBASE>));
                    }
                    services.AddSingleton<ConfigService>();

                    if (bdType.Equals("0"))
                    {
                        services.AddDbContext<EntityContext>();
                        services.AddDbContext<EntityContextBASE>();
                    }
                    else if (bdType.Equals("1"))
                    {
                        services.AddDbContext<EntityContext, EntityContextOracle>();
                        services.AddDbContext<EntityContextBASE, EntityContextBASEOracle>();
                    }
                    else if (bdType.Equals("2"))
                    {
                        services.AddDbContext<EntityContext, EntityContextPostgres>();
                        services.AddDbContext<EntityContextBASE, EntityContextBASEPostgres>();
                    }
                    var hcConfigService = new ConfigService();
                    services.AddHealthChecks()
                        .AddGnossDatabaseHealthCheck<EntityContext>(bdType, hcConfigService.ObtenerSqlConnectionString())
                        .AddGnossRedisHealthCheck(hcConfigService.ObtenerConexionRedisIPMaster("redis"))
                        .AddGnossVirtuosoHealthCheck(hcConfigService.ObtenerVirtuosoConnectionString().ConnectionString)
                        .AddGnossRabbitMQHealthCheck(hcConfigService.ObtenerRabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN));

                    services.AddHostedService<VisitClusterWorker>();
                })
#if !DEBUG
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureKestrel((ctx, options) =>
                        options.ListenAnyIP(ctx.Configuration.GetValue("ManagementPort", 8081)));
                    webBuilder.Configure(app =>
                    {
                        var managementPort = app.ApplicationServices
                            .GetRequiredService<IConfiguration>()
                            .GetValue("ManagementPort", 8081);
                        app.UseRouting();
                        app.UseEndpoints(endpoints => endpoints.MapGnossHealthEndpoints(managementPort));
                    });
                })
#endif
                ;
    }
}
