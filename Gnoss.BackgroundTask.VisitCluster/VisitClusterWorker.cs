using Es.Riam.Gnoss.Elementos.Suscripcion;
using Es.Riam.Gnoss.ServicioLive;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.VisitCluster
{
    public class VisitClusterWorker : Worker
    {
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;

        public VisitClusterWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<VisitClusterWorker> logger, ILoggerFactory loggerFactory) : base(logger, scopeFactory)
        {
            _configService = configService;
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            ControladorServicioGnoss.INTERVALO_SEGUNDOS = _configService.ObtenerIntervalo();

            //Minutos que van a pasar antes de que atualice los votos, visitas y comentarios.
           
            int  visitasVotosComentarios = _configService.ObtenerVisitasVotosComentarios();

            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new Controller(ScopedFactory, _configService, mLoggerFactory.CreateLogger<Controller>(), mLoggerFactory));
            return controladores;
        }
    }
}
