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
        private readonly ILogger<VisitClusterWorker> _logger;
        private readonly ConfigService _configService;

        public VisitClusterWorker(ILogger<VisitClusterWorker> logger, ConfigService configService, IServiceScopeFactory scopeFactory) : base(logger, scopeFactory)
        {
            _logger = logger;
            _configService = configService;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            ControladorServicioGnoss.INTERVALO_SEGUNDOS = _configService.ObtenerIntervalo();

            //Minutos que van a pasar antes de que atualice los votos, visitas y comentarios.
           
            int  visitasVotosComentarios = _configService.ObtenerVisitasVotosComentarios();

            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new Controller(ScopedFactory, _configService));
            return controladores;
        }
    }
}
