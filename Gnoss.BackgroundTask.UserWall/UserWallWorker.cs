using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Win.ServicioLiveUsuariosEspecifico;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.UserWall
{
    public class UserWallWorker : Worker
    {
        private readonly ILogger<UserWallWorker> _logger;
        private readonly ConfigService _configService;

        public UserWallWorker(ILogger<UserWallWorker> logger, ConfigService configService, IServiceScopeFactory scopeFactory)
            : base(logger, scopeFactory)
        {
            _logger = logger;
            _configService = configService;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            //Método que carga las iniciales que se van a comprobar en los diferentes hilos.

            int minPintarAgrupacionUsuariosEnProyecto = _configService.ObtenerMinutosAgruparRegistrosUsuariosEnProyecto();

            controladores.Add(new ControladorLiveUsuariosEspecifico(minPintarAgrupacionUsuariosEnProyecto, ScopedFactory, _configService));
            return controladores;
        }
    }
}
