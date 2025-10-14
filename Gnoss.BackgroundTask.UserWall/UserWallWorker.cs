using Es.Riam.Gnoss.Elementos.Suscripcion;
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
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;

        public UserWallWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<UserWallWorker> logger, ILoggerFactory loggerFactory)
            : base(logger, scopeFactory)
        {
            _configService = configService;
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            //Método que carga las iniciales que se van a comprobar en los diferentes hilos.

            int minPintarAgrupacionUsuariosEnProyecto = _configService.ObtenerMinutosAgruparRegistrosUsuariosEnProyecto();

            controladores.Add(new ControladorLiveUsuariosEspecifico(minPintarAgrupacionUsuariosEnProyecto, ScopedFactory, _configService, mLoggerFactory.CreateLogger<ControladorLiveUsuariosEspecifico>(), mLoggerFactory));
            return controladores;
        }
    }
}
