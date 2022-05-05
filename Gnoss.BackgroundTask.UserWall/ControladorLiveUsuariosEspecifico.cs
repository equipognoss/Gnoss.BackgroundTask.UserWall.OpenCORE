using System;
using System.Web;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.IO;
using System.Globalization;
using System.Reflection;
using System.Data;
using Es.Riam.Util;
using System.Xml;
using Es.Riam.Gnoss.Util.General;
using System.Net;
using Es.Riam.Gnoss.Logica.Live;
using Es.Riam.Gnoss.AD.Live.Model;
using Es.Riam.Gnoss.AD.Live;
using Es.Riam.Gnoss.Logica.Identidad;
using Es.Riam.Gnoss.Logica.Usuarios;
//using Es.Riam.Gnoss.CL.Identidad;
using Es.Riam.Gnoss.CL.Live;
using System.ServiceModel;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.CL.Facetado;
using Es.Riam.Gnoss.Logica.Suscripcion;
using Es.Riam.Gnoss.CL.Documentacion;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.Logica.Parametro;
using System.Linq;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.RabbitMQ;
using Newtonsoft.Json;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.Win.ServicioLiveUsuariosEspecifico
{
    internal class ControladorLiveUsuariosEspecifico : ControladorServicioGnoss
    {
        #region Constantes

        private const string COLA_USUARIOS_ESPECIFICO = "ColaUsuariosEspecifico";
        private const string EXCHANGE = "";

        #endregion

        #region Miembros

        /// <summary>
        /// Almacena el último Score que se ha asginado a cada perfil de usuario
        /// </summary>
        private Dictionary<Guid, int> mListaScorePorPerfil = new Dictionary<Guid, int>();

        private Dictionary<string, int> mListaScorePorProyUsuSuscr = new Dictionary<string, int>();

        private Dictionary<string, int> mListaScorePorUsuSuscr = new Dictionary<string, int>();

        private Guid mElementoID;

        private int mMinutosEntrePintadoAgrupacionNuevosRegistros;

        #endregion

        #region Constructores

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pFicheroConfiguracionSitioWeb">Ruta al archivo de configuración del sitio Web</param>
        public ControladorLiveUsuariosEspecifico(int pMinutosPintarAgrupacionNuevosRegistros, IServiceScopeFactory scopedFactory, ConfigService configService)
            : base(scopedFactory, configService)
        {
            mMinutosEntrePintadoAgrupacionNuevosRegistros = pMinutosPintarAgrupacionNuevosRegistros;
        }

        #endregion

        #region Métodos generales

        private void EstablecerDominioCache(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            bool correcto = false;
            while (!correcto)
            {
                try
                {
                    ParametroAplicacionCN parametroApliCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
                    ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
                    parametroAplicacionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);

                    //mDominio = gestorParametroAplicacion.ParametroAplicacion.Select("Parametro='UrlIntragnoss'")[0].Valor;
                    mDominio = gestorParametroAplicacion.ParametroAplicacion.Find(parametroApp=>parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;
                    mDominio = mDominio.Replace("http://", "").Replace("www.", "");

                    if (mDominio[mDominio.Length - 1] == '/')
                    {
                        mDominio = mDominio.Substring(0, mDominio.Length - 1);
                    }
                    correcto = true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog(loggingService.DevolverCadenaError(ex, "1.0"));
                    Thread.Sleep(1000);
                }
            }
        }

        private void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_USUARIOS_ESPECIFICO, loggingService, mConfigService, EXCHANGE, COLA_USUARIOS_ESPECIFICO);

                try
                {
                    rabbitMQClient.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarLecturaRabbit = false;
                }
                catch (Exception ex)
                {
                    mReiniciarLecturaRabbit = true;
                    loggingService.GuardarLogError(ex);
                }
            }
        }

        private bool ProcesarItem(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                try
                {
                    ComprobarCancelacionHilo();

                    System.Diagnostics.Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        LiveUsuariosDS.ColaUsuariosRow filaCola = (LiveUsuariosDS.ColaUsuariosRow)new LiveUsuariosDS().ColaUsuarios.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilasDeColaUsuariosEspecifico(filaCola, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);

                        filaCola = null;

                        servicesUtilVirtuosoAndReplication.ConexionAfinidad = "";

                        ControladorConexiones.CerrarConexiones(false);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return true;
                }
            }
        }        

        private void ProcesarFilasDeColaUsuariosEspecifico(LiveUsuariosDS.ColaUsuariosRow pFilaColaUsuario, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ComprobarCancelacionHilo();
            try
            {
                ProcesarFila(pFilaColaUsuario, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
                pFilaColaUsuario.NumIntentos = 7;
            }
            catch (Exception e)
            {
                loggingService.GuardarLogError(e);
            }
        }

        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            try
            {
                //UtilTrazas.TrazaHabilitada = true;

                EstablecerDominioCache(entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                BaseCL.mLanzarExcepciones = true;
                BaseCL.mUsarHilos = false;

                RealizarMantenimientoRabbitMQ(loggingService);
                //RealizarMantenimientoBD();
            }
            catch (Exception ex)
            {
                loggingService.GuardarLog(loggingService.DevolverCadenaError(ex, "1.0"));
            }
        }


        private List<Guid> ObtenerProyectosQueAgrupanEventosRegistroHome(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ParametroCN paramCN = new ParametroCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            List<Guid> listaProyectosConfigurados = paramCN.ObtenerProyectosQueAgrupanEventosRegistroHome();

            return listaProyectosConfigurados;
        }

        /// <summary>
        /// Actualizamos la actividad reciente de la home del usuario conectado
        /// </summary>
        private void ActualizarLivePerfilUsuario(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pUsuarioID, Guid pPerfilID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            AccionLive accion = (AccionLive)pFilaCola.Accion;

            if (accion.Equals(AccionLive.Eliminado))
            {
                pLiveUsuariosCL.EliminarLivePerfilUsuario(pUsuarioID, pPerfilID, pNombreCacheElemento);
            }
            else if (!accion.Equals(AccionLive.ReprocesarEventoHomeProyecto))
            {
                mListaScorePorPerfil[pPerfilID] = pLiveUsuariosCL.AgregarLivePerfilUsuario(pUsuarioID, pPerfilID, pNombreCacheElemento, ObtenerUltimoScorePerfil(pPerfilID));

                //cuantos elementos hay? > 100 eliminamos
                int num = pLiveUsuariosCL.ObtenerNumElementosPerfilUsuario(pUsuarioID, pPerfilID);
                if (num > 100)
                {
                    int fin = num - 100;
                    pLiveUsuariosCL.EliminaElementosPerfilUsuario(pUsuarioID, pPerfilID, 0, fin - 1);
                }
            }
        }

        /// <summary>
        /// Eliminamos la actividad reciente de la home del usuario conectado
        /// </summary>
        private void EliminarLivePerfilUsuario(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pUsuarioID, Guid pPerfilID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            pLiveUsuariosCL.EliminarLivePerfilUsuario(pUsuarioID, pPerfilID, pNombreCacheElemento);
        }

        /// <summary>
        /// Actualizamos la actividad reciente de las suscripciones ( suscripciones por proyecto )
        /// </summary>
        private void ActualizarLiveProyectoUsuarioSuscripciones(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pUsuarioID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            string claveProyUsuSuscr = "suscripciones_" + pFilaCola.ProyectoId.ToString() + pUsuarioID.ToString();

            AccionLive accion = (AccionLive)pFilaCola.Accion;

            if (accion.Equals(AccionLive.Eliminado))
            {
                pLiveUsuariosCL.EliminarLiveProyectoUsuarioSuscripciones(pUsuarioID, pFilaCola.ProyectoId, pNombreCacheElemento);
            }
            else if (!accion.Equals(AccionLive.ReprocesarEventoHomeProyecto))
            {
                mListaScorePorProyUsuSuscr[claveProyUsuSuscr] = pLiveUsuariosCL.AgregarLiveProyectoUsuarioSuscripciones(pUsuarioID, pFilaCola.ProyectoId, pNombreCacheElemento, ObtenerUltimoScoreProyectoUsuarioSuscripciones(pFilaCola.ProyectoId, pUsuarioID));

                //cuantos elementos hay? > 100 eliminamos
                int num = pLiveUsuariosCL.ObtenerNumElementosProyectoUsuarioSuscripciones(pUsuarioID, pFilaCola.ProyectoId);
                if (num > 100)
                {
                    int fin = num - 100;
                    pLiveUsuariosCL.EliminaElementosProyectoUsuarioSuscripciones(pUsuarioID, pFilaCola.ProyectoId, 0, fin - 1);
                }
            }
            else if (accion.Equals(AccionLive.Eliminado))
            {
                pLiveUsuariosCL.EliminarLiveProyectoUsuarioSuscripciones(pUsuarioID, pFilaCola.ProyectoId, pNombreCacheElemento);
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente de las suscripciones (suscripciones en ecosistema )
        /// </summary>
        private void ActualizarLiveUsuarioSuscripciones(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pUsuarioID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            string claveProyUsuSuscr = "suscripcionesUsu_" + pUsuarioID.ToString();

            AccionLive accion = (AccionLive)pFilaCola.Accion;

            if (accion.Equals(AccionLive.Eliminado))
            {
                pLiveUsuariosCL.EliminarLiveUsuarioSuscripciones(pUsuarioID, pNombreCacheElemento);
            }
            else if (!accion.Equals(AccionLive.ReprocesarEventoHomeProyecto))
            {
                mListaScorePorUsuSuscr[claveProyUsuSuscr] = pLiveUsuariosCL.AgregarLiveUsuarioSuscripciones(pUsuarioID, pNombreCacheElemento, ObtenerUltimoScoreUsuarioSuscripciones(pUsuarioID));

                //cuantos elementos hay? > 100 eliminamos
                int num = pLiveUsuariosCL.ObtenerNumElementosUsuarioSuscripciones(pUsuarioID);
                if (num > 100)
                {
                    int fin = num - 100;
                    pLiveUsuariosCL.EliminaElementosUsuarioSuscripciones(pUsuarioID, 0, fin - 1);
                }
            }
            else if (accion.Equals(AccionLive.Eliminado))
            {
                pLiveUsuariosCL.EliminarLiveUsuarioSuscripciones(pUsuarioID, pNombreCacheElemento);
            }
        }

        /// <summary>
        /// Eliminamos la actividad reciente de las suscripciones
        /// </summary>
        private void EliminarLiveProyectoUsuarioSuscripciones(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pUsuarioID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            pLiveUsuariosCL.EliminarLiveProyectoUsuarioSuscripciones(pUsuarioID, pFilaCola.ProyectoId, pNombreCacheElemento);

            pLiveUsuariosCL.EliminarLiveUsuarioSuscripciones(pUsuarioID, pNombreCacheElemento);
        }

        private void ProcesarFila(LiveUsuariosDS.ColaUsuariosRow pFilaCola, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            EstadoProyecto estado = proyCN.ObtenerEstadoProyecto(pFilaCola.ProyectoId);
            TipoAcceso tipoAcceso = proyCN.ObtenerTipoAccesoProyecto(pFilaCola.ProyectoId);

            if (estado != EstadoProyecto.Abierto)
            {
                //Si el proyecto no está abierto no notifico nada
                return;
            }
            GestorParametroAplicacionDS = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBD.ObtenerConfiguracionGnoss(GestorParametroAplicacionDS);
            mUrlIntragnoss = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroAplicacion => parametroAplicacion.Parametro.Equals("UrlIntragnoss")).FirstOrDefault().Valor;
            //GestorParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'UrlIntragnoss'")[0]["Valor"].ToString()
            FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            DocumentacionCL documentacionCL = new DocumentacionCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            documentacionCL.Dominio = mDominio;

            ObtenerIDElementoPrincipal(pFilaCola, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            AccionLive accion = (AccionLive)pFilaCola.Accion;

            //Si la accion es un comentario editado o eliminado, no actualizamos las colas
            bool comentarioEditadoOEliminado = accion.Equals(AccionLive.ComentarioEliminado) || accion.Equals(AccionLive.ComentarioEditado);

            //Si la accion es editado y no se ha cambiado la privacidad, no actualizamos las colas
            bool editadoSinCambiarPrivacidad = accion.Equals(AccionLive.Editado) && !pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA);

            if (comentarioEditadoOEliminado || editadoSinCambiarPrivacidad)
            {
                return;
            }

            string infoExtra = "";
            if (!pFilaCola.IsInfoExtraNull() && pFilaCola.InfoExtra != "didactalia" && !pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA))
            {
                infoExtra = "_" + pFilaCola.InfoExtra;
            }

            string nombreCacheElemento = pFilaCola.Tipo + "_" + mElementoID + "_" + pFilaCola.ProyectoId;
            if (!pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA))
            {
                nombreCacheElemento += infoExtra;
            }

            if (pFilaCola.Tipo.Equals((int)TipoLive.AgrupacionNuevosMiembros))
            {
                nombreCacheElemento = pFilaCola.Tipo + "_" + pFilaCola.ProyectoId;
            }

            LiveUsuariosCL liveUsuariosCL = new LiveUsuariosCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            liveUsuariosCL.Dominio = mDominio;

            bool esComunidadPrivada = tipoAcceso.Equals(TipoAcceso.Privado) || tipoAcceso.Equals(TipoAcceso.Reservado);

            bool esTipoDocumento = ((pFilaCola.Tipo == (int)TipoLive.Recurso) || (pFilaCola.Tipo == (int)TipoLive.Pregunta) || (pFilaCola.Tipo == (int)TipoLive.Debate));
            bool esTipoMiembro = ((pFilaCola.Tipo == (int)TipoLive.Miembro) || (pFilaCola.Tipo == (int)TipoLive.AgrupacionNuevosMiembros));

            if (esTipoDocumento)
            {
                DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                bool borrador = docCN.EsDocumentoBorrador(mElementoID);

                if (borrador)
                {
                    // No hay que procesar los borradores.
                    return;
                }           
                
                //Obtenemos la identidad del creador del evento del recurso, para evitar que llenes la home con tus propios recursos
                Guid? perfilPublicadorID = ObtenerIdentidadCreadorEventoRecurso(pFilaCola, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //Obtenemos la privacidad del recurso
                bool recursoPrivado = docCN.EsDocumentoEnProyectoPrivadoEditores(mElementoID, pFilaCola.ProyectoId);
                docCN.Dispose();

                bool privacidadCambiada = pFilaCola.Accion == (int)AccionLive.Editado && pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA);

                ParametroAplicacion parametroAplicacion = GestorParametroAplicacionDS.ParametroAplicacion.Find(parametroApp=>parametroApp.Parametro.Equals("EcosistemaSinHomeUsuario"));
                if (parametroAplicacion == null || parametroAplicacion.Valor == "false")
                {
                    AgregarEventoLiveRecurso(pFilaCola, liveUsuariosCL, nombreCacheElemento, perfilPublicadorID, esComunidadPrivada, recursoPrivado, privacidadCambiada, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }

                //Si la accion es de un comentario, un voto o un recurso editado, no actualizamos las suscripciones.
                bool esComentario = accion.Equals(AccionLive.ComentarioAgregado);
                bool esVoto = accion.Equals(AccionLive.Votado);
                bool esRecursoEditado = accion.Equals(AccionLive.Editado);

                if (!esComentario && !esVoto && (!esRecursoEditado || privacidadCambiada))
                {
                    AgregarEventoLiveRecursoSuscripciones(pFilaCola, liveUsuariosCL, nombreCacheElemento, perfilPublicadorID, esComunidadPrivada, recursoPrivado, privacidadCambiada, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
            }
            else if (esTipoMiembro && !pFilaCola.ProyectoId.Equals(ProyectoAD.MetaProyecto))
            {
                ParametroAplicacion parametroAplicacionBusqueda = GestorParametroAplicacionDS.ParametroAplicacion.Find(parametroApp=>parametroApp.Parametro.Equals("EcosistemaSinHomeUsuario"));
                if (parametroAplicacionBusqueda == null ||parametroAplicacionBusqueda.Valor == "false")
                {
                    AgregarEventoLiveMiembro(pFilaCola, liveUsuariosCL, nombreCacheElemento, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
            }
        }


        private void AgregarEventoLiveMiembro(LiveUsuariosDS.ColaUsuariosRow pFilaCola, LiveUsuariosCL liveUsuariosCL, string nombreCacheElemento, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            AccionLive accion = (AccionLive)pFilaCola.Accion;

            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            TipoAcceso tipoAcceso = proyCN.ObtenerTipoAccesoProyecto(pFilaCola.ProyectoId);
            bool esPublica = (tipoAcceso.Equals(TipoAcceso.Publico) || tipoAcceso.Equals(TipoAcceso.Restringido));
            proyCN.Dispose();

            if ((accion.Equals(AccionLive.Agregado)) && !pFilaCola.ProyectoId.Equals(ProyectoAD.MetaProyecto) && !pFilaCola.ProyectoId.Equals(ProyectoAD.ProyectoFAQ) && !pFilaCola.ProyectoId.Equals(ProyectoAD.ProyectoNoticias) && (!pFilaCola.ProyectoId.Equals(ProyectoAD.ProyectoDidactalia) || !string.IsNullOrEmpty(pFilaCola.InfoExtra)))
            {
                UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                Guid? usuarioMiembroID = usuarioCN.ObtenerUsuarioIDPorIDPerfil(pFilaCola.Id);
                Guid? perfilMiembroID = null;

                if (esPublica && !((TipoLive)pFilaCola.Tipo).Equals(TipoLive.AgrupacionNuevosMiembros))
                {
                    // A mis contactos solo se notifican las acciones de comunidades de acceso restringido y públicas
                    perfilMiembroID = pFilaCola.Id;
                }

                //Obtenemos la lista de los perfiles que se ven afectados por el evento
                Dictionary<Guid, Guid> listaUsuariosAfectadosEvento = ObtenerListaUsuariosAfectados(pFilaCola, perfilMiembroID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                usuarioCN.Dispose();

                foreach (Guid perfilID in listaUsuariosAfectadosEvento.Keys)
                {
                    Guid usuarioID = listaUsuariosAfectadosEvento[perfilID];

                    //Actualizamos la Actividad Reciente de la home del usuario
                    ActualizarLivePerfilUsuario(pFilaCola, usuarioID, perfilID, liveUsuariosCL, nombreCacheElemento);
                }
            }
        }

        private Guid? ObtenerIdentidadCreadorEventoRecurso(LiveUsuariosDS.ColaUsuariosRow pFilaCola, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Guid? perfilPublicadorID = null;

            if ((AccionLive)pFilaCola.Accion == AccionLive.Agregado || (AccionLive)pFilaCola.Accion == AccionLive.Editado || (AccionLive)pFilaCola.Accion == AccionLive.Eliminado)
            {
                IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                perfilPublicadorID = identCN.ObtenerPerfilIDPublicadorRecursoEnProyecto(mElementoID, pFilaCola.ProyectoId);
                identCN.Dispose();
            }
            //else if ((AccionLive)pFilaCola.Accion == AccionLive.ComentarioAgregado)
            //{
            //    IdentidadCN identCN = new IdentidadCN();
            //    perfilPublicadorID = identCN.ObtenerPerfilIDPublicadorComentarioEnRecurso(pFilaCola.Id);
            //    identCN.Dispose();
            //}
            //else if ((AccionLive)pFilaCola.Accion == AccionLive.Votado)
            //{
            //    IdentidadCN identCN = new IdentidadCN();
            //    perfilPublicadorID = identCN.ObtenerPerfilIDPublicadorVotoEnRecurso(pFilaCola.Id);
            //    identCN.Dispose();
            //}            

            return perfilPublicadorID;
        }


        private void AgregarEventoLiveRecurso(LiveUsuariosDS.ColaUsuariosRow pFilaCola, LiveUsuariosCL liveUsuariosCL, string nombreCacheElemento, Guid? perfilPublicadorID, bool esComunidadPrivada, bool recursoPrivado, bool privacidadCambiada, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Obtenemos la lista de los perfiles que se ven afectados por el evento
            Dictionary<Guid, Guid> listaUsuariosAfectadosEvento = ObtenerListaUsuariosAfectados(pFilaCola, perfilPublicadorID, entityContext, loggingService, servicesUtilVirtuosoAndReplication, esComunidadPrivada, recursoPrivado, privacidadCambiada);

            if (!recursoPrivado)
            {
                foreach (Guid perfilID in listaUsuariosAfectadosEvento.Keys)
                {
                    Guid usuarioID = listaUsuariosAfectadosEvento[perfilID];

                    //Actualizamos la Actividad Reciente de la home del usuario
                    ActualizarLivePerfilUsuario(pFilaCola, usuarioID, perfilID, liveUsuariosCL, nombreCacheElemento);
                }
            }
            else
            {
                if (privacidadCambiada)
                {
                    //Si la accion es un cambio de privacidad. Tenemos cargados los usuarios que no son editores.

                    //Eliminamos la fila del live de la home de todos los usuarios que no son editores
                    foreach (Guid perfilID in listaUsuariosAfectadosEvento.Keys)
                    {
                        Guid usuarioID = listaUsuariosAfectadosEvento[perfilID];

                        EliminarLivePerfilUsuario(pFilaCola, usuarioID, perfilID, liveUsuariosCL, nombreCacheElemento);
                    }
                }
                else
                {
                    foreach (Guid perfilID in listaUsuariosAfectadosEvento.Keys)
                    {
                        Guid usuarioID = listaUsuariosAfectadosEvento[perfilID];

                        ActualizarLivePerfilUsuario(pFilaCola, usuarioID, perfilID, liveUsuariosCL, nombreCacheElemento);
                    }
                }
            }
            liveUsuariosCL.Dispose();
        }

        private void AgregarEventoLiveRecursoSuscripciones(LiveUsuariosDS.ColaUsuariosRow pFilaCola, LiveUsuariosCL liveUsuariosCL, string nombreCacheElemento, Guid? perfilPublicadorID, bool esComunidadPrivada, bool recursoPrivado, bool privacidadCambiada, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Obtenemos la lista de los perfiles que estan suscritos a alguna categoria o al publicador
            Dictionary<Guid, Guid> listaUsuariosYPerfilesSuscritos = ObtenerListaUsuariosSuscritos(pFilaCola, perfilPublicadorID, !esComunidadPrivada, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            if (!recursoPrivado)
            {
                foreach (Guid perfilID in listaUsuariosYPerfilesSuscritos.Keys)
                {
                    Guid usuarioID = listaUsuariosYPerfilesSuscritos[perfilID];

                    //Agregamos la fila a las suscripciones
                    AgregarLiveSuscripciones(pFilaCola, perfilID, usuarioID, liveUsuariosCL, nombreCacheElemento, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
            }
            else
            {
                if (privacidadCambiada)
                {
                    //Si la accion es un cambio de privacidad. Tenemos cargados los usuarios que no son editores.

                    //Eliminamos la fila del live de suscripciones de todos los usuarios suscritos que no son editores
                    foreach (Guid perfilID in listaUsuariosYPerfilesSuscritos.Keys)
                    {
                        //Si se ha cambiado la privacidad, eliminamos la fila a todos menos a los editores y lectores.
                        if (listaUsuariosYPerfilesSuscritos.ContainsKey(perfilID))
                        {
                            Guid usuarioID = listaUsuariosYPerfilesSuscritos[perfilID];

                            EliminarLiveProyectoUsuarioSuscripciones(pFilaCola, usuarioID, liveUsuariosCL, nombreCacheElemento);
                        }
                    }
                }
                else
                {
                    //Obtenemos la lista de los perfiles que se ven afectados por el evento
                    Dictionary<Guid, Guid> listaUsuariosAfectadosEvento = ObtenerListaUsuariosAfectados(pFilaCola, perfilPublicadorID, entityContext, loggingService, servicesUtilVirtuosoAndReplication, esComunidadPrivada, recursoPrivado, privacidadCambiada);

                    foreach (Guid perfilID in listaUsuariosYPerfilesSuscritos.Keys)
                    {
                        Guid usuarioID = listaUsuariosYPerfilesSuscritos[perfilID];

                        bool estaSuscrito = listaUsuariosYPerfilesSuscritos.ContainsKey(perfilID);
                        bool afectadoPorEvento = listaUsuariosAfectadosEvento.ContainsKey(perfilID);

                        if (estaSuscrito && afectadoPorEvento)
                        {
                            AgregarLiveSuscripciones(pFilaCola, perfilID, usuarioID, liveUsuariosCL, nombreCacheElemento, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                        }
                    }
                }
            }
            liveUsuariosCL.Dispose();
        }

        private void AgregarLiveSuscripciones(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pPerfilID, Guid pUsuarioID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ActualizarLiveProyectoUsuarioSuscripciones(pFilaCola, pUsuarioID, pLiveUsuariosCL, pNombreCacheElemento);
            ActualizarLiveUsuarioSuscripciones(pFilaCola, pUsuarioID, pLiveUsuariosCL, pNombreCacheElemento);

            //Si se agrega un recurso o se cambia la privacidad
            bool aumentarContador = pFilaCola.Accion == (int)AccionLive.Agregado || (pFilaCola.Accion == (int)AccionLive.Editado && pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA));

            if (aumentarContador)
            {
                LiveCN liveCN = new LiveCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                liveCN.AumentarContadorNuevasSuscripciones(pPerfilID);
                liveCN.Dispose();
            }
        }

        private Dictionary<Guid, Guid> ObtenerListaUsuariosSuscritos(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid? perfilPublicadorID, bool pObtenerSuscritosMetaProyecto, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //obtenemos los perfiles suscritos a las categorias del recurso o a los autores del documento
            SuscripcionCN suscrCN = new SuscripcionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            List<Guid> listaPerfilesSuscritosCategorias = suscrCN.ListaPerfilesSuscritosAAlgunaCategoriaDeDocumento(pFilaCola.Id, pFilaCola.ProyectoId);
            List<Guid> listaPerfilesSuscritosAutor = suscrCN.ListaPerfilesSuscritosAPerfilEnComunidad(perfilPublicadorID.Value, pFilaCola.ProyectoId, pObtenerSuscritosMetaProyecto);
            suscrCN.Dispose();

            //Obtenemos los usuarios de los perfiles
            List<Guid> listaPerfiles = new List<Guid>();
            listaPerfiles.AddRange(listaPerfilesSuscritosCategorias);
            listaPerfiles.AddRange(listaPerfilesSuscritosAutor.Where(x => !listaPerfilesSuscritosCategorias.Contains(x)));
            if (perfilPublicadorID.HasValue)
            {
                //Eliminamos el perfil del publicador
                listaPerfiles.Remove(perfilPublicadorID.Value);
            }

            UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Dictionary<Guid, Guid> listaUsuariosYPerfiles = usuarioCN.ObtenerUsuariosIDPorIDPerfil(listaPerfiles);
            usuarioCN.Dispose();

            return listaUsuariosYPerfiles;
        }

        private Dictionary<Guid, Guid> ObtenerListaUsuariosAfectados(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid? perfilPublicadorID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, bool esComunidadPrivada = false, bool pRecursoPrivado = false, bool pPrivacidadCambiada = false)
        {
            UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            Dictionary<Guid, List<Guid>> listaUsuariosAfectadosPorEvento = new Dictionary<Guid, List<Guid>>();
            Dictionary<Guid, List<Guid>> listaGruposAfectadosPorEvento = new Dictionary<Guid, List<Guid>>();

            // listas para cargar los usuarios a los que no debe afectar este evento
            Dictionary<Guid, List<Guid>> listaUsuariosEliminarAfectadosPorEvento = new Dictionary<Guid, List<Guid>>();
            Dictionary<Guid, List<Guid>> listaGruposEliminarAfectadosPorEvento = new Dictionary<Guid, List<Guid>>();

            // Si el recurso es privado, cargamos solamente los editores y lectores
            // Si se hace un cambio de privacidad, se cargan todos, para quitar la fila a los que no son editores ni lectores.
            if (pRecursoPrivado && !pPrivacidadCambiada)
            {
                // Obtenemos los usuarios y los perfiles editores y lectores del recurso
                usuarioCN.ObtenerUsuarioIDEditoresLectoresRecurso(mElementoID, listaUsuariosAfectadosPorEvento);

                // Obtenemos los grupos de editores del recurso
                listaGruposAfectadosPorEvento = usuarioCN.ObtenerDiccionarioGruposYPerfilesPorProyectoYDocPrivado(pFilaCola.ProyectoId, mElementoID);
            }
            else
            {
                // Obtenemos los usuarios y los perfiles del proyecto
                usuarioCN.ObtenerUsuariosParticipanEnProyecto(pFilaCola.ProyectoId, listaUsuariosAfectadosPorEvento);

                if (perfilPublicadorID.HasValue && !esComunidadPrivada)
                {
                    // Obtenemos los contactos y los seguidores del usuario
                    usuarioCN.ObtenerUsuariosSonContactoOSeguidorDePerfilUsuario(perfilPublicadorID.Value, listaUsuariosAfectadosPorEvento);
                }

                // Si el recurso se ha cambiado a privado debemos quitar los editores de la lista
                if (pRecursoPrivado && pPrivacidadCambiada)
                {
                    // Obtenemos los usuarios y los perfiles editores y lectores del recurso
                    usuarioCN.ObtenerUsuarioIDEditoresLectoresRecurso(mElementoID, listaUsuariosEliminarAfectadosPorEvento);

                    // Obtenemos los grupos de editores del recurso
                    listaGruposEliminarAfectadosPorEvento = usuarioCN.ObtenerDiccionarioGruposYPerfilesPorProyectoYDocPrivado(pFilaCola.ProyectoId, mElementoID);
                }
            }

            Dictionary<Guid, Guid> listaUsuariosActualizar = new Dictionary<Guid, Guid>();

            foreach (Guid usuarioID in listaUsuariosAfectadosPorEvento.Keys)
            {
                if (!listaUsuariosEliminarAfectadosPorEvento.ContainsKey(usuarioID))
                {
                    foreach (Guid perfilID in listaUsuariosAfectadosPorEvento[usuarioID])
                    {
                        if (!listaUsuariosActualizar.ContainsKey(perfilID) && (!perfilPublicadorID.HasValue || perfilID != perfilPublicadorID.Value))
                        {
                            listaUsuariosActualizar.Add(perfilID, usuarioID);
                        }
                    }
                }
            }

            foreach (Guid grupoID in listaGruposAfectadosPorEvento.Keys)
            {
                if (!listaGruposEliminarAfectadosPorEvento.ContainsKey(grupoID))
                {
                    Dictionary<Guid, Guid> listaUsuariosYPerfilesGrupo = usuarioCN.ObtenerUsuariosIDPorIDPerfil(listaGruposAfectadosPorEvento[grupoID]);

                    foreach (Guid perfilID in listaUsuariosYPerfilesGrupo.Keys)
                    {
                        if (!listaUsuariosActualizar.ContainsKey(perfilID) && (!perfilPublicadorID.HasValue || perfilID != perfilPublicadorID.Value))
                        {
                            listaUsuariosActualizar.Add(perfilID, listaUsuariosYPerfilesGrupo[perfilID]);
                        }
                    }
                }
            }

            usuarioCN.Dispose();

            return listaUsuariosActualizar;
        }

        private void ObtenerIDElementoPrincipal(LiveUsuariosDS.ColaUsuariosRow pFilaCola, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            TipoLive tipo = (TipoLive)pFilaCola.Tipo;
            AccionLive accion = (AccionLive)pFilaCola.Accion;

            mElementoID = pFilaCola.Id;

            switch (tipo)
            {
                case TipoLive.Recurso:
                case TipoLive.Pregunta:
                case TipoLive.Debate:
                    DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                    switch (accion)
                    {
                        case AccionLive.ComentarioAgregado:
                            mElementoID = docCN.ObtenerIDDocumentoDeComentarioPorID(pFilaCola.Id);
                            break;
                        case AccionLive.Votado:
                            mElementoID = docCN.ObtenerIDDocumentoDeVotoPorID(pFilaCola.Id);
                            break;
                    }
                    docCN.Dispose();
                    break;
            }
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un perfil
        /// </summary>
        /// <param name="pPerfilID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScorePerfil(Guid pPerfilID)
        {
            if (!mListaScorePorPerfil.ContainsKey(pPerfilID))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorPerfil.Add(pPerfilID, -2);
            }
            return ++mListaScorePorPerfil[pPerfilID];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreProyectoUsuarioSuscripciones(Guid pProyectoID, Guid pUsuarioID)
        {
            string claveProyUsuSuscr = "suscripciones_" + pProyectoID + pUsuarioID;

            if (!mListaScorePorProyUsuSuscr.ContainsKey(claveProyUsuSuscr))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorProyUsuSuscr.Add(claveProyUsuSuscr, -2);
            }
            return ++mListaScorePorProyUsuSuscr[claveProyUsuSuscr];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreUsuarioSuscripciones(Guid pUsuarioID)
        {
            string claveUsuSuscr = "suscripcionesUsu_" + pUsuarioID;

            if (!mListaScorePorUsuSuscr.ContainsKey(claveUsuSuscr))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorUsuSuscr.Add(claveUsuSuscr, -2);
            }
            return ++mListaScorePorUsuSuscr[claveUsuSuscr];
        }

        #endregion

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ControladorLiveUsuariosEspecifico(mMinutosEntrePintadoAgrupacionNuevosRegistros, ScopedFactory, mConfigService);
        }
    }
}
