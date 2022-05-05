using System;
using System.Collections.Generic;
using System.Threading;
using System.Data;
using Es.Riam.Gnoss.AD.Live;
using Es.Riam.Gnoss.Logica.Live;
using Es.Riam.Gnoss.AD.Live.Model;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.Identidad;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.Logica.Suscripcion;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.CL.Documentacion;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.Web.MVC.Models;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using System.Linq;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.RabbitMQ;
using Es.Riam.Util;
using Newtonsoft.Json;
using Es.Riam.Gnoss.Recursos;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using ConfiguracionServicios = Es.Riam.Gnoss.Util.General.ConfiguracionServicios;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.ServicioLive
{
    internal class Controller : ControladorServicioGnoss
    {
        #region Constantes

        private const string EXCHANGE = "";
        private const string COLA_POPULARIDAD = "ColaPopularidad";
        private const string COLA_VISITAS = "ColaVisitas";

        /// <summary>
        /// Recursos
        /// </summary>
        public const double FactorPopularidad = 2;

        #endregion

        #region Statics

        private static object OBJETO_BLOQUEO_ARCHIVO_VISITAS = new object();

        #endregion

        #region Miembros

        /// <summary>
        /// Data set con los recursos a procesar
        /// </summary>
        private LiveDS mLiveDSRecursos = new LiveDS();

        /// <summary>
        /// Data set con la cola de popularidad
        /// </summary>
        private LiveDS mLiveDSColaPopularidad = new LiveDS();

        /// <summary>
        /// Guarda la hora actual y sirve para actualizar los votos, comentarios y visitas de los documentos
        /// </summary>
        private DateTime mFechaHace5Min = DateTime.Now;
        
        /// <summary>
        /// Minutos necesarios para actualizar los vosotos, visitas y comentaros
        /// </summary>
        int mVVC = 5;

        #endregion

        #region Constructores

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pFicheroConfiguracionBD">Ruta al archivo de configuración de la base de datos</param>
        public Controller(IServiceScopeFactory scopedFactory, ConfigService configService)
            : base(scopedFactory, configService)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pFicheroConfiguracionBD">Ruta al archivo de configuración de la base de datos</param>
        public Controller(IServiceScopeFactory scopedFactory, ConfigService configService, int pVVC)
            : base(scopedFactory, configService)
        {
            mVVC = pVVC;
        }

        #endregion

        #region Métodos generales

        #region Públicos

        /// <summary>
        /// Obtiene la popularidad de una identidad en una comunidad
        /// </summary>
        ///  /// <param name="pProyectoID">Identificador del proyecto </param>
        ///   /// <param name="pIdentidadID">Identificador de la identidad de la que se busca la popularidad</param>
        public double ObtengoPopularidadIdentidad(Guid pIdentidadID, Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            IdentidadCN idenCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            double popularidad = (double)idenCN.ObtenerPopularidadDeIdentidadEnProyecto(pIdentidadID, pProyectoID);

            //Normalizo la popularidad de 1 a 10
            double MaxPop = idenCN.ObtenerPopularidadMaxDeIdentidadEnProyecto(pProyectoID);
            if (MaxPop > 0)
            {
                popularidad = ((FactorPopularidad * popularidad) / MaxPop) + 1;
            }
            return popularidad;
        }

        /// <summary>
        /// Procesa una visita a un recurso.
        /// </summary>
        /// <param name="pColaRow">Fila de cola</param>
        /// <param name="pDocCN">CN de documento</param>
        private int ProcesarVisitaRecursoCada5Min(Guid pDocID, string pInfoExtra, int pNumVisitas, DocumentacionCN pDocCN)
        {
            Guid baseRecursosID = new Guid(pInfoExtra);
            return pDocCN.ActualizarNumeroConsultasDocumento(pDocID, pNumVisitas, baseRecursosID);
        }

        private void ActualizarVisitasModeloRecursoMVC(Guid pRecursoID, Guid pProyectoID, int pNumeroVisitasRecurso, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, EntityContextBASE entityContextBASE, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCL docCL = new DocumentacionCL(mFicheroConfiguracionBD, "recursos", entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            docCL.Dominio = mDominio;
            List<Guid> listaDocumentos = new List<Guid>();
            listaDocumentos.Add(pRecursoID);

            // Obtener el modelo del recurso en Redis
            Dictionary<Guid, ResourceModel> fichasRecursosModelos = docCL.ObtenerFichasRecursoMVC(listaDocumentos, pProyectoID);

            if (fichasRecursosModelos.Count > 0 && fichasRecursosModelos.ContainsKey(pRecursoID) && fichasRecursosModelos[pRecursoID] != null && pNumeroVisitasRecurso > -1)
            {
                // Si existe Actualizar el contador de visitas
                fichasRecursosModelos[pRecursoID].NumVisits = pNumeroVisitasRecurso;

                // Guardar el modelo del recurso en Redis
                docCL.AgregarFichasRecursoMVC(fichasRecursosModelos, pProyectoID);

            }
            else
            {
                // Refrescamos la caché del recurso
                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                baseComunidadCN.InsertarFilaEnColaRefrescoCache(pProyectoID, TiposEventosRefrescoCache.ModificarCaducidadCache, TipoBusqueda.Recursos, pRecursoID.ToString());
                baseComunidadCN.Dispose();
            }

            docCL.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="proyectoID"></param>
        /// <param name="colaRow"></param>
        /// <param name="idenCN"></param>
        /// <param name="docCN"></param>
        /// <param name="facCN"></param>
        /// <param name="susCN"></param>
        /// <param name="proCN"></param>
        public void ProcesarColaPopularidad(Guid proyectoID, LiveDS.ColaPopularidadRow colaRow, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoCN facCN = new FacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, null, entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            //facCN.FacetadoAD.CadenaConexionBase = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + Path.DirectorySeparatorChar + "config" + Path.DirectorySeparatorChar + "gnossBASE.config";
            facCN.FacetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

            IdentidadCN idenCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            SuscripcionCN susCN = new SuscripcionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            ProyectoCN proCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            #region Identidad1 visita un recurso de Identidad 2
            //MJ •	Identidad1 visita un recurso de Identidad 2. 1 * popularidad identidad1 a Identidad2
            if (colaRow.Accion == (int)AccionLive.VisitaRecurso)
            {

                //Obtengo la identidad que publico el recurso que es visitado (identidad2)
                Guid identidadRecursoVisitado = Guid.Empty;
                if (colaRow.ProyectoId != ProyectoAD.MetaProyecto)
                {
                    DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoDocumentoWebVinBRPorID(colaRow.Id, colaRow.ProyectoId);

                    if (docDW.ListaDocumentoWebVinBaseRecursos.Count > 0 && docDW.ListaDocumentoWebVinBaseRecursos.First().IdentidadPublicacionID.HasValue)
                    {
                        identidadRecursoVisitado = docDW.ListaDocumentoWebVinBaseRecursos.First().IdentidadPublicacionID.Value;
                    }
                }
                else
                {
                    DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoPorID(colaRow.Id);
                    if (docDW.ListaDocumento.Count > 0 && docDW.ListaDocumento.First().CreadorID.HasValue)
                    {
                        identidadRecursoVisitado = docDW.ListaDocumento.First().CreadorID.Value;
                    }
                }

                TipoAcceso tipoAccesoProyecto = proCN.ObtenerTipoAccesoProyecto(colaRow.ProyectoId);


                //Obtengo ID de identidad2 en MyGnoss
                Guid identidadMygnoss = idenCN.ObtenerIdentidadIDDeMyGNOSSPorIdentidad(identidadRecursoVisitado);

                //Obtengo la popularidad de identidad 1
                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, colaRow.ProyectoId, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //Multiplicamos la popularidad por el número de recursos que se han visitado?
                int numRecursos = 1;
                if (!string.IsNullOrEmpty(colaRow.InfoExtra))
                {
                    numRecursos = int.Parse(colaRow.InfoExtra.Substring(colaRow.InfoExtra.IndexOf("=") + 1));
                }
                popularidad = popularidad * numRecursos;


                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadRecursoVisitado, popularidad);
                    if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                    {
                        idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, popularidad);
                    }
                }
                else
                {

                    idenCN.ActualizarPopularidadIdentidades(identidadRecursoVisitado, 5);
                    if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                    {
                        idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, 5);
                    }
                }


                //EN VIRTUOSO
                // Se encarga el servicio de optimización cada Domingo... No es necesario actualizar la popularidad con cada visita que se recibe =/
                //facCN.ModificarPopularidadIdentidad(identidadRecursoVisitado.ToString());
                //if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                //{
                //    facCN.ModificarPopularidadIdentidad(identidadMygnoss.ToString());
                //}

            }
            #endregion

            #region Identidad1 vincula un recurso a un recurso de Identidad2
            //MJ •	Identidad1 vincula un recurso a un recurso de Identidad2. 5 a Identidad1. 3 * popularidad identidad1 a Identidad2

            if (colaRow.Accion == (int)AccionLive.VincularRecursoaRecurso)
            {

                //Obtengo la identidad que publico el recurso que es visitado (identidad2)
                Guid identidadRecursoVinculado = docCN.ObtenerDocumentoDocumentoWebVinBRPorID(colaRow.Id2, colaRow.ProyectoId).ListaDocumentoWebVinBaseRecursos.First().IdentidadPublicacionID.Value;
                TipoAcceso tipoAccesoProyecto = proCN.ObtenerTipoAccesoProyecto(colaRow.ProyectoId);


                //Obtengo ID de identidad2 en MyGnoss
                Guid identidadMygnoss = idenCN.ObtenerIdentidadIDDeMyGNOSSPorIdentidad(identidadRecursoVinculado);

                //Obtengo la popularidad de identidad 1              
                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id, colaRow.ProyectoId, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //Obtengo ID de identidad1 en MyGnoss
                Guid identidadMygnoss2 = idenCN.ObtenerIdentidadIDDeMyGNOSSPorIdentidad(colaRow.Id);

                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadRecursoVinculado, 3 * popularidad);
                    if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                    {
                        idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, 3 * popularidad);
                    }
                }
                else
                {

                    idenCN.ActualizarPopularidadIdentidades(identidadRecursoVinculado, 3);
                    if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                    {
                        idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, 3);
                    }
                }

                idenCN.ActualizarPopularidadIdentidades(colaRow.Id, 5);
                if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadMygnoss2, 5);
                }

                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadRecursoVinculado.ToString());
                facCN.ModificarPopularidadIdentidad(colaRow.Id.ToString());

                if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                {
                    facCN.ModificarPopularidadIdentidad(identidadMygnoss.ToString());

                    facCN.ModificarPopularidadIdentidad(identidadMygnoss2.ToString());
                }

            }

            #endregion

            #region Identidad1 desvincula un recurso a un recurso de Identidad2
            //MJ •	Identidad1 desvincula un recurso a un recurso de Identidad2. 5 a Identidad1. 3 * popularidad identidad1 a Identidad2

            if (colaRow.Accion == (int)AccionLive.DesincularRecursoaRecurso)
            {

                //Obtengo la identidad que publico el recurso que es visitado (identidad2)
                Guid identidadRecursoVinculado = docCN.ObtenerDocumentoDocumentoWebVinBRPorID(colaRow.Id2, colaRow.ProyectoId).ListaDocumentoWebVinBaseRecursos.First().IdentidadPublicacionID.Value;
                TipoAcceso tipoAccesoProyecto = proCN.ObtenerTipoAccesoProyecto(colaRow.ProyectoId);
                //Obtengo ID de identidad2 en MyGnoss
                Guid identidadMygnoss = idenCN.ObtenerIdentidadIDDeMyGNOSSPorIdentidad(identidadRecursoVinculado);

                //Obtengo la popularidad de identidad 1              
                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id, colaRow.ProyectoId, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //Obtengo ID de identidad1 en MyGnoss
                Guid identidadMygnoss2 = idenCN.ObtenerIdentidadIDDeMyGNOSSPorIdentidad(colaRow.Id);

                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadRecursoVinculado, -3 * popularidad);
                    if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                    {
                        idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, -3 * popularidad);
                    }
                }
                else
                {

                    idenCN.ActualizarPopularidadIdentidades(identidadRecursoVinculado, -3);
                    if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                    {
                        idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, -3);
                    }
                }

                idenCN.ActualizarPopularidadIdentidades(colaRow.Id, -5);
                if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadMygnoss2, -5);
                }
                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadRecursoVinculado.ToString());
                facCN.ModificarPopularidadIdentidad(colaRow.Id.ToString());
                if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                {
                    facCN.ModificarPopularidadIdentidad(identidadMygnoss.ToString());


                    facCN.ModificarPopularidadIdentidad(identidadMygnoss2.ToString());
                }

            }

            #endregion

            #region Suscripcion a Usuario
            if (colaRow.Accion == (int)AccionLive.SuscribirseUsuario)
            {
                //MJ: Identidad1 se hace seguidor de identidad2. 1 * popularidad identidad1 a Identidad2
                //Obtengo la identidad de la persona de la que se hace seguidor (identidad2)
                Guid identidadSeguida = susCN.ObtenerIdentidadSeguidorPorIDSuscripcion(colaRow.Id);



                //Obtengo la popularidad de identidad que se hace seguidor
                //SuscripcionDS susDS = new SuscripcionDS();
                //susDS = susCN.ObtenerSuscripciones(false);

                //SuscripcionDS.SuscripcionRow suscripcionRow = susDS.Suscripcion[0];

                //Guid identidadSeguidor = suscripcionRow.IdentidadID;



                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);


                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, 5 *
 popularidad);
                }
                else
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, 5);

                }

                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadSeguida.ToString());


            }
            #endregion

            #region Dessuscripcion a Usuario
            if (colaRow.Accion == (int)AccionLive.DessuscribirseUsuario)
            {
                //MJ: Identidad1 se hace seguidor de identidad2. 1 * popularidad identidad1 a Identidad2
                //Obtengo la identidad de la persona de la que se hace seguidor (identidad2)
                Guid identidadSeguida = colaRow.Id;



                //Obtengo la popularidad de identidad que se hace seguidor
                //SuscripcionDS susDS = new SuscripcionDS();
                //susDS = susCN.ObtenerSuscripciones(false);

                //SuscripcionDS.SuscripcionRow suscripcionRow = susDS.Suscripcion[0];

                //Guid identidadSeguidor = suscripcionRow.IdentidadID;



                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);


                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, -5 *
 popularidad);
                }
                else
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, -5);

                }

                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadSeguida.ToString());


            }
            #endregion

            #region Suscripcion a Usuario en Comunidad
            if (colaRow.Accion == (int)AccionLive.SuscribirseUsuarioComunidad)
            {
                //MJ: Identidad1 se hace seguidor de identidad2. 1 * popularidad identidad1 a Identidad2
                //Obtengo la identidad de la persona de la que se hace seguidor (identidad2)
                Guid identidadSeguida = susCN.ObtenerIdentidadSeguidorComPorIDSuscripcion(colaRow.Id);



                //Obtengo la popularidad de identidad que se hace seguidor
                //SuscripcionDS susDS = new SuscripcionDS();
                //susDS = susCN.ObtenerSuscripciones(false);

                //SuscripcionDS.SuscripcionRow suscripcionRow = susDS.Suscripcion[0];

                //Guid identidadSeguidor = suscripcionRow.IdentidadID;


                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, 5 * popularidad);
                }
                else { idenCN.ActualizarPopularidadIdentidades(identidadSeguida, 5); }
                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadSeguida.ToString());


            }
            #endregion

            #region Dessuscripcion a Usuario en Comunidad
            if (colaRow.Accion == (int)AccionLive.DessuscribirseUsuarioComunidad)
            {
                //MJ: Identidad1 se hace seguidor de identidad2. 1 * popularidad identidad1 a Identidad2
                //Obtengo la identidad de la persona de la que se hace seguidor (identidad2)
                Guid identidadSeguida = colaRow.Id;



                //Obtengo la popularidad de identidad que se hace seguidor
                //SuscripcionDS susDS = new SuscripcionDS();
                //susDS = susCN.ObtenerSuscripciones(false);

                //SuscripcionDS.SuscripcionRow suscripcionRow = susDS.Suscripcion[0];

                //Guid identidadSeguidor = suscripcionRow.IdentidadID;


                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, -5 * popularidad);
                }
                else { idenCN.ActualizarPopularidadIdentidades(identidadSeguida, -5); }
                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadSeguida.ToString());


            }
            #endregion

            #region Suscripcion a Blog
            if (colaRow.Accion == (int)AccionLive.SuscribirseUsuarioBlog)
            {
                //MJ: Identidad1 se hace seguidor de identidad2. 1 * popularidad identidad1 a Identidad2
                //Obtengo la identidad de la persona de la que se hace seguidor (identidad2)
                Guid identidadSeguida = susCN.ObtenerIdentidadSeguidorBlogPorIDSuscripcion(colaRow.Id);



                //Obtengo la popularidad de identidad que se hace seguidor
                //SuscripcionDS susDS = new SuscripcionDS();
                //susDS = susCN.ObtenerSuscripciones(false);

                //SuscripcionDS.SuscripcionRow suscripcionRow = susDS.Suscripcion[0];

                //Guid identidadSeguidor = suscripcionRow.IdentidadID;



                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);


                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, 5 *
 popularidad);
                }
                else
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, 5);

                }

                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadSeguida.ToString());


            }
            #endregion

            #region Dessuscripcion a Blog
            if (colaRow.Accion == (int)AccionLive.DessuscribirseUsuarioBlog)
            {
                //MJ: Identidad1 se hace seguidor de identidad2. 1 * popularidad identidad1 a Identidad2
                //Obtengo la identidad de la persona de la que se hace seguidor (identidad2)
                Guid identidadSeguida = colaRow.Id;



                //Obtengo la popularidad de identidad que se hace seguidor
                //SuscripcionDS susDS = new SuscripcionDS();
                //susDS = susCN.ObtenerSuscripciones(false);

                //SuscripcionDS.SuscripcionRow suscripcionRow = susDS.Suscripcion[0];

                //Guid identidadSeguidor = suscripcionRow.IdentidadID;



                double popularidad = ObtengoPopularidadIdentidad(colaRow.Id2, ProyectoAD.MetaProyecto, entityContext, loggingService, servicesUtilVirtuosoAndReplication);


                //EN SQL
                if (popularidad > 0)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, -5 *
 popularidad);
                }
                else
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadSeguida, -5);

                }

                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(identidadSeguida.ToString());


            }
            #endregion

            #region Añadir Articulo Blog

            if (colaRow.Accion == (int)AccionLive.AgregarArticuloBlog)
            {
                //MJ: Identidad1 crea un artículo de blog. 30 a Identidad1


                //MODIFICAR POPULARIDAD

                idenCN.ActualizarPopularidadIdentidades(colaRow.Id2, 30);


                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(colaRow.Id2.ToString());

            }
            #endregion

            #region Eliminar Articulo Blog

            if (colaRow.Accion == (int)AccionLive.EliminarArticuloBlog)
            {
                //MJ: Identidad1 borra un artículo de blog. 30 a Identidad1


                //MODIFICAR POPULARIDAD

                idenCN.ActualizarPopularidadIdentidades(colaRow.Id2, -30);


                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(colaRow.Id2.ToString());

            }
            #endregion

            #region Recurso Descertificado

            if (colaRow.Accion == (int)AccionLive.RecursoDesCertificado)
            {
                //MJ Identidad1 certifica un recurso de Identidad 2. 20 * NivelCertificacion a Identidad2


                //Obtengo ID identidad2 en MyGnoss
                DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoDocumentoWebVinBRPorID(colaRow.Id, colaRow.ProyectoId);

                AD.EntityModel.Models.Documentacion.Documento documentoRow = docDW.ListaDocumento.FirstOrDefault();

                TipoAcceso tipoAccesoProyecto = proCN.ObtenerTipoAccesoProyecto(colaRow.ProyectoId);

                Guid identidadMygnoss = idenCN.ObtenerIdentidadIDDeMyGNOSSPorIdentidad(documentoRow.CreadorID.Value);

                //Obtengo el nivel de certificacion
                ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                DataWrapperProyecto dataWrapperProyecto = proyCN.ObtenerNivelesCertificacionRecursosProyecto(colaRow.ProyectoId);

                int nivelcertificacion = dataWrapperProyecto.ListaNivelCertificacion.FirstOrDefault(nivelCert => nivelCert.NivelCertificacionID.Equals(colaRow.Id2)).Orden;
                nivelcertificacion = 100 / nivelcertificacion + 1;


                //EN SQL
                idenCN.ActualizarPopularidadIdentidades(documentoRow.CreadorID.Value, -nivelcertificacion * 20);
                if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                {
                    idenCN.ActualizarPopularidadIdentidades(identidadMygnoss, -nivelcertificacion * 20);
                }
                //EN VIRTUOSO

                facCN.ModificarPopularidadIdentidad(documentoRow.CreadorID.ToString());
                if (tipoAccesoProyecto == TipoAcceso.Publico || tipoAccesoProyecto == TipoAcceso.Restringido)
                {
                    facCN.ModificarPopularidadIdentidad(identidadMygnoss.ToString());
                }

            }

            #endregion
        }


       

        private void RealizarMantenimientoBD()
        {
            while (true)
            {
                using (var scope = ScopedFactory.CreateScope())
                {
                    LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                    EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                    IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                    LiveUsuariosCN liveUsuariosCN = new LiveUsuariosCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                    RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                    GnossCache gnossCache = scope.ServiceProvider.GetRequiredService<GnossCache>();
                    EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                    try
                    {

                        mVVC = mConfigService.ObtenerIntervaloVVC();
                        
                        ComprobarCancelacionHilo();

                        if (mReiniciarLecturaRabbit)
                        {
                            RealizarMantenimientoRabbitMQColaPopularidad(loggingService);
                            RealizarMantenimientoRabbitMQColaVisitas(loggingService);
                        }

                        DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                        if (File.Exists(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}.txt")))
                        {
                            string[] listaVisitas = null;
                            lock (OBJETO_BLOQUEO_ARCHIVO_VISITAS)
                            {
                                listaVisitas = File.ReadAllLines(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}.txt"));
                            }

                            bool existeCopiaAnterior = File.Exists(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Copia.txt"));

                            if (existeCopiaAnterior || listaVisitas.Count() > 100 || mFechaHace5Min.AddMinutes(mVVC) < DateTime.Now)
                            {
                                if (existeCopiaAnterior)
                                {
                                    listaVisitas = File.ReadAllLines(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}.txt"));
                                    if (File.Exists(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Procesados.txt")))
                                    {
                                        int numElementosProcesados = File.ReadAllLines(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Procesados.txt")).Length;
                                        listaVisitas = listaVisitas.Skip(numElementosProcesados).ToArray();
                                    }
                                }
                                else
                                {
                                    lock (OBJETO_BLOQUEO_ARCHIVO_VISITAS)
                                    {
                                        File.Move(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}.txt"), Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Copia.txt"));
                                    }
                                }


                                //Espero 5 minutos a procesar las visitas si hay pocas, si se han acumulado muchas visitas no espero
                                //this.GuardarLog("Procesando Visitas");
                                LiveDS liveDS = new LiveDS();
                                List<LiveDS.ColaRow> listaElementosPendientesVisitas = new List<LiveDS.ColaRow>();
                                foreach (string filaVisita in listaVisitas)
                                {
                                    object[] itemArray = JsonConvert.DeserializeObject<object[]>(filaVisita);

                                    LiveDS.ColaRow fila = (LiveDS.ColaRow)liveDS.Cola.Rows.Add(itemArray);
                                    listaElementosPendientesVisitas.Add(fila);
                                }
                                List<Guid> listaProyectosEnCola = ObtenerListaProyectosEnCola(listaElementosPendientesVisitas);

                                //Por cada proyecto con nuevas visitas:
                                foreach (Guid proyectoID in listaProyectosEnCola)
                                {
                                    List<LiveDS.ColaRow> listaVisitasPorProyectoID = listaElementosPendientesVisitas.Where(item => item.ProyectoId.Equals(proyectoID)).ToList();

                                    // Eventos

                                    Dictionary<Guid, int> dicRecNumVisitas = new Dictionary<Guid, int>();
                                    Dictionary<Guid, string> dicRecInfoExtra = new Dictionary<Guid, string>();
                                    Dictionary<Guid, Guid> dicRecInfoProy = new Dictionary<Guid, Guid>();
                                    Dictionary<Guid, DateTime> dicRecInfoFecha = new Dictionary<Guid, DateTime>();

                                    //Guardamos en un Dic las filas que se deben procesar:
                                    foreach (LiveDS.ColaRow colaRow in listaVisitasPorProyectoID)
                                    {
                                        string baseRecursosID = "";
                                        int numVisitas = 1;
                                        if (colaRow.InfoExtra.Contains("|"))
                                        {
                                            baseRecursosID = colaRow.InfoExtra.Split('|')[0];
                                            string numVisitasCola = colaRow.InfoExtra.Split('|')[1];
                                            numVisitas = int.Parse(numVisitasCola.Substring(numVisitasCola.IndexOf("=") + 1));
                                        }
                                        else
                                        {
                                            baseRecursosID = colaRow.InfoExtra;
                                        }

                                        if (colaRow.Accion == (int)AccionLive.VisitaRecurso)
                                        {
                                            if (!dicRecNumVisitas.ContainsKey(colaRow.Id))
                                            {
                                                dicRecNumVisitas.Add(colaRow.Id, numVisitas);
                                                dicRecInfoExtra.Add(colaRow.Id, baseRecursosID);
                                            }
                                            else
                                            {
                                                dicRecNumVisitas[colaRow.Id] = dicRecNumVisitas[colaRow.Id] + numVisitas;
                                            }
                                        }
                                    }

                                    //Por cada elemento del diccionario, le pasamos los campos, actualizamos y cambiamos el estado de las filas del DS.
                                    foreach (Guid recID in dicRecNumVisitas.Keys)
                                    {
                                        try
                                        {
                                            int numConsultasDocumento = ProcesarVisitaRecursoCada5Min(recID, dicRecInfoExtra[recID], dicRecNumVisitas[recID], docCN);

                                            ActualizarVisitasModeloRecursoMVC(recID, proyectoID, numConsultasDocumento, entityContext, loggingService, redisCacheWrapper, entityContextBASE, servicesUtilVirtuosoAndReplication);
                                        }
                                        catch (Exception ex)
                                        {
                                            loggingService.GuardarLog(ex.Message);
                                        }
                                        finally
                                        {
                                            File.AppendAllLines(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Procesados.txt"), new List<string> { "Linea procesada correctamente" });
                                        }

                                        liveDS.Dispose();
                                    }
                                }

                                File.Delete(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Copia.txt"));
                                File.Delete(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}_Procesados.txt"));
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        loggingService.GuardarLog("ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                    }
                    finally
                    {
                        //Actualizamos la fecha almacenada si han pasado 5 min.
                        if (mFechaHace5Min.AddMinutes(mVVC) < DateTime.Now)
                        {
                            mFechaHace5Min = DateTime.Now;
                        }

                        //Duermo el proceso el tiempo establecido
                        Thread.Sleep(INTERVALO_SEGUNDOS * 1000);
                    }
                }
            }
        }

        private List<Guid> ObtenerListaProyectosEnCola(List<LiveDS.ColaRow> pListaVisitas)
        {
            return pListaVisitas.Select(item => item.ProyectoId).Distinct().ToList();
        }

        /// <summary>
        /// Procesa cada suscripcion para crear su notificacion correspondiente
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            #region Establezco el dominio de la cache

            ParametroAplicacionCN parametroApliCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            ParametroAplicacionGBD gestorParamatroAppController = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
            gestorParamatroAppController.ObtenerConfiguracionGnoss(gestorParametroAplicacion);

            //mDominio = gestorParametroAplicacion.ParametroAplicacion.Select("Parametro='UrlIntragnoss'")[0]).Valor;
            mDominio = gestorParametroAplicacion.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).ToList().FirstOrDefault().Valor;
            mDominio = mDominio.Replace("http://", "").Replace("www.", "");

            if (mDominio[mDominio.Length - 1] == '/')
            {
                mDominio = mDominio.Substring(0, mDominio.Length - 1);
            }

            // mUrlIntragnoss = ((ParametroAplicacionDS.ParametroAplicacionRow)paramApliDS.ParametroAplicacion.Select("Parametro='UrlIntragnoss'")[0]).Valor;
            mUrlIntragnoss = gestorParametroAplicacion.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).ToList().FirstOrDefault().Valor; ;
            #endregion

            RealizarMantenimientoRabbitMQColaPopularidad(loggingService);
            RealizarMantenimientoRabbitMQColaVisitas(loggingService);
            RealizarMantenimientoBD();
        }

        private void RealizarMantenimientoRabbitMQColaVisitas(LoggingService loggingService,bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItemColaVisitas);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_VISITAS, loggingService, mConfigService, EXCHANGE, COLA_VISITAS);

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
        
        private bool ProcesarItemColaVisitas(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                UtilidadesVirtuoso utilidadesVirtuoso = scope.ServiceProvider.GetRequiredService<UtilidadesVirtuoso>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                GnossCache gnossCache = scope.ServiceProvider.GetRequiredService<GnossCache>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                try
                {
                    ComprobarCancelacionHilo();

                    System.Diagnostics.Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        LiveDS.ColaRow filaCola = (LiveDS.ColaRow)new LiveDS().Cola.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilaDeColaVisitas(filaCola, loggingService);

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

        private bool ProcesarFilaDeColaVisitas(LiveDS.ColaRow pFilaCola, LoggingService loggingService)
        {
            try
            {
                //string json = JsonConvert.SerializeObject(pFilaCola.ItemArray, Formatting.Indented, new JsonSerializerSettings
                //{
                //    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                //});
                string json = JsonConvert.SerializeObject(pFilaCola.ItemArray);

                lock (OBJETO_BLOQUEO_ARCHIVO_VISITAS)
                {
                    File.AppendAllLines(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "elementos", $"ListaElementosColaVisitas_{mPlataforma}.txt"), new string[] { json });
                }
            }
            catch (Exception ex)
            {
                loggingService.GuardarLogError(ex, $"No se ha podido escribir en el fichero la fila {pFilaCola}");
            }            

            return true;
        }

        private void RealizarMantenimientoRabbitMQColaPopularidad(LoggingService loggingService, bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItemColaPopularidad);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_POPULARIDAD, loggingService, mConfigService, EXCHANGE, COLA_POPULARIDAD);

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

        private bool ProcesarItemColaPopularidad(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
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
                        LiveDS.ColaPopularidadRow filaCola = (LiveDS.ColaPopularidadRow)new LiveDS().ColaPopularidad.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilasDeColaPopularidad(filaCola, entityContext, loggingService, virtuosoAD, servicesUtilVirtuosoAndReplication);

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

        private bool ProcesarFilasDeColaPopularidad(LiveDS.ColaPopularidadRow pFilaCola, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            try
            {
                if (pFilaCola.NumIntentos < 6)
                {
                    bool procesado = true;
                    try
                    {
                        ProcesarColaPopularidad(pFilaCola.ProyectoId, pFilaCola, entityContext, loggingService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    }
                    catch (Exception ex)
                    {
                        loggingService.GuardarLogError(ex);
                    }

                    //Cambiamos el número de intentos a 7 para procesarlas cada 5 min.
                    if (pFilaCola.Accion == (int)AccionLive.Votado || pFilaCola.Accion == (int)AccionLive.ComentarioAgregado || pFilaCola.Accion == (int)AccionLive.ComentarioEditado || pFilaCola.Accion == (int)AccionLive.ComentarioEliminado)
                    {
                        pFilaCola.NumIntentos = 7;
                    }
                    else if (procesado)
                    {
                        pFilaCola.Delete();
                    }
                    else
                    {
                        pFilaCola.NumIntentos++;
                    }

                }
            }
            catch (Exception ex)
            {
                loggingService.GuardarLog($"ERROR al procesar ColaPopularidad: {ex.Message}");                   
            }

            return true;
        }
        
        #endregion

        #endregion

        #region Métodos sobreescritos

        protected override ControladorServicioGnoss ClonarControlador()
        {
            Controller controlador = new Controller(ScopedFactory, mConfigService, mVVC);
            return controlador;
        }

        #endregion

    }
}
