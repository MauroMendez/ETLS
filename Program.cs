using ETL.Data.NpgSql;
using System;
using Microsoft.EntityFrameworkCore;
using ETL.Data.Sql;
using System.Linq;
using System.Collections.Generic;

namespace ETL
{
    internal class Program
    {
        static void Main(string[] args)
        {


            //Conexción a postgress
            using (var npgsql_context = new NpgSql_Context())
            {
                // conexión sql
                using (var sql_context = new Sql_Context())
                {
                    // Cargamos alumnos
                    //string cargaAlumnos = CargaAlumnos();
                    //Console.WriteLine(cargaAlumnos);

                    //Cargamos conceptos
                    //string cargaConceptosS = CargaConceptos();


                    // Cargamos listas de precios
                    //string cargaListaPrecios = CargaListaPrecios();

                    //Carga detalle de cuentas
                    string cargaDetCuentas = CargaDetalleCuentas();



                    Console.WriteLine("Fin de operación");
                    Console.ReadKey();
                    Environment.Exit(0);

                    string CargaAlumnos()
                    {
                        // se hace la consulta de la vista
                        //where ALU2.Crmid > 5423
                        List<Etlalumno> alumnosv2list = (from ALU2 in npgsql_context.Etlalumno where ALU2.Crmid > 5360 && ALU2.Crmid <= 5422 orderby ALU2.Crmid ascending, ALU2.Beid descending select ALU2).ToList();
                        foreach (var alumnosv2 in alumnosv2list)
                        {
                            //Declaramos los objetos a insertar
                            Alumno alumnov3 = new Alumno();
                            GeneralesAlumno generalesAlumnov3 = new GeneralesAlumno();


                            bool existe = sql_context.Alumno.Any(a => a.AlId == alumnosv2.Crmid);

                            if (existe)
                            {
                                Console.WriteLine("Alumno ya existe");
                            }
                            else
                            {
                                // variables provisionales en lo que se termina la vista
                                string carrera = alumnosv2.Acarid;
                                string Estatus = alumnosv2.Coddescripcion;
                                string beca = alumnosv2.Bcid;
                                string mod = alumnosv2.Alumodalidad;




                                //se condiciona si el estatus viene nul se pone estatus eliminado

                                // si no se busca el estatus en la base sql y se asigna al alumnov3
                                int idEstatus = (from est in sql_context.EstatusList
                                                 where est.SlDescripcion == Estatus.Trim()
                                                 select est.SlStatusId).FirstOrDefault();

                                if (Estatus.Trim() == "" || string.IsNullOrEmpty(Estatus.Trim()) || Estatus.Trim() == "---")
                                {
                                    alumnov3.AlStatusActual = 44;
                                }
                                else if (Estatus.Trim() == "BAJA INTER ESCOLAR")
                                {

                                    alumnov3.AlStatusActual = 38;
                                }
                                else
                                {

                                    alumnov3.AlStatusActual = idEstatus;
                                }


                                // obtenemos el id de la carrera y lo asignamos al alumnov3
                                int idCarrera = (from car in sql_context.Carreras
                                                 where car.CarreraClave == carrera.Trim()
                                                 select car.Idcarrera).FirstOrDefault();

                                if (idCarrera == 0)
                                {
                                    alumnov3.AlCarrera = 74;

                                }
                                else
                                {
                                    alumnov3.AlCarrera = idCarrera;

                                }


                                if (beca == null || beca.Trim() == "" || string.IsNullOrEmpty(beca))
                                {
                                    alumnov3.AlBecaActual = 0;
                                    alumnov3.AlBecaParcialidad = 0;
                                }
                                else
                                {
                                    // obtenemos el id de la beca y lo asignamos al alumnov3
                                    int idBeca = (from bec in sql_context.Becas
                                                  where bec.BecasClave == beca.Trim()
                                                  select bec.BecasId).FirstOrDefault();
                                    alumnov3.AlBecaActual = idBeca;
                                    alumnov3.AlBecaParcialidad = (int)alumnosv2.Beparcialidades;
                                }



                                // validamos el idmodalidad
                                int idMod = (mod.Trim() == "PRE") ? 1 : (mod.Trim() == "EJE") ? 2 : (mod.Trim() == "ONL") ? 3 : 1;
                                alumnov3.AlModalidadActual = idMod;


                                // validamos la campaña
                                int idCamapana = (from cam in sql_context.Campana
                                                  where cam.Campanio == alumnosv2.Alureganio && cam.Campperiodo == alumnosv2.Aluregperiodo
                                                  select cam.Campid).FirstOrDefault();
                                alumnov3.AlCicloActual = idCamapana;



                                alumnov3.AlId = (long)alumnosv2.Crmid;
                                alumnov3.AlMatricula = alumnosv2.Alunocontrol;
                                alumnov3.AlFechaIngreso = DateTime.MinValue;
                                alumnov3.AlNombre = alumnosv2.Crmnombre;
                                alumnov3.AlApp = alumnosv2.Crmapaterno;


                                if (DBNull.Value.Equals(alumnosv2.Crmamaterno) || alumnosv2.Crmamaterno == null || alumnosv2.Crmamaterno == "")
                                {
                                    alumnov3.AlApm = "";
                                }
                                else
                                {
                                    alumnov3.AlApm = alumnosv2.Crmamaterno;

                                }

                                try
                                {

                                    alumnov3.AlFechaNac = (DateTime)alumnosv2.Crmfenac;
                                }
                                catch (Exception)
                                {
                                    alumnov3.AlFechaNac = DateTime.MinValue;
                                }

                                try
                                {
                                    if (alumnosv2.Aacfmail == null)
                                    {
                                        alumnov3.AlCorreoInst = "";

                                    }
                                    else
                                    {
                                        alumnov3.AlCorreoInst = alumnosv2.Aacfmail;

                                    }
                                }
                                catch (Exception)
                                {


                                }
                                alumnov3.AlSexo = false;

                                if (alumnosv2.Alureganio == 0 || DBNull.Value.Equals(alumnosv2.Alureganio) || alumnosv2.Alureganio == null)
                                {
                                    alumnov3.AlAnoPeriodoActual = 2000;
                                }
                                else
                                {
                                    alumnov3.AlAnoPeriodoActual = (short)alumnosv2.Alureganio;

                                }

                                alumnov3.AlEsquemaPagoActual = 1;



                                alumnov3.AlDocumentos = false;
                                //alumnov3.AlFechaInicioNivel = ((DateTime)alumnosv2.Crmmodfecha == null) ? DateTime.MinValue: (DateTime)alumnosv2.Crmmodfecha;
                                alumnov3.AlFechaInicioNivel = DateTime.MinValue;
                                alumnov3.AlCurp = "XXX";
                                alumnov3.AlBecaInscripcion = 0;
                                alumnov3.AlDescPromocion = 0;


                                if (alumnosv2.Aluregperiodo == 0 || DBNull.Value.Equals(alumnosv2.Aluregperiodo) || alumnosv2.Aluregperiodo == null)
                                {
                                    alumnov3.AlPeriodoActual = 1;
                                }
                                else
                                {
                                    alumnov3.AlPeriodoActual = (short)alumnosv2.Aluregperiodo;

                                }

                                alumnov3.AlCoutaAdmin = 4;
                                alumnov3.AlMontoDesc = 0;
                                alumnov3.AlSemestre = alumnosv2.Aacsemestreactual;

                                generalesAlumnov3.GaAlumnoId = (long)alumnosv2.Crmid;
                                generalesAlumnov3.AlId = (long)alumnosv2.Crmid;
                                generalesAlumnov3.GaNombreTutor = (alumnosv2.Aacfnomtutor == null) ? "" : alumnosv2.Aacfnomtutor.Trim();
                                generalesAlumnov3.GaApptutor = (alumnosv2.Aacfapellidop == null) ? "" : alumnosv2.Aacfapellidop.Trim();
                                generalesAlumnov3.GaApmtutor = (alumnosv2.Aacfapellidom == null) ? "" : alumnosv2.Aacfapellidom.Trim();
                                generalesAlumnov3.GaCalle = "";
                                generalesAlumnov3.GaNueroExt = "";
                                generalesAlumnov3.GaNumeroInterior = "";
                                if (alumnosv2.Aluestado == "" || string.IsNullOrEmpty(alumnosv2.Aluestado) || alumnosv2.Aluestado == " ")
                                {
                                    generalesAlumnov3.GaEstado = 21;
                                }
                                else
                                {

                                    try
                                    {
                                        generalesAlumnov3.GaEstado = Convert.ToInt32(alumnosv2.Aluestado.Trim());


                                        if (generalesAlumnov3.GaEstado > 32)
                                        {
                                            generalesAlumnov3.GaEstado = 21;

                                        }
                                    }
                                    catch (Exception)
                                    {

                                        generalesAlumnov3.GaEstado = 21;
                                    }
                                }

                                generalesAlumnov3.GaMunicipio = 1569;
                                //generalesAlumnov3.GaMunicipio = ((int)alumnosv2.Crmmunicipio == null) ? null : (int)alumnosv2.Crmmunicipio;




                                if (DBNull.Value.Equals(alumnosv2.Aacftelefonot) || alumnosv2.Aacftelefonot == null || alumnosv2.Aacftelefonot == "")
                                {
                                    generalesAlumnov3.GaTelefonoTutor = "";
                                }
                                else
                                {

                                    generalesAlumnov3.GaTelefonoTutor = (alumnosv2.Aacftelefonot == null) ? " " : alumnosv2.Aacftelefonot.Trim();
                                }


                                if (DBNull.Value.Equals(alumnosv2.Aacftelefono) || alumnosv2.Aacftelefono == null || alumnosv2.Aacftelefono == "")
                                {
                                    generalesAlumnov3.GaTelefonoAlumno = "";
                                }
                                else
                                {
                                    generalesAlumnov3.GaTelefonoAlumno = (alumnosv2.Aacftelefono == null) ? " " : alumnosv2.Aacftelefono.Trim();

                                }

                                if (DBNull.Value.Equals(alumnosv2.Aacfmail) || alumnosv2.Aacfmail == null || alumnosv2.Aacfmail.Trim() == "")
                                {
                                    generalesAlumnov3.GaCorreoAlternativo = "";
                                }
                                else
                                {
                                    generalesAlumnov3.GaCorreoAlternativo = (alumnosv2.Aacfmail == null) ? " " : alumnosv2.Aacfmail.Trim();

                                }


                                if (DBNull.Value.Equals(alumnosv2.Aacftelefono) || alumnosv2.Aacftelefono == null || alumnosv2.Aacftelefono.Trim() == "")
                                {
                                    generalesAlumnov3.GaTelefonoCasa = "";
                                }
                                else
                                {
                                    generalesAlumnov3.GaTelefonoCasa = (alumnosv2.Aacftelefono == null) ? " " : alumnosv2.Aacftelefono.Trim();

                                }

                                string addAlumno = "";
                                string addGenAlumno = "";

                                try
                                {
                                    sql_context.Alumno.Add(alumnov3);
                                    sql_context.SaveChanges();
                                    addAlumno = $"Alumno {alumnov3.AlMatricula.Trim()} transportado de la version 2 al 3";

                                }
                                catch (Exception e)
                                {
                                    addAlumno = "ERROR: " + e.Message;
                                }
                                Console.WriteLine(addAlumno);

                                try
                                {
                                    sql_context.GeneralesAlumno.Add(generalesAlumnov3);
                                    sql_context.SaveChanges();
                                    addGenAlumno = $"GeneralAlumno {alumnov3.AlMatricula} transportado de la version 2 al 3";

                                }
                                catch (Exception e)
                                {
                                    addAlumno = "ERROR: " + e.Message;

                                }

                                Console.WriteLine(addGenAlumno);
                            }
                        }
                        return "alumnosCargados";
                    }

                    string CargaConceptos()
                    {
                        List<Etlcatalogoconcepto> conceptosV2 = (from CCC2 in npgsql_context.Etlcatalogoconcepto select CCC2).ToList();
                        foreach (Etlcatalogoconcepto conceptov2 in conceptosV2)
                        {
                            CatalogoConceptos conceptosv3 = new CatalogoConceptos();
                            string clave = conceptov2.Cptipodoc;
                            string desc = conceptov2.Cpconcepto;

                            if (DBNull.Value.Equals(clave))
                            {
                                conceptosv3.ConClave = "GEN";
                                conceptosv3.ConTipoConcepto = 4;
                                conceptosv3.ConDescripcion = (DBNull.Value.Equals(desc)) ? "" : desc;
                                conceptosv3.ConRequisitoInscripcion = false;
                                conceptosv3.ConUsuid = 9;
                                conceptosv3.ConFechaRegistro = DateTime.Now;
                                sql_context.CatalogoConceptos.Add(conceptosv3);
                                sql_context.SaveChanges();
                                Console.WriteLine("Concepto registrado");
                                Console.WriteLine("<---------->");
                            }
                            else
                            {
                                if (clave.Trim() == "COL")
                                {

                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe la colegiatura");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 1;
                                        conceptosv3.ConDescripcion = "Colegiatura";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "ADM")
                                {

                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe la beca administrativa");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 3;
                                        conceptosv3.ConDescripcion = "Beca administrativa";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "CA")
                                {
                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe la cuota admin");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 1;
                                        conceptosv3.ConDescripcion = "Cuota Administrativa";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "CDM")
                                {
                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe la cuota admin");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 1;
                                        conceptosv3.ConDescripcion = "Cuota Administrativa";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "MOD")
                                {
                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe maestría");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 1;
                                        conceptosv3.ConDescripcion = "Maestria";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "CADM")
                                {
                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe cuota administrativa otoño");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 1;
                                        conceptosv3.ConDescripcion = "Cuota Administrativa Otoño";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "RU")
                                {
                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe saldo a favor");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 3;
                                        conceptosv3.ConDescripcion = "Saldo a Favor";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (clave.Trim() == "PROM")
                                {
                                    if (buscaConepto(clave.Trim()))
                                    {
                                        Console.WriteLine("Ya existe Descuento de Promoción");
                                    }
                                    else
                                    {
                                        conceptosv3.ConClave = clave.Trim();
                                        conceptosv3.ConTipoConcepto = 3;
                                        conceptosv3.ConDescripcion = "Descuento de Promoción";
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                }
                                else if (desc.Contains("BECA") || desc.Contains("Beca"))
                                {

                                    if (DBNull.Value.Equals(clave))
                                    {
                                        conceptosv3.ConClave = "GEN";
                                        conceptosv3.ConTipoConcepto = 4;
                                        conceptosv3.ConDescripcion = (DBNull.Value.Equals(desc)) ? "" : desc;
                                        conceptosv3.ConRequisitoInscripcion = false;
                                        conceptosv3.ConUsuid = 9;
                                        conceptosv3.ConFechaRegistro = DateTime.Now;
                                        sql_context.CatalogoConceptos.Add(conceptosv3);
                                        sql_context.SaveChanges();
                                        Console.WriteLine("Concepto registrado");
                                        Console.WriteLine("<---------->");
                                    }
                                    else
                                    {
                                        if (buscaConepto(clave.Trim()))
                                        {
                                            Console.WriteLine("Ya existe esta beca");

                                        }
                                        else
                                        {
                                            conceptosv3.ConClave = clave.Trim();
                                            conceptosv3.ConTipoConcepto = 3;
                                            conceptosv3.ConDescripcion = (DBNull.Value.Equals(desc)) ? "" : desc;
                                            conceptosv3.ConRequisitoInscripcion = false;
                                            conceptosv3.ConUsuid = 9;
                                            conceptosv3.ConFechaRegistro = DateTime.Now;
                                            sql_context.CatalogoConceptos.Add(conceptosv3);
                                            sql_context.SaveChanges();
                                            Console.WriteLine("Concepto registrado");
                                            Console.WriteLine("<---------->");
                                        }
                                    }

                                }
                                else
                                {

                                    conceptosv3.ConClave = clave.Trim();
                                    conceptosv3.ConTipoConcepto = 2;
                                    conceptosv3.ConDescripcion = (DBNull.Value.Equals(desc)) ? "" : desc;
                                    conceptosv3.ConRequisitoInscripcion = false;
                                    conceptosv3.ConUsuid = 9;
                                    conceptosv3.ConFechaRegistro = DateTime.Now;
                                    sql_context.CatalogoConceptos.Add(conceptosv3);
                                    sql_context.SaveChanges();
                                    Console.WriteLine("Concepto registrado");
                                    Console.WriteLine("<---------->");
                                }
                            }
                        }
                        return "Conceptos cargados";
                    }


                    bool buscaConepto(string clave)
                    {
                        return sql_context.CatalogoConceptos.Any(a => a.ConClave == clave.Trim());
                    }

                    string CargaListaPrecios()
                    {


                        // select de vista
                        List<Cclistapreciosconcepto> listaPreciosv2 = (from LP in npgsql_context.Cclistapreciosconcepto
                                                                       orderby LP.Lpconceptodes
                                                                       select new Cclistapreciosconcepto
                                                                       {
                                                                           Lpdivisionid = LP.Lpdivisionid,
                                                                           Lpconceptoid = LP.Lpconceptoid,
                                                                           Lpconceptodes = LP.Lpconceptodes,
                                                                           Lpconceptoprecio = LP.Lpconceptoprecio,
                                                                           Lpconceptofecini = LP.Lpconceptofecini,
                                                                           Lpconceptofecfin = LP.Lpconceptofecfin
                                                                       }).ToList();

                        int cont = 0;

                        foreach (Cclistapreciosconcepto listaPv2 in listaPreciosv2)
                        {
                            ListaPrecios listaPreciov3 = new ListaPrecios();
                            cont++;


                            if (cont == 165)
                            {
                                Console.WriteLine("Lista de precios existente");
                                Console.WriteLine("<---------->");
                            }
                            else
                            {
                                string conceptoLPv2 = listaPv2.Lpconceptoid.Trim();
                                string nivelLPv2 = listaPv2.Lpdivisionid.Trim();

                                if (!buscaConepto(conceptoLPv2.Trim()))
                                {

                                    CatalogoConceptos conceptosv3 = new CatalogoConceptos();
                                    conceptosv3.ConClave = conceptoLPv2.Trim();
                                    conceptosv3.ConTipoConcepto = 2;
                                    conceptosv3.ConDescripcion = (DBNull.Value.Equals(listaPv2.Lpconceptodes)) ? "" : listaPv2.Lpconceptodes;
                                    conceptosv3.ConRequisitoInscripcion = false;
                                    conceptosv3.ConUsuid = 9;
                                    conceptosv3.ConFechaRegistro = DateTime.Now;
                                    sql_context.CatalogoConceptos.Add(conceptosv3);
                                    sql_context.SaveChanges();
                                    Console.WriteLine("Concepto registrado");
                                    Console.WriteLine("<---------->");
                                }

                                int idConcepto = (from con in sql_context.CatalogoConceptos where con.ConClave == conceptoLPv2.Trim() select con.ConId).FirstOrDefault();


                                listaPreciov3.LpDescripcion = listaPv2.Lpconceptodes;
                                listaPreciov3.LpConcepto = idConcepto;
                                listaPreciov3.LpMonto = listaPv2.Lpconceptoprecio;
                                listaPreciov3.LpParcialidades = false;
                                listaPreciov3.LpCarrera = 74;
                                listaPreciov3.LpGeneracion = 2020;


                                if (listaPv2.Lpdivisionid == "" || DBNull.Value.Equals(listaPv2.Lpdivisionid))
                                {

                                    listaPreciov3.LpNivel = 0;
                                }
                                else
                                {
                                    int nivelId = (from NI in sql_context.Niveles where NI.NivelNombre == nivelLPv2.Trim() select NI.NivelId).FirstOrDefault();

                                    listaPreciov3.LpNivel = nivelId;

                                }

                                listaPreciov3.LpFechaInicio = listaPv2.Lpconceptofecini;
                                listaPreciov3.LpFechaFin = listaPv2.Lpconceptofecfin;
                                listaPreciov3.LpUsuid = 9;
                                listaPreciov3.LpFechaRegistro = DateTime.Now;
                                sql_context.ListaPrecios.Add(listaPreciov3);
                                sql_context.SaveChanges();
                                Console.WriteLine("Lista de precios registrada");
                                Console.WriteLine("<------------------------->");

                            }
                        }












                        return "Listas de precios cargadas";
                    }

                    string CargaDetalleCuentas()
                    {

                        List<Etlcuentasdetalle> detallesv2 = (from ccdv2 in npgsql_context.Etlcuentasdetalle where ccdv2.Cpid > 2928 orderby ccdv2.Cpid ascending select ccdv2).Take(2500).ToList();

                        foreach (Etlcuentasdetalle detallev2 in detallesv2)
                        {
                            DetalleCuentaPorCobrar detalleCuentav3 = new DetalleCuentaPorCobrar();


                            //int idDTCuenta = (from dcpc in sql_context.DetalleCuentaPorCobrar where dcpc.DcpcId == detallev2.Cpid select dcpc.DcpcId).Any();


                            bool existe = sql_context.DetalleCuentaPorCobrar.Any(a => a.DcpcId == detallev2.Cpid);



                            if (detallev2.Cpid != 0 || existe)
                            {
                                detalleCuentav3.DcpcId = (int)detallev2.Cpid;
                                detalleCuentav3.DcpcCuentaId = (int)detallev2.Cpnodoc;
                                detalleCuentav3.DcpcDescripcion = detallev2.Cpconcepto;
                                detalleCuentav3.DcpcDocLinea = detallev2.Cpdoclinea;
                                detalleCuentav3.DcpcImporteTotal = (decimal)detallev2.Cpimportetotal;
                                detalleCuentav3.DcpcImportePendiente = (decimal)detallev2.Cpimportepend;
                                detalleCuentav3.DcpcFechaVencimiento = (DateTime)detallev2.Cpfechaven;
                                detalleCuentav3.DcpcFechaCierreCuenta = DateTime.MinValue;
                                detalleCuentav3.DcpcReferenciaCuentaDetalle = (int)detallev2.Cpidref;
                                detalleCuentav3.DcpcEstatus = (detallev2.Cpestatus == 1) ? 21 : 26;

                                if (detallev2.Aluid == 0 || DBNull.Value.Equals(detallev2.Aluid) || detallev2.Aluid == null)
                                {

                                    detalleCuentav3.DcpcUsuid = 0;
                                }
                                else
                                {

                                    detalleCuentav3.DcpcUsuid = (long)detallev2.Aluid;
                                }

                                detalleCuentav3.DcpcFechaRegistro = (DateTime)detallev2.Cpfecreg;


                                try
                                {
                                    sql_context.DetalleCuentaPorCobrar.Add(detalleCuentav3);
                                    sql_context.SaveChanges();
                                    Console.WriteLine("Detalle cuenta por cobrar registrado");
                                    Console.WriteLine("<---------------------------------->");
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("ERROR: " + e.Message);
                                    Console.WriteLine("<---------------------------------->");
                                    throw;
                                }
                            }
                            else
                            {
                                Console.WriteLine("Cuenta por cobrar existente");
                                Console.WriteLine("<---------------------------------->");
                            }

                        }



                        return "Cuentas cargadas";
                    }



                }

            }




        }
    }
}
