using ETL.Data.NpgSql;
using System;
using Microsoft.EntityFrameworkCore;
using ETL.Data.Sql;
using System.Linq;

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
                    // se hace la consulta de la vista
                    // select vista


                    //Declaramos los objetos a insertar
                    Alumno alumnov3 = new Alumno();
                    GeneralesAlumno generalesAlumnov3 = new GeneralesAlumno();

                    // variables provisionales en lo que se termina la vista
                    string carrera = "ADE-PL10";
                    string Estatus = "PREINSCRITO";
                    string beca = "ERAC-RT";

                    // foreach de la vista para asignar propiedades
                    //foreach           --->


                    //se condiciona si el estatus viene nul se pone estatus eliminado
                        
                    // si no se busca el estatus en la base sql y se asigna al alumnov3
                    int idEstatus = (from est in sql_context.EstatusList 
                                     where est.SlDescripcion == Estatus.Trim() 
                                     select est.SlStatusId).FirstOrDefault();
                    alumnov3.AlStatusActual = idEstatus;


                    // obtenemos el id de la carrera y lo asignamos al alumnov3
                    int idCarrera = (from car in sql_context.Carreras 
                                     where car.CarreraClave == carrera.Trim() 
                                     select car.Idcarrera).FirstOrDefault();
                    alumnov3.AlCarrera = idCarrera;


                    // obtenemos el id de la beca y lo asignamos al alumnov3
                    int idBeca = (from bec in sql_context.Becas 
                                  where bec.BecasClave == beca.Trim() 
                                  select bec.BecasId).FirstOrDefault();
                    alumnov3.AlBecaActual = idBeca;



                    Talumno talumno = (from talu in npgsql_context.Talumno select talu).FirstOrDefault();

                    Console.WriteLine(talumno.Alufacemail);
                    Console.ReadKey();
                }
                
            }

        }
    }
}
