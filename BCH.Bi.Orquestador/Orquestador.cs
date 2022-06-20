using System;
using System.Data.SqlClient;
using System.Collections;
using BCH.Data.Conexion;
using System.Data;

namespace BCH.Bi.Orquestador
{
    public class Orquestador
    {
        public string ConnectionString { get; set; }

        public void RunProcessAfterExecution()
        {
            RunProcessExecution(0, ConnectionString);
        }

        internal static void RunProcessExecution(int idProceso, string cnnStr)
        {
            DbObject cns = new DbObject(cnnStr);
            ArrayList arr = new ArrayList();
            DataTable tb = new DataTable();
            SqlParameter par = new SqlParameter
            {
                ParameterName = "@ID_PROCESO",
                SqlDbType = System.Data.SqlDbType.BigInt,
                Value = idProceso
            };

            arr.Add(par);

            tb = cns.ExecuteSPDataTable("USP_GET_PROCESOS_MATUTINO", arr);

            foreach (DataRow item in tb.Rows)
            {
                int processId;
                string locationPkg;
                string locationConfigFile;
                string packageType;

                processId = int.Parse(item["ID_PROCESO"].ToString());
                locationPkg = item["RUTA_PROCESO"].ToString();
                locationConfigFile = item["RUTA_CONFIG"].ToString();
                packageType = item["TIPO_PROCESO"].ToString();
                string processName = item["NOMBRE_PROCESO"].ToString();

                Proceso proceso = new Proceso
                {
                    IdProceso = processId,
                    RutaProceso = locationPkg,
                    ArchivoConfiguracion = locationConfigFile,
                    NombreProceso = processName,
                    TipoArchivo = packageType
                };

                Ejecutor ejecutor = new Ejecutor
                {
                    ConnectionString = cnnStr
                };                

                System.Threading.Thread thread = new System.Threading.Thread(() => ejecutor.RunProcess(proceso));
                thread.Start();
            }
        }

        internal static void SetExecutionStatus(Proceso proceso, DateTime inicio, DateTime termino, int exitCode, string cnnStr)
        {
            DbObject cns = new DbObject(cnnStr);
            ArrayList arr = new ArrayList();
            DataTable tb = new DataTable();

            SqlParameter par = new SqlParameter
            {
                ParameterName = "@ID_PROCESO",
                SqlDbType = System.Data.SqlDbType.BigInt,
                Value = proceso.IdProceso
            };

            arr.Add(par);

            par = new SqlParameter
            {
                ParameterName = "@FECHA_COMIENZO",
                SqlDbType = System.Data.SqlDbType.DateTime,
                Value = inicio
            };

            arr.Add(par);

            par = new SqlParameter
            {
                ParameterName = "@FECHA_TERMINO",
                SqlDbType = System.Data.SqlDbType.DateTime,
                Value = termino
            };

            arr.Add(par);

            par = new SqlParameter
            {
                ParameterName = "@RESULTADO",
                SqlDbType = System.Data.SqlDbType.VarChar,
                Value = "ExitCode : " + exitCode.ToString()
            };

            arr.Add(par);

            cns.ExecuteSPScalar("usp_TB_MALLA_PROCESO_MATUTINO_LOG_Insert", arr);
        }
    }
}