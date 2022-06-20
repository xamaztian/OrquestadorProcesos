using System;
using WinSCP;
using System.Data.SqlClient;
using System.Collections;
using BCH.Data.Conexion;
using System.Data;
using BCH.Bi.Utils;

namespace AutomatizacionCargaGestiones
{
    class Program
    {
        public static string RUTA_GESTIONES = System.Configuration.ConfigurationManager.AppSettings["RutaGestiones"];
        public static readonly string S3_COMPLEMENTO = @"01_S3\";
        public static readonly string IBR_COMPLEMENTO = @"02_IBR\";
        public static readonly string RUTA_LOCAL_PROCESO = System.Configuration.ConfigurationManager.AppSettings["RutaTemporales"];
        public static string connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["cnBanchdb13IntNegocios"].ConnectionString;
        public static string connectionStringTLMK = System.Configuration.ConfigurationManager.ConnectionStrings["cnBanchdb13IntNegociosTLMK"].ConnectionString;

        static void Main(string[] args)
        {
            SqlParameter par = new SqlParameter
            {
                ParameterName = "@CANAL",
                SqlDbType = System.Data.SqlDbType.VarChar,
                Value = "S3"
            };
            ArrayList arr = new ArrayList
            {
                par
            };
            DateTime fechaEjecucion;
            string canal = "";
            string rutaLocalTmp;
            string rutaOrigen;
            string usuario, pass;
            bool _switchRunCargaGestiones = false;
            System.Threading.Thread prcRecupero;
            System.Threading.Thread prcRecuperoInbound;

            ArrayList lstProcAsync = new ArrayList();

            DataTable table = Credenciales();
            usuario = table.Rows[0]["usuario"].ToString();
            pass = table.Rows[0]["pass"].ToString();

            try
            {
                //SF.Utils.Impersonation impersonation = new SF.Utils.Impersonation();
                //Impersonation impersonation = new Impersonation();
                string rutaDestinoS3;
                canal = "S3";
                //impersonation.Impersonations("bch", usuario, pass);

                try
                {
                    fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                    rutaOrigen = "/home/externo/informes/" + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");
                    rutaDestinoS3 = RUTA_GESTIONES + @"\" + S3_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                    if (!System.IO.Directory.Exists(rutaDestinoS3))
                        System.IO.Directory.CreateDirectory(rutaDestinoS3);

                    ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoS3);

                    rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");

                    if (!ValidaEstadoCarga(fechaEjecucion, "S3"))
                    {
                        _switchRunCargaGestiones = true;
                        ActualizaEstadoCarga(fechaEjecucion
                            , "S3"
                            , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, S3_COMPLEMENTO, "TBL_GESTIONES_CALL_S3_TMP", "*GESTIONES_SEGUROS_S3.txt", "DIARIA_S3", "S3"));
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

                /******************FIDELIZACIÓN***************************/

                try
                {
                    par = new SqlParameter
                    {
                        ParameterName = "@CANAL",
                        SqlDbType = System.Data.SqlDbType.VarChar,
                        Value = "FIDELIZACION"
                    };
                    arr = new ArrayList
                {
                    par
                };

                    fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                    rutaOrigen = "/home/externo/informes/" + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");
                    rutaDestinoS3 = RUTA_GESTIONES + @"\" + S3_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                    if (!System.IO.Directory.Exists(rutaDestinoS3))
                        System.IO.Directory.CreateDirectory(rutaDestinoS3);

                    ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoS3);

                    rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");


                    if (!ValidaEstadoCarga(fechaEjecucion, "FIDELIZACION"))
                    {
                        ActualizaEstadoCarga(fechaEjecucion
                            , "FIDELIZACION"
                            , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, S3_COMPLEMENTO, "TBL_GESTIONES_CALL_FIDE_TMP", "*Fidelizacion*.txt", "DIARIA_FIDE", "FIDELIZACION"));
                    }
                }
                catch (Exception)
                {

                }

                /******************MIGRACION***************************/

                try
                {
                    par = new SqlParameter
                    {
                        ParameterName = "@CANAL",
                        SqlDbType = System.Data.SqlDbType.VarChar,
                        Value = "MIGRACION"
                    };
                    arr = new ArrayList
                {
                    par
                };

                    fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                    rutaOrigen = "/home/externo/informes/" + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");
                    rutaDestinoS3 = RUTA_GESTIONES + @"\" + S3_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                    if (!System.IO.Directory.Exists(rutaDestinoS3))
                        System.IO.Directory.CreateDirectory(rutaDestinoS3);

                    ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoS3);

                    rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");

                    if (!ValidaEstadoCarga(fechaEjecucion, "MIGRACION"))
                    {
                        ActualizaEstadoCarga(fechaEjecucion
                            , "MIGRACION"
                            , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, S3_COMPLEMENTO, "TBL_GESTIONES_CALL_FIDE_TMP", "*Migra*.txt", "DIARIA_FIDE", "MIGRACION"));

                    }
                }
                catch (Exception)
                {

                }

                /******************RECUPERO_OUTBOUND***************************/

                try
                {
                    par = new SqlParameter
                    {
                        ParameterName = "@CANAL",
                        SqlDbType = System.Data.SqlDbType.VarChar,
                        Value = "RECUPERO_OUTBOUND"
                    };
                    arr = new ArrayList
                {
                    par
                };

                    fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                    rutaOrigen = "/home/externo/informes/" + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");
                    rutaDestinoS3 = RUTA_GESTIONES + @"\" + S3_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                    if (!System.IO.Directory.Exists(rutaDestinoS3))
                        System.IO.Directory.CreateDirectory(rutaDestinoS3);

                    ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoS3);

                    rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");

                    if (!ValidaEstadoCarga(fechaEjecucion, "RECUPERO_OUTBOUND"))
                    {
                        ActualizaEstadoCarga(fechaEjecucion
                            , "RECUPERO_OUTBOUND"
                            , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, S3_COMPLEMENTO, "TBL_GESTIONES_RECUPERO_TMP", "*recupero*folios_S3.txt", "RECUPERO", "RECUPERO_OUTBOUND"));

                        prcRecupero = new System.Threading.Thread(() => new DbObject(connectionStringTLMK).RunProcedure("SP_CARGA_RESULTANTE_RECUPERACION", null));
                        lstProcAsync.Add(prcRecupero);
                        prcRecupero.Start();
                    }
                }
                catch (Exception)
                {

                }

                /******************RECUPERO_INBOUND***************************/

                try
                {
                    par = new SqlParameter
                    {
                        ParameterName = "@CANAL",
                        SqlDbType = System.Data.SqlDbType.VarChar,
                        Value = "RECUPERO_INBOUND"
                    };
                    arr = new ArrayList
                {
                    par
                };

                    fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                    rutaOrigen = "/home/externo/informes/" + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");
                    rutaDestinoS3 = RUTA_GESTIONES + @"\" + S3_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                    if (!System.IO.Directory.Exists(rutaDestinoS3))
                        System.IO.Directory.CreateDirectory(rutaDestinoS3);

                    ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoS3);

                    rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");

                    if (!ValidaEstadoCarga(fechaEjecucion, "RECUPERO_INBOUND"))
                    {
                        ActualizaEstadoCarga(fechaEjecucion
                            , "RECUPERO_INBOUND"
                            , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, S3_COMPLEMENTO, "TBL_GESTIONES_RECUPERO_TMP", "*recupero*inbound*.txt", "RECUPERO", "RECUPERO_INBOUND"));

                        prcRecuperoInbound = new System.Threading.Thread(() => new DbObject(connectionStringTLMK).RunProcedure("SP_CARGA_RESULTANTE_RECUPERACION_INBOUND", null));
                        prcRecuperoInbound.Start();
                        lstProcAsync.Add(prcRecuperoInbound);
                    }
                }
                catch (Exception)
                {

                }

                /******************RETENCION***************************/

                try
                {
                    par = new SqlParameter
                    {
                        ParameterName = "@CANAL",
                        SqlDbType = System.Data.SqlDbType.VarChar,
                        Value = "RETENCION"
                    };
                    arr = new ArrayList
                {
                    par
                };

                    fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                    rutaOrigen = "/home/externo/informes/" + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");
                    rutaDestinoS3 = RUTA_GESTIONES + @"\" + S3_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                            fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                    if (!System.IO.Directory.Exists(rutaDestinoS3))
                        System.IO.Directory.CreateDirectory(rutaDestinoS3);

                    ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoS3);

                    rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");

                    if (!ValidaEstadoCarga(fechaEjecucion, "RETENCION"))
                    {
                        ActualizaEstadoCarga(fechaEjecucion
                            , "RETENCION"
                            , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, S3_COMPLEMENTO, "TBL_GESTIONES_RETENCION_TMP", "*retencion*folios*.txt", "RETENCION", "RETENCION"));
                    }
                }
                catch (Exception)
                {

                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            try
            {
                par = new SqlParameter
                {
                    ParameterName = "@CANAL",
                    SqlDbType = System.Data.SqlDbType.VarChar,
                    Value = "IBR"
                };
                arr = new ArrayList
            {
                par
            };
                fechaEjecucion = DateTime.Parse(new DbObject(connectionString).ExecuteSPScalar("USP_GET_PENDIENTE_CARGA_TLMK", arr).ToString());

                canal = "IBR";
                rutaOrigen = "/home/nfsmnt/nfsmnt/sftp_BCH_Gestiones/RESULTANTE_GESTIONES/" + fechaEjecucion.Year.ToString() + "/" +
                        fechaEjecucion.Month.ToString("0#") + "/" + fechaEjecucion.Day.ToString("0#");
                string rutaDestinoIBR = RUTA_GESTIONES + @"\" + IBR_COMPLEMENTO + fechaEjecucion.Year.ToString() +
                        fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\";

                if (!System.IO.Directory.Exists(rutaDestinoIBR))
                    System.IO.Directory.CreateDirectory(rutaDestinoIBR);

                ExtraeArchivosCanal(canal, rutaOrigen, rutaDestinoIBR);

                rutaLocalTmp = RUTA_LOCAL_PROCESO + fechaEjecucion.Year.ToString() +
                   fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#");

                if (!System.IO.Directory.Exists(rutaLocalTmp))
                    System.IO.Directory.CreateDirectory(rutaLocalTmp);
                else
                    System.IO.Directory.Delete(rutaLocalTmp, true);

                CargaGestionCanal(rutaLocalTmp, fechaEjecucion, IBR_COMPLEMENTO, "TBL_GESTIONES_CALL_IBR_TMP", "*.txt", "DIARIA_IBR", "IBR");

                if (!ValidaEstadoCarga(fechaEjecucion, "IBR"))
                {
                    _switchRunCargaGestiones = true;

                    ActualizaEstadoCarga(fechaEjecucion
                        , "IBR"
                        , CargaGestionCanal(rutaLocalTmp, fechaEjecucion, IBR_COMPLEMENTO, "TBL_GESTIONES_CALL_IBR_TMP", "*.txt", "DIARIA_IBR", "IBR"));
                }
            }
            catch (Exception)
            {
                
            }

            if (_switchRunCargaGestiones)
            {
                System.Threading.Thread prcResumenGestiones = new System.Threading.Thread(() => new DbObject(connectionStringTLMK).RunProcedure("SP_CARGA_RESULTANTE", null));
                prcResumenGestiones.Start();
                lstProcAsync.Add(prcResumenGestiones);
            }

            foreach (System.Threading.Thread item in lstProcAsync)
            {
                item.Join();
            }

            BCH.Bi.Orquestador.Orquestador orquestador = new BCH.Bi.Orquestador.Orquestador
            {
                ConnectionString = connectionString
            };
         
            orquestador.RunProcessAfterExecution();
        }

        #region CARGA_CANALES
        private static void ExtraeArchivosCanal(string canal, string rutaOrigen, string rutaDestino)
        {
            using (Session session = new Session())
            {
                session.Open(CargaArchivos(canal));
                TransferOptions transferOptions = new TransferOptions();
                transferOptions.TransferMode = TransferMode.Binary;

                if (System.IO.Directory.Exists(rutaDestino))
                    System.IO.Directory.Delete(rutaDestino, true);

                System.IO.Directory.CreateDirectory(rutaDestino);

                TransferOperationResult transferResult;
                transferResult =
                    session.GetFiles(rutaOrigen + @"/*", rutaDestino, false, transferOptions);

                // Throw on any error
                transferResult.Check();

                // Print results
                foreach (TransferEventArgs transfer in transferResult.Transfers)
                {
                    Console.WriteLine("Descarga de archivo {0}", transfer.FileName);
                }
            }
        }

        static SessionOptions CargaArchivos(string parametroCanalCarga)
        {
            SessionOptions sessionOptions = null;
            switch (parametroCanalCarga)
            {
                case "S3":
                    sessionOptions = new SessionOptions
                    {
                        Protocol = Protocol.Sftp,
                        HostName = "190.215.48.198",
                        UserName = "externo",
                        Password = "S3Chile.",
                        SshHostKeyFingerprint = "ssh-rsa 2048 88:87:8f:d9:3e:c9:6d:2e:57:8d:d7:bf:4f:44:81:70"
                    };
                    break;
                case "IBR":
                    sessionOptions = new SessionOptions
                    {
                        Protocol = Protocol.Sftp,
                        PortNumber = 2222,
                        HostName = "190.196.182.125",
                        UserName = "sftp_bch_gestion",
                        Password = "$Sftp.Gesti0n.2018",
                        SshHostKeyFingerprint = "ssh-ed25519 256 38:16:96:d2:8a:9d:95:18:ce:ab:19:ad:62:8f:78:bf"
                    };
                    break;
                default:
                    break;
            }

            return sessionOptions;
        }

        private static bool CargaGestionCanal(string rutaProcesoLocal, DateTime fechaEjecucion, string complementoRuta, string tablaDestino, string patronBusqueda, string procDestino, string tipoDestino)
        {
            bool estadoCarga = true;
            string rutaProcesoLocalTmp = rutaProcesoLocal + "\\" + tipoDestino;

            if (System.IO.Directory.Exists(rutaProcesoLocalTmp))
                System.IO.Directory.Delete(rutaProcesoLocalTmp, true);

            System.IO.Directory.CreateDirectory(rutaProcesoLocalTmp);

            try
            {
                string[] archivos = System.IO.Directory.GetFiles(RUTA_GESTIONES + @"\" + complementoRuta + fechaEjecucion.Year.ToString() +
                    fechaEjecucion.Month.ToString("0#") + fechaEjecucion.Day.ToString("0#") + "\\", patronBusqueda);

                ArrayList hebras = new ArrayList();
                GeneraCargas(tipoDestino, rutaProcesoLocalTmp, archivos, ref hebras);

                foreach (System.Diagnostics.Process item in hebras)
                    item.WaitForExit();

                hebras = new ArrayList();

                GeneraCargas(tipoDestino, rutaProcesoLocalTmp, archivos, ref hebras);

                foreach (System.Diagnostics.Process item in hebras)
                    item.WaitForExit();

                System.Diagnostics.Process.Start("CMD.exe", string.Format(@"/C bcp dbo.{1} format nul -n -c -t ^| -S banchdb13 -d DB_GESTIONES_TLMK -U inteligencianegocios -P B4nch1l3 -f {0}\{1}.fmt", rutaProcesoLocalTmp, tablaDestino)).WaitForExit();

                hebras.Clear();
                hebras = null;

                string[] archivosCargaSql = System.IO.Directory.GetFiles(rutaProcesoLocalTmp + "\\", "*.txt");

                foreach (string item in archivosCargaSql)
                {
                    new DbObject(connectionString).RunQuery("TRUNCATE TABLE [DB_GESTIONES_TLMK].[dbo].[" + tablaDestino + "]");

                    System.IO.FileInfo fileInfo = new System.IO.FileInfo(item);
                    string cmdText = string.Format(@"/C bcp dbo.{1} in ""{0}"" -S banchdb13 -d DB_GESTIONES_TLMK -U inteligencianegocios -P B4nch1l3 -f {2}\{1}.fmt", item, tablaDestino, rutaProcesoLocalTmp);

                    System.Diagnostics.ProcessStartInfo startInfo = new System.Diagnostics.ProcessStartInfo("CMD.exe", cmdText);
                    //startInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Maximized;

                    System.Diagnostics.Process proc = new System.Diagnostics.Process
                    {
                        StartInfo = startInfo
                    };

                    proc.Start();
                    proc.WaitForExit();

                    new DbObject(connectionStringTLMK).RunProcedure("SP_CARGA_GESTION_" + procDestino, null);

                    System.Threading.Thread.Sleep(500);
                }
            }
            catch (Exception e)
            {
                estadoCarga = false;
                Console.WriteLine(e.Message);
            }

            return estadoCarga;
        }

        private static void GeneraCargas(string tipoDestino, string rutaProcesoLocalTmp, string[] archivos,ref ArrayList hebras)
        {
            foreach (string item in archivos)
            {
                System.IO.FileInfo fileInfo = new System.IO.FileInfo(item);

                if (!ValidaCargaArchivo(tipoDestino, fileInfo.Name) && fileInfo.Length > 1024)
                {
                    string cmdText = string.Format(@"/C for /f ""skip=1 usebackq tokens=*"" %h in (""{1}"") do echo %h^|{2} >> ""{0}\{2}""", rutaProcesoLocalTmp, item, fileInfo.Name);

                    System.Diagnostics.ProcessStartInfo startInfo = new System.Diagnostics.ProcessStartInfo("CMD.exe", cmdText);
                    startInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;

                    System.Diagnostics.Process proc = new System.Diagnostics.Process
                    {
                        StartInfo = startInfo
                    };

                    proc.Start();
                    hebras.Add(proc);
                }
            }
        }

        private static bool ValidaCargaArchivo(string param, string archivo)
        {
            Console.WriteLine("Validando si el archivo se encuentra cargado...");

            DbObject cns = new DbObject(connectionString);
            ArrayList arr = new ArrayList();
            SqlParameter par = new SqlParameter
            {
                ParameterName = "@CANAL",
                SqlDbType = System.Data.SqlDbType.VarChar,
                Value = param
            };

            arr.Add(par);

            par = new SqlParameter
            {
                ParameterName = "@ARCHIVO",
                SqlDbType = System.Data.SqlDbType.VarChar,
                Value = archivo
            };

            arr.Add(par);

            Console.WriteLine("Archivo : " + archivo);

            var x = cns.ExecuteSPScalar("VALIDA_ARCHIVO", arr);
            try
            {
                if (x != null)
                {
                    Console.WriteLine("Encontrado");
                    Console.WriteLine("----------");
                    return true;
                }
                else
                {
                    Console.WriteLine("----------");
                    return false;
                }
            }
            catch (Exception)
            {
                Console.WriteLine("----------");
                return false;
            }
        
        }

        private static void ActualizaEstadoCarga(DateTime fechaCarga, string canal, bool estado)
        {
            DbObject cns = new DbObject(connectionStringTLMK);
            ArrayList arr = new ArrayList();
            SqlParameter par = new SqlParameter
            {
                ParameterName = "@CANAL",
                SqlDbType = System.Data.SqlDbType.VarChar,
                Value = canal
            };

            arr.Add(par);
            par = new SqlParameter
            {
                ParameterName = "@FECHA",
                SqlDbType = System.Data.SqlDbType.Date,
                Value = fechaCarga
            };

            arr.Add(par);
            par = new SqlParameter
            {
                ParameterName = "@ESTADO",
                SqlDbType = System.Data.SqlDbType.Bit,
                Value = estado
            };

            arr.Add(par);

            cns.ExecuteSPScalar("USP_SET_CARGA_CANAL", arr);
        }

        private static bool ValidaEstadoCarga(DateTime fechaCarga, string canal)
        {
            bool estadoCarga = false;
            DbObject cns = new DbObject(connectionStringTLMK);
            ArrayList arr = new ArrayList();
            SqlParameter par = new SqlParameter
            {
                ParameterName = "@CANAL",
                SqlDbType = System.Data.SqlDbType.VarChar,
                Value = canal
            };

            arr.Add(par);
            par = new SqlParameter
            {
                ParameterName = "@FECHA",
                SqlDbType = System.Data.SqlDbType.Date,
                Value = fechaCarga
            };

            arr.Add(par);

            estadoCarga = (bool)(cns.ExecuteSPScalar("USP_VALIDA_CARGA_CANAL", arr));

            return estadoCarga;
        }

        private static DataTable Credenciales()
        {
            DbObject cns = new DbObject(connectionString);
            DataTable tb;

            tb = cns.ExecuteSPDataTable("usr_psw", null);

            return tb;
        }

        #endregion CARGA_CANALES
       
    }
}
