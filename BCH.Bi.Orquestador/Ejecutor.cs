using System;

namespace BCH.Bi.Orquestador
{
    internal class Ejecutor
    {
        internal string ConnectionString { get; set; }

        internal void RunProcess(Proceso proceso)
        {
            if (proceso.TipoArchivo == "DTSX")
                RunDtsxProcess(proceso);
            if (proceso.TipoArchivo == "EXE")
                RunExeProcess(proceso);
        }

        void RunExeProcess(Proceso proceso)
        {
            System.Diagnostics.ProcessStartInfo startInfo = new System.Diagnostics.ProcessStartInfo(proceso.RutaProceso);

            Run(startInfo, proceso);
        }

        void RunDtsxProcess(Proceso proceso)
        {
            System.Diagnostics.ProcessStartInfo startInfo = new System.Diagnostics.ProcessStartInfo(@"c:\Program Files\Microsoft SQL Server\110\DTS\Binn\DTExec.exe"
                , string.Format(@"/File ""{0}"" /Conf ""{1}""", proceso.RutaProceso, proceso.ArchivoConfiguracion));

            Run(startInfo, proceso);
        }

        void Run(System.Diagnostics.ProcessStartInfo startInfo, Proceso proceso)
        {
            DateTime fecInicio, fecTermino;
            fecInicio = DateTime.Now;

            System.Diagnostics.Process proc = new System.Diagnostics.Process
            {
                StartInfo = startInfo
            };
            proc.Start();
            proc.WaitForExit();

            fecTermino = DateTime.Now;

            int output = proc.ExitCode;

            Orquestador.SetExecutionStatus(proceso, fecInicio, fecTermino, output, ConnectionString);

            if (output == 0)
                Orquestador.RunProcessExecution(int.Parse(proceso.IdProceso.ToString()), ConnectionString);
        }
    }
}
