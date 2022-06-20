using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BCH.Bi.Orquestador
{
    internal class Proceso
    {
        public long IdProceso { get; set; }
        public string NombreProceso { get; set; }
        public string RutaProceso { get; set; }
        public string ArchivoConfiguracion { get; set; }
        public string TipoArchivo { get; set; }
    }
}
