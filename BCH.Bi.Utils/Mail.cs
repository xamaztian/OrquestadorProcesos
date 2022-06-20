namespace BCH.Bi.Utils
{
    public static class Mail
    {

        /// <summary>
        /// Método encargado de enviar correos.
        /// </summary>
        /// <param name="asunto">Asunto.</param>
        /// <param name="mensaje">Mensaje (puede ser texto HTML).</param>
        /// <param name="de">Remitente.</param>
        /// <param name="para">Destinatario (para más de uno, separar por punto y coma).</param>
        /// <param name="CC">Con Copia (para más de uno, separar por punto y coma).</param>
        /// <param name="bCC">Con Copia Oculta (para más de uno, separar por punto y coma).</param>
        /// <param name="adjunto">Ruta del archivo adjunto (para más de uno, separar por punto y coma).</param>
        /// <param name="ipServidorCorreos">IP de servidor de correos.</param>
        public static void enviarCorreo(string asunto, string mensaje, string de, string para, string CC, string bCC, string adjunto, string ipServidorCorreos)
        {
            System.Net.Mail.MailMessage msj = new System.Net.Mail.MailMessage();
            msj.From = new System.Net.Mail.MailAddress(de);
            msj.Subject = asunto;
            msj.Body = mensaje;
            msj.IsBodyHtml = true;

            foreach (string destinatario in para.Split(';'))
            {
                msj.To.Add(destinatario.Trim());
            }

            if (CC != null && !string.IsNullOrEmpty(CC))
            {
                foreach (string Copiado in CC.Split(';'))
                    msj.CC.Add(Copiado.Trim());
            }

            if (bCC != null && !string.IsNullOrEmpty(bCC))
            {
                foreach (string CopiadoOculto in bCC.Split(';'))
                    msj.Bcc.Add(CopiadoOculto.Trim());
            }

            if (adjunto != null && !string.IsNullOrEmpty(adjunto))
            {
                foreach (string nombreArchivo in adjunto.Split(';'))
                    msj.Attachments.Add(new System.Net.Mail.Attachment(nombreArchivo.Trim()));
            }

            if (ipServidorCorreos == null)
                ipServidorCorreos = System.Configuration.ConfigurationManager.AppSettings["IpServidorCorreo"].Trim();

            System.Net.Mail.SmtpClient enviar = new System.Net.Mail.SmtpClient();
            enviar.Host = ipServidorCorreos;
            enviar.Send(msj);

            enviar = null;
            msj.Dispose();
        }
    }
}
