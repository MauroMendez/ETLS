﻿// <auto-generated> This file has been auto generated by EF Core Power Tools. </auto-generated>
#nullable disable
using System;
using System.Collections.Generic;

namespace ETL.Data.Sql
{
    public partial class DocumentoAlumno
    {
        public long IdDocumentoAl { get; set; }
        public long AlId { get; set; }
        public long IdDocumento { get; set; }
        public int Addcestatus { get; set; }

        public virtual EstatusList AddcestatusNavigation { get; set; }
        public virtual Alumno Al { get; set; }
        public virtual Documentos IdDocumentoNavigation { get; set; }
    }
}