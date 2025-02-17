﻿// <auto-generated> This file has been auto generated by EF Core Power Tools. </auto-generated>
#nullable disable
using System;
using System.Collections.Generic;

namespace ETL.Data.Sql
{
    public partial class DatosFacturacion
    {
        public DatosFacturacion()
        {
            Facturas = new HashSet<Facturas>();
        }

        public string DfRfc { get; set; }
        public string DfNombre { get; set; }
        public string DfApp { get; set; }
        public string DfApm { get; set; }
        public string DfCalle { get; set; }
        public string DfNoExt { get; set; }
        public string DfNoInt { get; set; }
        public string DfColonia { get; set; }
        public int DfEstado { get; set; }
        public int DfMunicipio { get; set; }
        public string DfUso { get; set; }
        public string DfEmail { get; set; }

        public virtual Estados DfEstadoNavigation { get; set; }
        public virtual Municipios DfMunicipioNavigation { get; set; }
        public virtual UsosFacturacion DfUsoNavigation { get; set; }
        public virtual ICollection<Facturas> Facturas { get; set; }
    }
}