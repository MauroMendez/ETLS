﻿// <auto-generated> This file has been auto generated by EF Core Power Tools. </auto-generated>
#nullable disable
using System;
using System.Collections.Generic;

namespace ETL.Data.Sql
{
    public partial class BitacoraGral
    {
        public BitacoraGral()
        {
            BitacoraDetalle = new HashSet<BitacoraDetalle>();
        }

        public long Bgid { get; set; }
        public int BgmodoloId { get; set; }
        public string BgtipoMov { get; set; }
        public string Bgcomentario { get; set; }
        public DateTime? Bgfecha { get; set; }
        public string Bgproceso { get; set; }
        public long? Bgalumno { get; set; }

        public virtual ICollection<BitacoraDetalle> BitacoraDetalle { get; set; }
    }
}