﻿// <auto-generated> This file has been auto generated by EF Core Power Tools. </auto-generated>
#nullable disable
using System;
using System.Collections.Generic;

namespace ETL.Data.Sql
{
    public partial class Periodos
    {
        public Periodos()
        {
            Alumno = new HashSet<Alumno>();
            Grupos = new HashSet<Grupos>();
        }

        public int PeriodosId { get; set; }
        public string PeriodosNombre { get; set; }

        public virtual ICollection<Alumno> Alumno { get; set; }
        public virtual ICollection<Grupos> Grupos { get; set; }
    }
}