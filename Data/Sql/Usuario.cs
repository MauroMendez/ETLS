﻿// <auto-generated> This file has been auto generated by EF Core Power Tools. </auto-generated>
#nullable disable
using System;
using System.Collections.Generic;

namespace ETL.Data.Sql
{
    public partial class Usuario
    {
        public Usuario()
        {
            BecaAlumno = new HashSet<BecaAlumno>();
            Becaalubitacora = new HashSet<Becaalubitacora>();
            Campana = new HashSet<Campana>();
            Campanameta = new HashSet<Campanameta>();
            Carreras = new HashSet<Carreras>();
            ConvenioHistorico = new HashSet<ConvenioHistorico>();
            DocumentoBitacora = new HashSet<DocumentoBitacora>();
            Empresas = new HashSet<Empresas>();
            Ferias = new HashSet<Ferias>();
            GeneralesProspecto = new HashSet<GeneralesProspecto>();
            Modulo = new HashSet<Modulo>();
            MoodleKey = new HashSet<MoodleKey>();
            PlanesEstudio = new HashSet<PlanesEstudio>();
            ProgramasCv = new HashSet<ProgramasCv>();
            Reglamentos = new HashSet<Reglamentos>();
            Tbusuario = new HashSet<Tbusuario>();
            Tbususesion = new HashSet<Tbususesion>();
        }

        public long Usuid { get; set; }
        public int Rolid { get; set; }
        public string Usuusuario { get; set; }
        public string Usupassword { get; set; }
        public string Usutipo { get; set; }
        public long? Usuidtipo { get; set; }
        public string Usukey { get; set; }
        public bool Usuactivo { get; set; }
        public short? UsumoodleP { get; set; }
        public short? UsumoodleL { get; set; }
        public short? UsumoodleM { get; set; }
        public string Usumovil { get; set; }
        public string Usuip { get; set; }
        public decimal Usupid { get; set; }
        public string Usuequipo { get; set; }
        public byte[] Usuimage { get; set; }
        public DateTime? UsulastChange { get; set; }
        public DateTime? Ususession { get; set; }

        public virtual ICollection<BecaAlumno> BecaAlumno { get; set; }
        public virtual ICollection<Becaalubitacora> Becaalubitacora { get; set; }
        public virtual ICollection<Campana> Campana { get; set; }
        public virtual ICollection<Campanameta> Campanameta { get; set; }
        public virtual ICollection<Carreras> Carreras { get; set; }
        public virtual ICollection<ConvenioHistorico> ConvenioHistorico { get; set; }
        public virtual ICollection<DocumentoBitacora> DocumentoBitacora { get; set; }
        public virtual ICollection<Empresas> Empresas { get; set; }
        public virtual ICollection<Ferias> Ferias { get; set; }
        public virtual ICollection<GeneralesProspecto> GeneralesProspecto { get; set; }
        public virtual ICollection<Modulo> Modulo { get; set; }
        public virtual ICollection<MoodleKey> MoodleKey { get; set; }
        public virtual ICollection<PlanesEstudio> PlanesEstudio { get; set; }
        public virtual ICollection<ProgramasCv> ProgramasCv { get; set; }
        public virtual ICollection<Reglamentos> Reglamentos { get; set; }
        public virtual ICollection<Tbusuario> Tbusuario { get; set; }
        public virtual ICollection<Tbususesion> Tbususesion { get; set; }
    }
}