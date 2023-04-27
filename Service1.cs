using ProcessaXML.DataServer;
using ProcessaXML.Process;
using ProcessaXML.Consulta;
using ProcessaXML.VexPedidos;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;
using System.Linq.Expressions;
using System.Linq;
using System.Xml.Serialization;

namespace ProcessaXML
{
    public partial class Service1 : ServiceBase
    {
        string sVersao;
        bool bAtivaLog;
        string qtdeCritica = "0";
        Timer tempo = new Timer();
        AppSettingsReader configurationAppSettings = new System.Configuration.AppSettingsReader();
        string caminho, sProcessado;
        string sLogo;
        string sManual;
        string sCriticados;
        string sIgnorada;
        string sEnvioHoraDiario1;
        string sEnvioHoraDiario2;
        string sEnvioHoraDiario3;
        string sEnvioHoraDiario4;
        StreamReader oReader;
        FileInfo arq;
        string sCodUsuario = "";
        string cnpjEmpresa;
        int tamNumeroMov;
        int sCodColigada;
        string movCte, movNfs;
        int idprdCte;
        bool aguardaVex;
        bool duplicado;
        bool ignorada;
        bool excluir;
        string sCodUsuarioTBC;
        string sSenhaTBC;
        string serverTBC;
        string asmxTBC;
        string asmxProcessTBC;
        string asmxConsultaTBC;
        string idNatNfsExt = "";
        string idNatNfsInt = "";
        string codDocCte = "";
        string codDocNfe = "";
        string codDocNfs = "";
        string colTransp = "";
        string codTransp = "";
        string codLocCte = "";
        string codLocNfe = "";
        string serieNF = "";
        string tmvorignfs = "";
        string tmvorignfe = "";
        string tmvdestnfe = "";
        string tmvdestnfs = "";
        string movimentoAux2 = "";
        bool temLote;
        bool temRetorno;
        bool enviavex;
        bool enviavexnovo;
        bool enviarVex;
        bool ignoraPreco;
        itensNfe itensNfs = new itensNfe();
        bool nfe;
        bool importado;
        dbTools dbt = new dbTools();
        int buscaUnidade = 0;
        bool lote = false;
        bool carregado = false;
        bool vinculado = false;
        bool tagEncontrada = true;
        decimal maxAp;
        decimal minAp;
        string userCriacao = "";
        string dataInclusao;

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            this.tempo = new System.Timers.Timer(10000D);  // 30000 milliseconds = 30 seconds
            this.tempo.AutoReset = true;
            this.tempo.Elapsed += new System.Timers.ElapsedEventHandler(this.OnElapsedTime);
            this.tempo.Start();
        }

        protected override void OnStop()
        {
            this.tempo.Stop();
        }

        public void OnElapsedTime(object sender, ElapsedEventArgs e)
        {
            tempo.Enabled = false;
            if (!novoLeItensNfTransf("", null, null, e, null, null, null)) return;

            CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("pt-BR");
            CarregaApp(e);
            var dir = new DirectoryInfo(caminho);
            this.bAtivaLog = true;

            foreach (FileInfo arq in dir.GetFiles("*.csv")) //Verifica quantos arquivos XMLs possui no repostório.
            {
                importado = true;
                try
                {
                    //insere todas as linhas em uma lista.
                    List<string[]> listA = new List<string[]>();
                    using (var reader = new StreamReader(caminho + "\\" + arq.Name))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(';');
                            if (!values[0].ToString().Contains("Tipo de Registro"))
                            {
                                listA.Add(values);
                            }
                        }
                    }
                    //carrega as linhas e começa a inserção linha por linha
                    //as variaveis são as colunas.
                    foreach (string[] notas in listA)
                    {
                        try
                        {
                            string A = "";
                            string B = "";
                            string C = "";
                            string D = "";
                            string E = "";
                            string F = "";
                            string G = "";
                            string H = "";
                            string I = "";
                            string J = "";
                            string K = "";
                            string L = "";
                            string M = "";
                            string N = "";
                            string O = "";
                            string P = "";
                            string Q = "";
                            string R = "";
                            string S = "";
                            string T = "";
                            string U = "";
                            string V = "";
                            string W = "";
                            string X = "";
                            string Y = "";
                            string Z = "";
                            string AA = "";
                            string AB = "";
                            string AC = "";
                            string AD = "";
                            string AE = "";
                            string AF = "";
                            string AG = "";
                            string AH = "";
                            string AI = "";
                            string AJ = "";
                            string AK = "";
                            string AL = "";
                            string AM = "";
                            string AN = "";
                            string AO = "";
                            string AP = "";
                            string AQ = "";
                            string AR = "";
                            string AS = "";
                            string AT = "";
                            string AU = "";
                            string AV = "";
                            string AW = "";
                            string AX = "";
                            string AY = "";
                            string AZ = "";
                            string BA = "";
                            string BB = "";
                            string BC = "";
                            string BD = "";
                            string BE = "";
                            string BF = "";
                            string BG = "";
                            string BH = "";
                            string BI = "";
                            string BJ = "";
                            string BK = "";
                            string BL = "";
                            string BM = "";
                            string BN = "";
                            string BO = "";
                            string BP = "";
                            string BQ = "";
                            string BR = "";
                            string BS = "";
                            string BT = "";
                            string BU = "";
                            try { A = notas[0].ToString(); } catch { }
                            try { B = notas[1].ToString(); } catch { }
                            try { C = notas[2].ToString(); } catch { }
                            try { D = notas[3].ToString(); } catch { }
                            try { E = notas[4].ToString(); } catch { }
                            try { F = notas[5].ToString(); } catch { }
                            try { G = notas[6].ToString(); } catch { }
                            try { H = notas[7].ToString(); } catch { }
                            try { I = notas[8].ToString(); } catch { }
                            try { J = notas[9].ToString(); } catch { }
                            try { K = notas[10].ToString(); } catch { }
                            try { L = notas[11].ToString(); } catch { }
                            try { M = notas[12].ToString(); } catch { }
                            try { N = notas[13].ToString(); } catch { }
                            try { O = notas[14].ToString(); } catch { }
                            try { P = notas[15].ToString(); } catch { }
                            try { Q = notas[16].ToString(); } catch { }
                            try { R = notas[17].ToString(); } catch { }
                            try { S = notas[18].ToString(); } catch { }
                            try { T = notas[19].ToString(); } catch { }
                            try { U = notas[20].ToString(); } catch { }
                            try { V = notas[21].ToString(); } catch { }
                            try { W = notas[22].ToString(); } catch { }
                            try { X = notas[23].ToString(); } catch { }
                            try { Y = notas[24].ToString(); } catch { }
                            try { Z = notas[25].ToString(); } catch { }
                            try { AA = notas[26].ToString(); } catch { }
                            try { AB = notas[27].ToString(); } catch { }
                            try { AC = notas[28].ToString(); } catch { }
                            try { AD = notas[29].ToString(); } catch { }
                            try { AE = notas[30].ToString(); } catch { }
                            try { AF = notas[31].ToString(); } catch { }
                            try { AG = notas[32].ToString(); } catch { }
                            try { AH = notas[33].ToString(); } catch { }
                            try { AI = notas[34].ToString(); } catch { }
                            try { AJ = notas[35].ToString(); } catch { }
                            try { AK = notas[36].ToString(); } catch { }
                            try { AL = notas[37].ToString(); } catch { }
                            try { AM = notas[38].ToString(); } catch { }
                            try { AN = notas[39].ToString(); } catch { }
                            try { AO = notas[40].ToString(); } catch { }
                            try { AP = notas[41].ToString(); } catch { }
                            try { AQ = notas[42].ToString(); } catch { }
                            try { AR = notas[43].ToString(); } catch { }
                            try { AS = notas[44].ToString(); } catch { }
                            try { AT = notas[45].ToString(); } catch { }
                            try { AU = notas[46].ToString(); } catch { }
                            try { AV = notas[47].ToString(); } catch { }
                            try { AW = notas[48].ToString(); } catch { }
                            try { AX = notas[49].ToString(); } catch { }
                            try { AY = notas[50].ToString(); } catch { }
                            try { AZ = notas[51].ToString(); } catch { }
                            try { BA = notas[52].ToString(); } catch { }
                            try { BB = notas[53].ToString(); } catch { }
                            try { BC = notas[54].ToString(); } catch { }
                            try { BD = notas[55].ToString(); } catch { }
                            try { BE = notas[56].ToString(); } catch { }
                            try { BF = notas[57].ToString(); } catch { }
                            try { BG = notas[58].ToString(); } catch { }
                            try { BH = notas[59].ToString(); } catch { }
                            try { BI = notas[60].ToString(); } catch { }
                            try { BJ = notas[61].ToString(); } catch { }
                            try { BK = notas[62].ToString(); } catch { }
                            try { BL = notas[63].ToString(); } catch { }
                            try { BM = notas[64].ToString(); } catch { }
                            try { BN = notas[65].ToString(); } catch { }
                            try { BO = notas[66].ToString(); } catch { }
                            try { BP = notas[67].ToString(); } catch { }
                            try { BQ = notas[68].ToString(); } catch { }
                            try { BR = notas[69].ToString(); } catch { }
                            try { BS = notas[70].ToString(); } catch { }
                            try { BT = notas[71].ToString(); } catch { }
                            try { BU = notas[72].ToString(); } catch { }

                            dadosNfe dadosNF = new dadosNfe();
                            itensNfe item = new itensNfe();
                            var listaItem = new List<itensNfe>();
                            DataSet dataDados;
                            DataTable dt;
                            const int MaxLengthDesc = 100;
                            string descricaoItem = BU.ToString();
                            string expression;
                            string sort;
                            DataRow[] foundRows;
                            DataRow linha;
                            bool xmlErro = false;
                            bool bValEnv = false;
                            if (descricaoItem.Length > MaxLengthDesc)
                            {
                                descricaoItem = BU.ToString().Substring(0, MaxLengthDesc) + "...";
                            }
                            //verifica se é o cnpj da empresa
                            if (AI.ToString() != "" || !string.IsNullOrEmpty(AI.ToString()))
                            {
                                string RaizCnpjExt = AI.ToString().Substring(0, AI.ToString().IndexOf("/"));
                                cnpjEmpresa = ResultadoSQL("SELECT CNPJ FROM ZPARAMETROS WHERE CNPJ LIKE ('%" + RaizCnpjExt + "%')", 0);
                                string RaizCnpjInt = cnpjEmpresa.Substring(0, cnpjEmpresa.IndexOf("/"));
                                if (RaizCnpjExt == RaizCnpjInt)
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("SELECT CNPJ,MOVCTE,IDPRDCTE,TAMNUMEROMOV,CODTDOCTE,CODTDONFE,COLTRANSP,CODTRANSP,CODLOCCTE,CODLOCNFE,CODCOLIGADA,MOVNFS,IDNAT,IDNATEXT,SERIENFS,CODTDONFS, TMVORIGEM_NFE, TMVORIGEM_NFS, TMVDESTINO_NFE, TMVDESTINO_NFS FROM ZPARAMETROS(NOLOCK) WHERE CNPJ = '" + AI.ToString() + "'", connection);
                                        try
                                        {
                                            connection.Open();
                                            SqlDataReader dr;
                                            dr = command.ExecuteReader();
                                            if (dr.HasRows)
                                            {
                                                while (dr.Read())
                                                {
                                                    movCte = dr[1].ToString();
                                                    idprdCte = int.Parse(dr[2].ToString());
                                                    tamNumeroMov = int.Parse(dr[3].ToString());
                                                    codDocCte = dr[4].ToString();
                                                    codDocNfe = dr[5].ToString();
                                                    colTransp = dr[6].ToString();
                                                    codTransp = dr[7].ToString();
                                                    codLocCte = dr[8].ToString();
                                                    codLocNfe = dr[9].ToString();
                                                    sCodColigada = int.Parse(dr[10].ToString());
                                                    movNfs = dr[11].ToString();
                                                    idNatNfsInt = dr[12].ToString();
                                                    idNatNfsExt = dr[13].ToString();
                                                    serieNF = dr[14].ToString();
                                                    codDocNfs = dr[15].ToString();
                                                    tmvorignfe = dr[16].ToString();
                                                    tmvorignfs = dr[17].ToString();
                                                    tmvdestnfe = dr[18].ToString();
                                                    tmvdestnfs = dr[19].ToString();
                                                }
                                            }
                                            dr.Close();

                                        }
                                        catch (Exception ex)
                                        {
                                            fLog(arq.Name, "aqui25");
                                            command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                            command.ExecuteReader();
                                            // this.oReader.Close();
                                        }
                                    }
                                    dataDados = new DataSet();
                                    dataDados = validaCnpjEmpresa(arq.Name + "-" + D.ToString());
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        dt = dataDados.Tables[0];
                                        expression = "CGC = '" + cnpjEmpresa + "'";

                                        fLog(arq.Name + "-" + D.ToString(), expression);
                                        // Use the Select method to find all rows matching the filter.
                                        foundRows = dt.Select(expression);
                                        for (int i = 0; i < foundRows.Length; i++)
                                        {

                                            dadosNF.SColigada = foundRows[i]["CODCOLIGADA"].ToString();
                                            dadosNF.SFilial = foundRows[i]["CODFILIAL"].ToString();

                                        }

                                    }
                                }
                                else { fLog(arq.Name + "-" + D.ToString(), "CNPJ" + AI.ToString() + " Não configurado."); importado = false; continue; }
                            }
                            else continue;


                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXML(NOLOCK) WHERE FLAG_STATUS IS NULL AND NOME_XML = '" + arq.Name + "-" + D.ToString() + "'", connection);
                                connection.Open();
                                SqlDataReader dr;
                                dr = command.ExecuteReader();
                                if (dr.HasRows)
                                {
                                    using (SqlConnection connection2 = dbt.GetConnection())
                                    {
                                        SqlCommand cmd2 = new SqlCommand("UPDATE ZCRITICAXML SET FLAG_STATUS = 'E' WHERE FLAG_STATUS IS NULL AND NOME_XML = '" + arq.Name + "-" + D.ToString() + "'", connection2);
                                        try
                                        {
                                            connection2.Open();
                                            SqlDataReader dr2;
                                            dr2 = cmd2.ExecuteReader();
                                        }
                                        catch { }
                                    }

                                }
                            }

                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXML(NOLOCK) WHERE FLAG_STATUS = 'E' AND NOME_XML = '" + arq.Name + "-" + D.ToString() + "'", connection);
                                try
                                {
                                    connection.Open();
                                    SqlDataReader dr;
                                    dr = command.ExecuteReader();
                                    if (dr.HasRows)
                                    {
                                        bValEnv = true;
                                    }

                                }
                                catch (Exception ex)
                                {
                                    fLog(arq.Name, "aqui26");
                                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                    command.ExecuteReader();
                                    importado = false;
                                    continue;
                                }
                            }
                            if (bValEnv)
                            {
                                //MoveToCritica(arq.Name)
                                if (this.bAtivaLog)
                                {
                                    fLog(arq.Name + "-" + D.ToString(), "XML Criticado");
                                }
                                importado = false;
                                continue;
                            }
                            fLog(arq.Name + "-" + D.ToString(), C.ToString());
                            string[] split = C.ToString().Substring(0, 10).Split('/');
                            DateTime dataAux = DateTime.Parse(C.ToString());
                            fLog(arq.Name + "-" + D.ToString(), dataAux.ToString());

                            dadosNF.SDataEmissao = split[2] + '-' + split[1] + '-' + split[0];
                            fLog(arq.Name + "-" + D.ToString(), dadosNF.SDataEmissao);
                            //verifica se a nota já foi inclusa
                            if (fvalXmlProcessadoTransf(arq.Name + "-" + D.ToString(), dadosNF.SColigada, D.ToString(), "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC))
                            {
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("INSERT INTO ZLOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) VALUES ('" + arq.Name + "-" + D.ToString() + "',convert(datetime, '" + String.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), 'FIS', '" + sCodUsuario + "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                    }
                                    catch (Exception ex2)
                                    {
                                        fLog(arq.Name, "aqui27");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        importado = false;
                                        continue;
                                    }
                                }
                                importado = false;
                                continue;
                            }
                            else
                            {
                                dadosNF.CnpjEmi = K.ToString();
                                dadosNF.NomeEmi = L.ToString().Replace("'", "");
                                dadosNF.CnpjDest = AI.ToString();
                                dadosNF.NomeDest = AL.ToString();
                                dataDados = new DataSet();
                                //busca emissor cadastro
                                dataDados = buscaDados(arq.Name + "-" + D.ToString(), "CGCCFO = '" + K.ToString() + "' AND (FCFO.CODCOLIGADA = 0 OR FCFO.CODCOLIGADA =  " + dadosNF.SColigada + ") AND ATIVO = 1", "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];


                                    expression = "CGCCFO = '" + K.ToString() + "'";
                                    fLog(arq.Name + "-" + D.ToString(), expression);
                                    sort = "CODCOLIGADA ASC";


                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression, sort);
                                    for (int i = 0; i < foundRows.Length; i++)
                                    {
                                        dadosNF.SColCfoEmi = foundRows[i]["CODCOLIGADA"].ToString();
                                        dadosNF.SCodCfoEmi = foundRows[i]["CODCFO"].ToString();
                                    }
                                }
                                if (dadosNF.SColCfoEmi == "" || string.IsNullOrEmpty(dadosNF.SColCfoEmi))
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO,TIPO_DOC) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', '" + dadosNF.SDataEmissao + "' , 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'CLIENTE / FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui28");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            importado = false;
                                            continue;
                                        }
                                    }
                                }
                                else
                                {
                                    try
                                    {
                                        //Busca Centro de Custo
                                        dataDados = new DataSet();
                                        dataDados = buscaDados(arq.Name + "-" + D.ToString(), "FCFODEF.CODCOLCFO = " + dadosNF.SColCfoEmi + " AND FCFODEF.CODCOLIGADA =  " + dadosNF.SColigada + " AND FCFODEF.CODCFO = '" + dadosNF.SCodCfoEmi + "'", "FinCFODEFDataBR", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataDados.Tables.Count > 0)
                                        {
                                            dt = dataDados.Tables[0];
                                            linha = dt.Rows[0];
                                            dadosNF.SCodccusto = linha["CODCCUSTO"].ToString();
                                        }
                                        else
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO,TIPO_DOC) VALUES ('" + dadosNF.SColigada + "','" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', '" + dadosNF.SDataEmissao + "', 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'FORNECEDOR NÃO POSSUI CENTRO DE CUSTO DEFAULT', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3')", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    xmlErro = true;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    fLog(arq.Name, "aqui29");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    importado = false;
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception) { }
                                    try
                                    {
                                        //busca o produto
                                        dataDados = new DataSet();
                                        dataDados = buscaDados(arq.Name + "-" + D.ToString(), "CODNOFORN = '" + AC.ToString() + "' AND FCFO.CODCFO = '" + dadosNF.SCodCfoEmi + "' AND TPRDCFO.CODCOLCFO =  " + dadosNF.SColCfoEmi + " AND TPRDCFO.ATIVO = 1", "EstPrdCfoDataBR", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataDados.Tables.Count > 0)
                                        {
                                            dt = dataDados.Tables[0];


                                            expression = "CODNOFORN = '" + AC.ToString() + "'";
                                            fLog(arq.Name + "-" + D.ToString(), expression);
                                            sort = "CODCOLCFO ASC";


                                            // Use the Select method to find all rows matching the filter.
                                            foundRows = dt.Select(expression, sort);
                                            for (int j = 0; j < foundRows.Length; j++)
                                            {
                                                item.Idprd = foundRows[j]["IDPRD"].ToString();
                                            }
                                        }
                                        if (item.Idprd == "" || string.IsNullOrEmpty(item.Idprd))
                                        {
                                            xmlErro = true;
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO,CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC) VALUES " +
                                                            "('" + dadosNF.SColigada + "','" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', '" + dadosNF.SDataEmissao + "','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "', '" + dadosNF.NomeEmi + "', 'CMP', '" + 1 + "', '" + AC.ToString() + "', 'NÃO ENCONTRADO VÍNCULO DO CÓDIGO DO PRODUTO DO FORNECEDOR COM O PRODUTO DO RM', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3')", connection);

                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui30");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    //oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }

                                            }
                                        }
                                    }
                                    catch (Exception) { }
                                }

                                // Verificar Pedidos do Fornecedor:
                                DataTable dt2;
                                string flag;
                                string insertCritica;
                                string insertErrosAplic;
                                List<String> produtos = new List<String>();
                                dataDados = dbt.resultadoConsulta("SELECT FLAG_STATUS, NUMEROMOV_ASS, NSEQ_ASS, IDMOV_ASS FROM ZCRITICAXML WHERE NOME_XML = '" + arq.Name + "-" + D.ToString() + "' AND CODCFO = '" + dadosNF.SCodCfoEmi + "'");
                                try
                                {
                                    dt2 = dataDados.Tables["Table"];
                                }
                                catch { dt2 = null; }
                                try
                                {
                                    flag = dt2.Rows[0]["FLAG_STATUS"].ToString();
                                }
                                catch { flag = ""; }



                                if ((flag != "P" && flag != "S" && flag != "E") || flag == null)
                                {
                                    if (!String.IsNullOrEmpty(dadosNF.SCodCfoEmi) && (!String.IsNullOrEmpty(item.Idprd)))
                                    {
                                        dataDados = buscaDados(D.ToString(), " TMOV.STATUS IN ('A','G') AND TMOV.CODCFO = '" + dadosNF.SCodCfoEmi + "' AND TMOV.CODTMV = '" + tmvorignfs + "'", "MovMovimentoTBCData", "svcjoins_xml", "svcjoins_xml");
                                        dt = dataDados.Tables["TMOV"];

                                        if (dt == null)
                                        {
                                            insertCritica = "INSERT INTO ZCRITICAXML (COD_PRD_AUX, COD_PRD,  CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC, DESC_SERVICO, QUANTIDADE) VALUES ('" + AC.ToString() + "', '" + item.Idprd + "', '" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'PO','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', '" + "NÃO FORAM ENCONTRADOS PEDIDOS PARA O FORNECEDOR: " + dadosNF.SCodCfoEmi.ToString() + "', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3', '" + descricaoItem + "', " + qtdeCritica + ")";
                                            insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                            CriticarXML(insertCritica, insertErrosAplic);
                                            importado = false;
                                            continue;
                                        }
                                        else if (dt.Rows.Count >= 1)
                                        {
                                            int countProd = 0;
                                            string idMovProdEncontrado = "";
                                            int nSeqProdEncontrado = -1;
                                            DataRow prodEncontrado = null;
                                            foreach (DataRow dr in dt.Rows)
                                            {
                                                prodEncontrado = buscarProduto(D.ToString(), int.Parse(dr["IDMOV"].ToString()), int.Parse(item.Idprd));
                                                if (prodEncontrado != null)
                                                {
                                                    if (int.Parse(prodEncontrado["NSEQITMMOV"].ToString()) != -1)
                                                    {
                                                        idMovProdEncontrado = dr["IDMOV"].ToString();
                                                        nSeqProdEncontrado = int.Parse(prodEncontrado["NSEQITMMOV"].ToString());
                                                        countProd++;
                                                    }
                                                }
                                            }
                                            if (countProd > 1)
                                            {
                                                insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC, DESC_SERVICO, COD_PRD, COD_PRD_AUX, QUANTIDADE) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'PO','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', '" + "EXISTE MAIS DE UM PEDIDO COM O PRODUTO: " + item.Idprd.ToString() + "', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3', '" + descricaoItem + "', '" + AC.ToString() + "', '" + item.Idprd + "', " + qtdeCritica + ")";
                                                insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                                CriticarXML(insertCritica, insertErrosAplic);
                                                importado = false;
                                                continue;
                                            }
                                            else if (countProd == 0)
                                            {
                                                insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC, DESC_SERVICO, COD_PRD, COD_PRD_AUX, QUANTIDADE) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'PO','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', '" + "PRODUTO \"" + idMovProdEncontrado + "\" NÃO ENCONTRADO EM NENHUM PEDIDO DO FORNECEDOR: \"" + dadosNF.SCodCfoEmi.ToString() + "\"', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3', '" + descricaoItem + "', '" + AC.ToString() + "', '" + item.Idprd + "', " + qtdeCritica + ")";
                                                insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                                CriticarXML(insertCritica, insertErrosAplic);
                                                importado = false;
                                                continue;

                                            }
                                            else
                                            {
                                                itensNfs.Idmov = int.Parse(idMovProdEncontrado);
                                                itensNfs.Nseq = nSeqProdEncontrado;
                                            }
                                        }
                                    }
                                }
                                else if (flag == "S")
                                {
                                    // dataDados = buscaDados(D.ToString(), " TMOV.NUMEROMOV = '" + dt2.Rows[0]["NUMEROMOV_ASS"].ToString() + "' AND TMOV.CODCFO = '" + dadosNF.SCodCfoEmi + "'", "MovMovimentoTBCData", "svcjoins_xml", "svcjoins_xml");
                                    // itensNfs.Idmov = int.Parse(dataDados.Tables["TMOV"].Rows[0]["IDMOV"].ToString());
                                    itensNfs.Idmov = int.Parse(dt2.Rows[0]["IDMOV_ASS"].ToString());
                                    itensNfs.PedidoDeCompra = dt2.Rows[0]["NUMEROMOV_ASS"].ToString();
                                    if (dt2.Rows[0]["NSEQ_ASS"].ToString() != "")
                                    {
                                        itensNfs.Nseq = int.Parse(dt2.Rows[0]["NSEQ_ASS"].ToString());
                                    }
                                    else
                                    {
                                        DataRow drAux = buscarProduto(D.ToString(), int.Parse(itensNfs.Idmov.ToString()), int.Parse(item.Idprd));
                                        itensNfs.Nseq = int.Parse(drAux["NSEQITMMOV"].ToString());
                                    }

                                }
                                else if (flag == "P")
                                {
                                    itensNfs.PedidoDeCompra = "";
                                }
                                dadosNF.DescricaoServico = descricaoItem;
                                dadosNF.SCodLoc = codLocCte;
                                dadosNF.CodTdo = codDocNfs;
                                //dadosNF.CodCpg = "143";
                                dadosNF.SNumeromov = B.ToString();
                                dadosNF.Serie = serieNF;
                                dadosNF.Idnat = idNatNfsInt;
                                item.Idnat = idNatNfsInt;
                                dadosNF.TotalNf = AA.ToString();
                                dadosNF.ChaveAcesso = D.ToString();

                                if (dadosNF.SNumeromov.Length <= tamNumeroMov)
                                {
                                    string nAux = new string('0', tamNumeroMov - dadosNF.SNumeromov.Length);
                                    dadosNF.SNumeromov = nAux + dadosNF.SNumeromov;
                                }
                                else
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC) VALUES ('" + dadosNF.SColigada + "','" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', '" + dadosNF.SDataEmissao + "', 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'TAMANHO DO NUMERO DE DOCUMENTO INFORMADO É MENOR QUE O TAMANHO DO NÚMERO DA NOTA', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            importado = false;
                                            continue;

                                        }
                                        catch (Exception ex2)
                                        {

                                            fLog(arq.Name, "aqui40");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            importado = false;
                                            continue;
                                        }
                                    }
                                }
                                item.CodUnd = "UN"; //aqui buscar cadastro
                                                    //impostos
                                                    //item.ValorPis = BD.ToString();
                                                    //item.ValorCofins = BE.ToString();
                                item.ValorInss = BF.ToString();
                                item.ValorIr = BG.ToString();
                                item.ValorCsll = BH.ToString();
                                item.AliqIss = AD.ToString();
                                item.ValorIss = AE.ToString();
                                item.ValorProduto = AA.ToString();

                                if (!string.IsNullOrEmpty(item.ValorInss) && item.ValorInss != "0" && item.ValorInss != "0,00")
                                {
                                    double valor;
                                    double total;
                                    double aliquota;
                                    valor = double.Parse(item.ValorInss);
                                    total = double.Parse(item.ValorProduto);
                                    aliquota = Math.Round(valor * 100 / total, 2);
                                    item.AliqInss = aliquota.ToString();
                                }
                                if (!string.IsNullOrEmpty(item.ValorIr) && item.ValorIr != "0" && item.ValorIr != "0,00")
                                {
                                    double valor;
                                    double total;
                                    double aliquota;
                                    valor = double.Parse(item.ValorIr);
                                    total = double.Parse(item.ValorProduto);
                                    aliquota = Math.Round(valor * 100 / total, 2);
                                    item.AliqIr = aliquota.ToString();
                                }
                                if (!string.IsNullOrEmpty(item.ValorCsll) && item.ValorCsll != "0" && item.ValorCsll != "0,00")
                                {
                                    double valor;
                                    double total;
                                    double aliquota;
                                    valor = double.Parse(item.ValorCsll);
                                    //valorPis = double.Parse(item.ValorPis);
                                    //valorCofins = double.Parse(item.ValorCofins);
                                    total = double.Parse(item.ValorProduto);
                                    aliquota = Math.Round((valor) * 100 / total, 2); // + valorPis + valorCofins
                                    item.AliqCsll = aliquota.ToString();
                                    item.ValorCsll = (valor).ToString(); // + valorPis + valorCofins
                                }

                                //busca natureza
                                if (xmlErro)
                                {
                                    fLog(arq.Name + "-" + D.ToString(), "Tem erro");
                                    if (oReader != null)
                                    {
                                        oReader.Close();
                                    }
                                    importado = false;
                                    continue;
                                }
                                else
                                {
                                    xmlErro = false;
                                    dadosNF.MovNfe = movNfs; //aqui buscar do cadastro
                                    if (xmlErro)
                                    {
                                        fLog(arq.Name + "-" + D.ToString(), "Tem erro item");
                                        if (oReader != null)
                                        {
                                            oReader.Close();
                                        }
                                        importado = false;
                                        continue;
                                    }
                                    else
                                    {
                                        string codColResult, resultado = "", pontoEVirgula;
                                        listaItem.Add(item);
                                        dadosNF.Itens = listaItem.ToArray();

                                        if (itensNfs.PedidoDeCompra == "")
                                        {
                                            resultado = PopulaTabelasNfse(dadosNF, e);
                                        }
                                        else
                                        {
                                            resultado = processarXMLNFS(dadosNF);
                                        }
                                        try
                                        {
                                            codColResult = resultado.Substring(0, dadosNF.SColigada.Length);
                                            pontoEVirgula = resultado.Substring(dadosNF.SColigada.Length, 1);
                                        }
                                        catch
                                        {
                                            codColResult = "";
                                            pontoEVirgula = "";
                                        }
                                        if ((codColResult == dadosNF.SColigada & pontoEVirgula == ";") || resultado == "1")
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM ZCRITICAXML WHERE NOME_XML = '" + arq.Name + "-" + D.ToString() + "'", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    fLog("", "");
                                                    fLog("=========================================", "");
                                                    fLog("(NFS) " + D.ToString(), ": PROCESSADO COM SUCESSO!");
                                                    fLog("=========================================", "");
                                                }
                                                catch (Exception)
                                                {
                                                    importado = false;
                                                    continue;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                resultado = resultado.Replace("'", "");
                                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC) VALUES ('" + dadosNF.SColigada + "','" + dadosNF.SFilial + "', '" + arq.Name + "-" + D.ToString() + "', '" + dadosNF.SDataEmissao + "', 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', '" + resultado.ToString().Substring(0, 230) + "', 'C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'3')", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    importado = false;
                                                    continue;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    fLog(arq.Name, "aqui41");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "-" + D.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    //File.Delete(sCriticados & "\" & arq.Name)
                                                    //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                                    //File.Delete(caminho & "\" & arq.Name)
                                                    importado = false;
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }


                        catch (Exception ex)
                        {
                            fLog(arq.Name, ex.ToString());
                            importado = false;
                            continue;
                        }
                    }
                }
                catch (Exception ex)
                {
                    fLog(arq.Name, ex.Message);
                    importado = false;
                    continue;
                }
                if (importado)
                {
                    File.Delete(sProcessado + "\\" + arq.Name);
                    File.Copy(caminho + "\\" + arq.Name, sProcessado + "\\" + arq.Name);
                    File.Delete(caminho + "\\" + arq.Name);
                }
            }
            this.bAtivaLog = true;
            fLog("Numero de arquivos", dir.GetFiles("*.xml").Length.ToString());
            this.bAtivaLog = true;
            foreach (FileInfo arq in dir.GetFiles("*.xml")) //Verifica quantos arquivos XMLs possui no repostório.
            {

                bAtivaLog = true;
                bool xmlErro = false;
                duplicado = false;
                ignorada = false;
                try
                {
                    //Se bAtivaLog for TRUE salva informação no Log
                    if (this.bAtivaLog == true)
                    {
                    }

                    tempo.Enabled = false;
                    //Cria uma instância de um documento XML
                    XmlDocument xmlDoc = new XmlDocument();
                    XmlNamespaceManager ns = new XmlNamespaceManager(xmlDoc.NameTable);
                    ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe");
                    XPathNavigator xpathNav = xmlDoc.CreateNavigator();
                    XPathNavigator node;
                    string arquivoxml = caminho + "\\" + arq.Name;
                    //Caminho onde se encontra os xmls + nome do xml que está sendo tratado.
                    bool bValEnv = false;

                    //Verifica XML foi Criticado
                    using (SqlConnection connection = dbt.GetConnection())
                    {
                        SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXML(NOLOCK) WHERE FLAG_STATUS IS NULL AND NOME_XML = '" + arq.Name + "'", connection);
                        connection.Open();
                        SqlDataReader dr;
                        dr = command.ExecuteReader();
                        if (dr.HasRows)
                        {
                            using (SqlConnection connection2 = dbt.GetConnection())
                            {
                                SqlCommand cmd2 = new SqlCommand("UPDATE ZCRITICAXML SET FLAG_STATUS = 'E' WHERE FLAG_STATUS IS NULL AND NOME_XML = '" + arq.Name + "'", connection2);
                                try
                                {
                                    connection2.Open();
                                    SqlDataReader dr2;
                                    dr2 = cmd2.ExecuteReader();
                                    this.oReader.Close();
                                }
                                catch { }
                            }

                        }
                    }

                    using (SqlConnection connection = dbt.GetConnection())
                    {
                        SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXML(NOLOCK) WHERE FLAG_STATUS = 'E' AND NOME_XML = '" + arq.Name + "'", connection);
                        try
                        {
                            connection.Open();
                            SqlDataReader dr;
                            dr = command.ExecuteReader();
                            if (dr.HasRows)
                            {
                                bValEnv = true;
                            }

                        }
                        catch (Exception ex)
                        {
                            fLog(arq.Name, "aqui42");
                            command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                            command.ExecuteReader();
                            this.oReader.Close();
                            atualizaCritica(arq.Name);
                            continue;
                        }
                    }

                    if (bValEnv)
                    {
                        //MoveToCritica(arq.Name)
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "XML Criticado");
                        }
                        atualizaCritica(arq.Name);
                        continue;
                    }
                    if (this.bAtivaLog)
                    {
                        fLog(arq.Name, "Valida XML");
                    }

                    XDocument xDoc;
                    try
                    {
                        oReader = new StreamReader(arquivoxml, Encoding.GetEncoding("ISO-8859-1")); //Converte o XML para o encond ("ISO-8859-1") possibilitando ler arquivos XML que possuem acentos em seus textos
                        xDoc = XDocument.Load(oReader);
                    }
                    catch (Exception ex)
                    {
                        if (bAtivaLog)
                        {
                            fLog(arq.Name, "catch linha 212 " + ex.Message);
                        }
                        string sMsgErro;
                        sMsgErro = ex.Message;
                        if (sMsgErro.Length >= 37)
                        {
                            sMsgErro = sMsgErro.Substring(0, 37);
                        }
                        if ((sMsgErro == "Elemento raiz inexistente.") || (sMsgErro == "O processo não pode acessar o arquivo") || (sMsgErro == "Root element is missing."))
                        {
                            oReader.Close();
                            atualizaCritica(arq.Name);
                            continue;
                        }
                        using (SqlConnection connection = dbt.GetConnection())
                        {
                            SqlCommand command = new SqlCommand("INSERT INTO ZLOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) VALUES ('" + arq.Name + "',convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), 'FIS', '" + sCodUsuario + "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO ENCONTRA-SE CORROMPIDO. MENSAGEM DE ERRO: " + sMsgErro + "')", connection);
                            try
                            {
                                connection.Open();
                                command.ExecuteReader();
                            }
                            catch (Exception ex2)
                            {
                                fLog(arq.Name, "aqui43");
                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                command.ExecuteReader();
                                oReader.Close();
                                //File.Delete(sCriticados & "\" & arq.Name)
                                //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                //File.Delete(caminho & "\" & arq.Name)
                                atualizaCritica(arq.Name);
                                continue;
                            }
                        }
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Enviado para pasta Criticado linha 232" + ex.Message);
                        }
                        oReader.Close();
                        File.Delete(sCriticados + "\\" + arq.Name);
                        File.Copy(caminho + "\\" + arq.Name, sCriticados + "\\" + arq.Name);
                        File.Delete(caminho + "\\" + arq.Name);
                        atualizaCritica(arq.Name);
                        continue;
                    }


                    //Verifica RegrasXML
                    //RegrasXML(e)

                    //Função que remove acentos
                    string sXmlTratado;
                    string idXml;
                    sXmlTratado = RemoverAcentos(xDoc.ToString(), arq.Name);
                    xmlDoc.Load(new StringReader(sXmlTratado));

                    //Verifica leiaute
                    if (this.bAtivaLog)
                    {
                        fLog(arq.Name, "Le Versão");
                    }
                    xmlDoc.Load(arquivoxml);
                    XmlElement root = xmlDoc.DocumentElement;
                    this.sVersao = root.GetAttribute("versao");
                    if (sVersao == "" || sVersao == null)
                    {
                        try
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe", ns);
                            sVersao = node.GetAttribute("versao", "");
                        }
                        catch
                        { }
                    }
                    XmlNamespaceManager ns2 = new XmlNamespaceManager(xmlDoc.NameTable);
                    ns2.AddNamespace("cte", "http://www.portalfiscal.inf.br/cte");
                    string cnpjAux;
                    DataSet dataDados;
                    DataTable dt;
                    string expression;
                    string sort;
                    DataRow[] foundRows;
                    DataRow linha;
                    try
                    {
                        node = xpathNav.SelectSingleNode("//cte:infCte", ns2);
                        if (node != null)
                        {
                            idXml = node.GetAttribute("Id", "").ToString();
                            nfe = false;
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe", ns);
                            idXml = node.GetAttribute("Id", "").ToString();
                            nfe = true;
                        }
                    }
                    catch
                    {
                        node = xpathNav.SelectSingleNode("//nfe:infNFe", ns);
                        idXml = node.GetAttribute("Id", "").ToString();
                        nfe = true;
                    }
                    if (nfe == false)
                    {
                        int tipoEmi, tipoRem, tipoDest, tipoToma;
                        string cnpjToma = "";
                        dadosCte dados = new dadosCte();
                        dados.Idprd = idprdCte;
                        dados.MovCte = movCte;
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Inicia Dados CTE");
                            fLog(arq.Name, "Emissor");
                        }
                        try
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:emit/cte:CNPJ", ns2);
                            dados.CnpjEmi = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoEmi = 2;
                        }
                        catch
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:emit/cte:CPF", ns2);
                            dados.CnpjEmi = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoEmi = 1;
                        }
                        node = xpathNav.SelectSingleNode("//cte:infCte/cte:emit/cte:xNome", ns2);
                        dados.NomeEmi = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Destinatario");
                        }
                        try
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:dest/cte:CNPJ", ns2);
                            dados.CnpjDest = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoDest = 2;
                        }
                        catch
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:dest/cte:CPF", ns2);
                            dados.CnpjDest = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoDest = 1;
                        }
                        node = xpathNav.SelectSingleNode("//cte:infCte/cte:dest/cte:xNome", ns2);
                        dados.NomeDest = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Remetente");
                        }
                        try
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:rem/cte:CNPJ", ns2);
                            dados.CnpjRem = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoRem = 2;
                        }
                        catch
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:rem/cte:CPF", ns2);
                            dados.CnpjRem = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoRem = 1;
                        }
                        node = xpathNav.SelectSingleNode("//cte:infCte/cte:rem/cte:xNome", ns2);
                        dados.NomeRem = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        try
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:toma4/cte:CNPJ", ns2);
                            cnpjToma = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                            tipoToma = 2;
                        }
                        catch
                        {
                            try
                            {
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:toma4/cte:CPF", ns2);
                                cnpjToma = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                                tipoToma = 1;
                            }
                            catch { }
                        }
                        node = xpathNav.SelectSingleNode("//cte:infCte", ns2);
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "ChaveAcesso");
                        }
                        dados.ChaveAcesso = node.GetAttribute("Id", "").ToString();       //Chave de acesso da NF-e
                        dados.ChaveAcesso = dados.ChaveAcesso.Replace("CTe", "");
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Data Emissão");
                        }
                        node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:dhEmi", ns2);
                        dados.SDataEmissao = node.InnerXml.ToString().Substring(0, 10);

                        try
                        {
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:infCTeNorm/cte:infDoc", ns2);
                            XPathNodeIterator xPathIterator = node.SelectChildren(XPathNodeType.Element);
                            foreach (XPathNavigator book in xPathIterator)
                            {
                                dados.Obs = dados.Obs + "SERVIÇO DE TRANSPORTE REF. NF: " + book.Value.ToString().Substring(25, 9) + "\n";
                            }
                        }
                        catch { }
                        cnpjEmpresa = null;
                        long aux = long.Parse(dados.CnpjDest);
                        string RaizCnpj = string.Format(@"{0:00\.000\.000\/0000\-00}", aux);
                        try
                        {
                            RaizCnpj = RaizCnpj.Substring(0, RaizCnpj.IndexOf("/"));
                        }
                        catch
                        {
                            RaizCnpj = RaizCnpj.Substring(0, 8);
                        }
                        using (SqlConnection connection = dbt.GetConnection())
                        {
                            SqlCommand command = new SqlCommand("SELECT CNPJ,MOVCTE,IDPRDCTE,TAMNUMEROMOV,CODTDOCTE,CODTDONFE,COLTRANSP,CODTRANSP,CODLOCCTE,CODLOCNFE,CODCOLIGADA,MOVNFS,IDNAT,IDNATEXT,SERIENFS,CODTDONFS, TMVORIGEM_NFE, TMVORIGEM_NFS, TMVDESTINO_NFE, TMVDESTINO_NFS FROM ZPARAMETROS(NOLOCK) WHERE CNPJ LIKE '%" + RaizCnpj + "%'", connection);
                            try
                            {
                                connection.Open();
                                SqlDataReader dr;
                                dr = command.ExecuteReader();
                                if (dr.HasRows)
                                {
                                    while (dr.Read())
                                    {
                                        cnpjEmpresa = dr[0].ToString();
                                        movCte = dr[1].ToString();
                                        idprdCte = int.Parse(dr[2].ToString());
                                        tamNumeroMov = int.Parse(dr[3].ToString());
                                        codDocCte = dr[4].ToString();
                                        codDocNfe = dr[5].ToString();
                                        colTransp = dr[6].ToString();
                                        codTransp = dr[7].ToString();
                                        codLocCte = dr[8].ToString();
                                        codLocNfe = dr[9].ToString();
                                        sCodColigada = int.Parse(dr[10].ToString());
                                        movNfs = dr[11].ToString();
                                        idNatNfsInt = dr[12].ToString();
                                        idNatNfsExt = dr[13].ToString();
                                        serieNF = dr[14].ToString();
                                        codDocNfs = dr[15].ToString();
                                        tmvorignfe = dr[16].ToString();
                                        tmvorignfs = dr[17].ToString();
                                        tmvdestnfe = dr[18].ToString();
                                        tmvdestnfs = dr[19].ToString();
                                    }
                                }
                                dr.Close();
                            }
                            catch (Exception ex)
                            {
                                fLog(arq.Name, "aqui44");
                                command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                command.ExecuteReader();
                                // this.oReader.Close();
                            }
                        }

                        //cnpjEmpresa = ResultadoSQL("SELECT CNPJ FROM ZPARAMETROS WHERE CNPJ LIKE ('%" + RaizCnpj + "%')", 0);
                        if (cnpjEmpresa != null)
                        {
                            dataDados = new DataSet();
                            dataDados = validaCnpjEmpresa(arq.Name);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables[0];
                                expression = "CGC = '" + cnpjEmpresa + "'";
                                if (this.bAtivaLog)
                                {
                                    fLog(arq.Name, expression);
                                }
                                // Use the Select method to find all rows matching the filter.
                                foundRows = dt.Select(expression);
                                for (int i = 0; i < foundRows.Length; i++)
                                {

                                    dados.SColigada = foundRows[i]["CODCOLIGADA"].ToString();
                                    dados.SFilial = foundRows[i]["CODFILIAL"].ToString();

                                }
                            }
                        }
                        else
                        {
                            aux = long.Parse(dados.CnpjRem);
                            RaizCnpj = string.Format(@"{0:00\.000\.000\/0000\-00}", aux);
                            try
                            {
                                RaizCnpj = RaizCnpj.Substring(0, RaizCnpj.IndexOf("/"));
                            }
                            catch
                            {
                                RaizCnpj = RaizCnpj.Substring(0, 8);
                            }
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("SELECT CNPJ,MOVCTE,IDPRDCTE,TAMNUMEROMOV,CODTDOCTE,CODTDONFE,COLTRANSP,CODTRANSP,CODLOCCTE,CODLOCNFE,CODCOLIGADA,MOVNFS,IDNAT,IDNATEXT,SERIENFS,CODTDONFS, TMVORIGEM_NFE, TMVORIGEM_NFS, TMVDESTINO_NFE, TMVDESTINO_NFS FROM ZPARAMETROS(NOLOCK) WHERE CNPJ LIKE '%" + RaizCnpj + "%'", connection);
                                try
                                {
                                    connection.Open();
                                    SqlDataReader dr;
                                    dr = command.ExecuteReader();
                                    if (dr.HasRows)
                                    {
                                        while (dr.Read())
                                        {
                                            cnpjEmpresa = dr[0].ToString();
                                            movCte = dr[1].ToString();
                                            idprdCte = int.Parse(dr[2].ToString());
                                            tamNumeroMov = int.Parse(dr[3].ToString());
                                            codDocCte = dr[4].ToString();
                                            codDocNfe = dr[5].ToString();
                                            colTransp = dr[6].ToString();
                                            codTransp = dr[7].ToString();
                                            codLocCte = dr[8].ToString();
                                            codLocNfe = dr[9].ToString();
                                            sCodColigada = int.Parse(dr[10].ToString());
                                            movNfs = dr[11].ToString();
                                            idNatNfsInt = dr[12].ToString();
                                            idNatNfsExt = dr[13].ToString();
                                            serieNF = dr[14].ToString();
                                            codDocNfs = dr[15].ToString();
                                            tmvorignfe = dr[16].ToString();
                                            tmvorignfs = dr[17].ToString();
                                            tmvdestnfe = dr[18].ToString();
                                            tmvdestnfs = dr[19].ToString();
                                        }
                                    }
                                    dr.Close();
                                }
                                catch (Exception ex)
                                {
                                    fLog(arq.Name, "aqui45");
                                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('DADOS INICIAIS', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                    command.ExecuteReader();
                                    // this.oReader.Close();
                                }
                            }
                            if (cnpjEmpresa != null)
                            {
                                dataDados = new DataSet();
                                dataDados = validaCnpjEmpresa(arq.Name);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    expression = "CGC = '" + cnpjEmpresa + "'";
                                    if (this.bAtivaLog)
                                    {
                                        fLog(arq.Name, expression);
                                    }
                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression);
                                    for (int i = 0; i < foundRows.Length; i++)
                                    {

                                        dados.SColigada = foundRows[i]["CODCOLIGADA"].ToString();
                                        dados.SFilial = foundRows[i]["CODFILIAL"].ToString();

                                    }
                                }
                            }
                            else
                            {
                                aux = long.Parse(cnpjToma);
                                RaizCnpj = string.Format(@"{0:00\.000\.000\/0000\-00}", aux);
                                try
                                {
                                    RaizCnpj = RaizCnpj.Substring(0, RaizCnpj.IndexOf("/"));
                                }
                                catch
                                {
                                    RaizCnpj = RaizCnpj.Substring(0, 8);
                                }
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("SELECT CNPJ,MOVCTE,IDPRDCTE,TAMNUMEROMOV,CODTDOCTE,CODTDONFE,COLTRANSP,CODTRANSP,CODLOCCTE,CODLOCNFE,CODCOLIGADA,MOVNFS,IDNAT,IDNATEXT,SERIENFS,CODTDONFS, TMVORIGEM_NFE, TMVORIGEM_NFS, TMVDESTINO_NFE, TMVDESTINO_NFS FROM ZPARAMETROS(NOLOCK) WHERE CNPJ LIKE '%" + RaizCnpj + "%'", connection);
                                    try
                                    {
                                        connection.Open();
                                        SqlDataReader dr;
                                        dr = command.ExecuteReader();
                                        if (dr.HasRows)
                                        {
                                            while (dr.Read())
                                            {
                                                cnpjEmpresa = dr[0].ToString();
                                                movCte = dr[1].ToString();
                                                idprdCte = int.Parse(dr[2].ToString());
                                                tamNumeroMov = int.Parse(dr[3].ToString());
                                                codDocCte = dr[4].ToString();
                                                codDocNfe = dr[5].ToString();
                                                colTransp = dr[6].ToString();
                                                codTransp = dr[7].ToString();
                                                codLocCte = dr[8].ToString();
                                                codLocNfe = dr[9].ToString();
                                                sCodColigada = int.Parse(dr[10].ToString());
                                                movNfs = dr[11].ToString();
                                                idNatNfsInt = dr[12].ToString();
                                                idNatNfsExt = dr[13].ToString();
                                                serieNF = dr[14].ToString();
                                                codDocNfs = dr[15].ToString();
                                                tmvorignfe = dr[16].ToString();
                                                tmvorignfs = dr[17].ToString();
                                                tmvdestnfe = dr[18].ToString();
                                                tmvdestnfs = dr[19].ToString();
                                            }
                                        }
                                        dr.Close();
                                    }
                                    catch (Exception ex)
                                    {
                                        fLog(arq.Name, "aqui6");
                                        command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('DADOS INICIAIS', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                        command.ExecuteReader();
                                        // this.oReader.Close();
                                    }
                                }
                                if (cnpjEmpresa != null)
                                {
                                    dataDados = new DataSet();
                                    dataDados = validaCnpjEmpresa(arq.Name);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        dt = dataDados.Tables[0];
                                        expression = "CGC = '" + cnpjEmpresa + "'";
                                        if (this.bAtivaLog)
                                        {
                                            fLog(arq.Name, expression);
                                        }
                                        // Use the Select method to find all rows matching the filter.
                                        foundRows = dt.Select(expression);
                                        for (int i = 0; i < foundRows.Length; i++)
                                        {
                                            dados.SColigada = foundRows[i]["CODCOLIGADA"].ToString();
                                            dados.SFilial = foundRows[i]["CODFILIAL"].ToString();
                                        }
                                    }
                                }
                            }
                        }
                        if (dados.SColigada == "" || string.IsNullOrEmpty(dados.SColigada))
                        {
                            oReader.Close();
                            atualizaCritica(arq.Name);
                            continue;
                        }

                        if (fvalXmlProcessadoTransf(arq.Name, dados.SColigada, dados.ChaveAcesso, "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC))
                        {
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("INSERT INTO ZLOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) VALUES ('" + arq.Name + "',convert(datetime, '" + String.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), 'FIS', '" + sCodUsuario + "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception ex2)
                                {
                                    fLog(arq.Name, "aqui47");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    //oReader.Close()
                                    //File.Delete(sCriticados & "\" & arq.Name)
                                    //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                    //File.Delete(caminho & "\" & arq.Name)
                                    atualizaCritica(arq.Name);
                                    continue;
                                }
                            }
                            oReader.Close();
                            if (this.bAtivaLog)
                            {
                                fLog(arq.Name, "Enviado para pasta Criticado linha 1016");
                            }
                            File.Delete(sCriticados + "\\" + arq.Name);
                            File.Copy(caminho + "\\" + arq.Name, sCriticados + "\\" + arq.Name);
                            File.Delete(caminho + "\\" + arq.Name);
                            atualizaCritica(arq.Name);
                            continue;
                        }
                        else
                        {
                            if (tipoEmi == 1)
                            {
                                cnpjAux = Convert.ToUInt64(dados.CnpjEmi).ToString(@"000\.000\.000\-00");
                            }
                            else
                            {
                                cnpjAux = Convert.ToUInt64(dados.CnpjEmi).ToString(@"00\.000\.000\/0000\-00");
                            }
                            dataDados = new DataSet();
                            dataDados = buscaDados(arq.Name, "CGCCFO = '" + cnpjAux + "' AND (FCFO.CODCOLIGADA = 0 OR FCFO.CODCOLIGADA =  " + dados.SColigada + ") AND ATIVO = 1", "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables[0];


                                expression = "CGCCFO = '" + cnpjAux + "'";
                                if (this.bAtivaLog)
                                {
                                    fLog(arq.Name, expression);
                                }
                                sort = "CODCOLIGADA ASC";


                                // Use the Select method to find all rows matching the filter.
                                foundRows = dt.Select(expression, sort);
                                for (int i = 0; i < foundRows.Length; i++)
                                {
                                    dados.SColCfoEmi = foundRows[i]["CODCOLIGADA"].ToString();
                                    dados.SCodCfoEmi = foundRows[i]["CODCFO"].ToString();
                                }
                            }
                            if (dados.SColCfoEmi == "" || string.IsNullOrEmpty(dados.SColCfoEmi))
                            {
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'CLIENTE / FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                        xmlErro = true;
                                    }
                                    catch (Exception ex2)
                                    {
                                        fLog(arq.Name, "aqui48");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        oReader.Close();
                                        //File.Delete(sCriticados & "\" & arq.Name)
                                        //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                        //File.Delete(caminho & "\" & arq.Name)
                                        atualizaCritica(arq.Name);
                                        continue;
                                    }
                                }

                            }
                            if (dados.CnpjRem != cnpjEmpresa.Replace(".", "").Replace("/", "").Replace("-", ""))
                            {
                                if (tipoRem == 1)
                                {
                                    cnpjAux = Convert.ToUInt64(dados.CnpjRem).ToString(@"000\.000\.000\-00");
                                }
                                else
                                {
                                    cnpjAux = Convert.ToUInt64(dados.CnpjRem).ToString(@"00\.000\.000\/0000\-00");
                                }
                                dataDados = new DataSet();
                                dataDados = buscaDados(arq.Name, "CGCCFO = '" + cnpjAux + "' AND (FCFO.CODCOLIGADA = 0 OR FCFO.CODCOLIGADA =  " + dados.SColigada + ") AND ATIVO = 1", "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];

                                    expression = "CGCCFO = '" + cnpjAux + "'";
                                    if (this.bAtivaLog)
                                    {
                                        fLog(arq.Name, expression);
                                    }
                                    sort = "CODCOLIGADA ASC";


                                    // Use the Select method to find all rows matching the filter.

                                    foundRows = dt.Select(expression, sort);
                                    for (int i = 0; i < foundRows.Length; i++)
                                    {
                                        dados.SColCfoRem = foundRows[i]["CODCOLIGADA"].ToString();
                                        dados.SCodCfoRem = foundRows[i]["CODCFO"].ToString();
                                    }
                                }
                                if (dados.SColCfoRem == "" || string.IsNullOrEmpty(dados.SColCfoRem))
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjRem + "','" + dados.NomeRem + "', 'CLIENTE / FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui49");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }

                                }
                            }
                            if (dados.CnpjDest != cnpjEmpresa.Replace(".", "").Replace("/", "").Replace("-", ""))
                            {
                                if (tipoDest == 1)
                                {
                                    cnpjAux = Convert.ToUInt64(dados.CnpjDest).ToString(@"000\.000\.000\-00");
                                }
                                else
                                {
                                    cnpjAux = Convert.ToUInt64(dados.CnpjDest).ToString(@"00\.000\.000\/0000\-00");
                                }
                                dataDados = new DataSet();
                                dataDados = buscaDados(arq.Name, "CGCCFO = '" + cnpjAux + "' AND (FCFO.CODCOLIGADA = 0 OR FCFO.CODCOLIGADA =  " + dados.SColigada + ") AND ATIVO = 1", "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];

                                    expression = "CGCCFO = '" + cnpjAux + "'";
                                    if (this.bAtivaLog) { fLog(arq.Name, expression); }
                                    sort = "CODCOLIGADA ASC";


                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression, sort);
                                    for (int i = 0; i < foundRows.Length; i++)
                                    {
                                        dados.SColCfoDest = foundRows[i]["CODCOLIGADA"].ToString();
                                        dados.SCodCfoDest = foundRows[i]["CODCFO"].ToString();
                                    }
                                }
                                if (dados.SColCfoDest == "" || string.IsNullOrEmpty(dados.SColCfoDest))
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjDest + "','" + dados.NomeDest + "', 'CLIENTE / FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C',0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui50");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }

                                }
                            }
                            try
                            {
                                //Busca Centro de Custo
                                dataDados = new DataSet();
                                dataDados = buscaDados(arq.Name, "FCFODEF.CODCOLCFO = " + dados.SColCfoEmi + " AND FCFODEF.CODCOLIGADA =  " + dados.SColigada + " AND FCFODEF.CODCFO = '" + dados.SCodCfoEmi + "'", "FinCFODEFDataBR", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    linha = dt.Rows[0];
                                    dados.SCodccusto = linha["CODCCUSTO"].ToString();
                                }
                                else
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'FORNECEDOR NÃO POSSUI CENTRO DE CUSTO DEFAULT', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui51");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                }


                            }
                            catch (Exception)
                            {

                            }
                            /*try
                            {
                                //Busca Forma de Pagamento
                                
                                dataDados = new DataSet(); dataDados = buscaDados(arq.Name, "TCPGFCFO.CODCFO = '" + dados.SCodCfoEmi + "' AND TCPGFCFO.CODCOLCFO = " + dados.SColCfoEmi + " AND TCPGFCFO.CODCOLIGADA =  " + dados.SColigada + " AND TCPGFCFO.DEFAULTCOMPRA = 1", "MovCPGFCFOData", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    linha = dt.Rows[0];
                                    dados.CodCpg = linha["CODCPGCOMPRA"].ToString();
                                }
                                else
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO) VALUES ('" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'FORNECEDOR NÃO POSSUI CONDIÇÃO DE PAGAMENTO DE COMPRA DEFAULT', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082)", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            continue;
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {

                            }
                            try
                            {
                                //Busca Local de Estoque
                                dataDados = new DataSet();
                                dataDados = buscaDados(arq.Name, "TLOC.CODCOLIGADA = " + dados.SColigada + " AND TLOC.CODFILIAL = " + dados.SFilial + " AND TLOC.INATIVO = 0", "EstLOCData", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    linha = dt.Rows[0];
                                    dados.SCodLoc = linha["CODLOC"].ToString();
                                }
                                else
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO) VALUES ('" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'NENHUM LOCAL DE ESTOQUE ENCONTRADO PARA A FILIAL', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082)", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            continue;
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {

                            }*/
                            dados.SCodLoc = codLocCte;
                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:nCT", ns2);
                            dados.SNumeromov = node.InnerXml.ToString(); //Numero do Movimento 
                            if (dados.SNumeromov.Length <= tamNumeroMov)
                            {
                                string nAux = new string('0', tamNumeroMov - dados.SNumeromov.Length);
                                dados.SNumeromov = nAux + dados.SNumeromov;
                            }
                            else
                            {
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'TAMANHO DO NUMERO DE DOCUMENTO INFORMADO É MENOR QUE O TAMANHO DO NÚMERO DO CTE', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                        oReader.Close();
                                        atualizaCritica(arq.Name);
                                        continue;
                                    }
                                    catch (Exception ex2)
                                    {
                                        fLog(arq.Name, "aqui52");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        oReader.Close();
                                        //File.Delete(sCriticados & "\" & arq.Name)
                                        //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                        //File.Delete(caminho & "\" & arq.Name)
                                        atualizaCritica(arq.Name);
                                        continue;
                                    }
                                }
                            }

                            node = xpathNav.SelectSingleNode("//cte:infCte/cte:vPrest/cte:vTPrest", ns2);
                            dados.DvProduto = node.InnerXml.ToString().Replace(".", ",");
                            //busca natureza
                            try
                            {
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMS00/cte:CST", ns2);
                                if (node != null)
                                {
                                    dados.SCodSTIcms = node.InnerXml.ToString();
                                }
                                else 
                                {
                                    node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMSSN/cte:CST", ns2);

                                    if (node != null)
                                    {
                                        dados.SCodSTIcms = node.InnerXml.ToString();
                                    }
                                    else
                                    {
                                        dados.SCodSTIcms = "";
                                    }
                                    

                                }
                            }
                            catch { }
                            try
                            {
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMS00/cte:vBC", ns2);
                                if (node != null)
                                {
                                    dados.SBCIcms = node.InnerXml.ToString().Replace(".", ",");
                                }
                                else
                                {
                                    node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMSSN/cte:vBC", ns2);
                                    if (node != null)
                                    {
                                        dados.SBCIcms = node.InnerXml.ToString().Replace(".", ",");
                                    }
                                    else
                                    {
                                        dados.SBCIcms = "0";
                                    }
                                }
                            }
                            catch { }
                            try
                            {
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMS00/cte:pICMS", ns2);
                                if (node != null)
                                {
                                    dados.DAliqIcms = node.InnerXml.ToString().Replace(".", ",");
                                }

                                else
                                {
                                    node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMSSN/cte:pICMS", ns2);
                                    if (node != null)
                                    {
                                        dados.DAliqIcms = node.InnerXml.ToString().Replace(".", ",");
                                    }

                                    else
                                    {
                                        dados.DAliqIcms = "0";
                                    }
                                }
                            }
                            catch
                            { }
                            try
                            {
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMS00/cte:vICMS", ns2);
                                if (node != null)
                                {
                                    dados.DValIcms = node.InnerXml.ToString().Replace(".", ",");
                                }
                                else
                                {
                                    node = xpathNav.SelectSingleNode("//cte:infCte/cte:imp/cte:ICMS/cte:ICMSSN/cte:vICMS", ns2);
                                    if (node != null)
                                    {
                                        dados.DValIcms = node.InnerXml.ToString().Replace(".", ",");
                                    }
                                    else
                                    {
                                        dados.DValIcms = "0";
                                    }
                                }
                            }
                            catch
                            { }
                            try
                            {
                                string idnatColigada = "";
                                string idnat = "";
                                ArrayList idnatInversa = new ArrayList();
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:CFOP", ns2);
                                dados.Cfop = node.InnerXml.ToString();
                                dados.Cfop = Convert.ToInt16(dados.Cfop).ToString(@"0\.000");
                                DataSet dataNat = new DataSet();
                                if (this.bAtivaLog) { fLog(arq.Name, "Busca dados Natureza"); }
                                dataDados = buscaDados(arq.Name, "(DCFOP.CODCOLIGADA = " + dados.SColigada + " OR DCFOP.codcoligada = 0) AND CODNAT = '" + dados.Cfop + "'", "FisNaturezaData", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    expression = "CODNAT = '" + dados.Cfop + "'";
                                    sort = "CODCOLIGADA ASC";

                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression, sort);
                                    for (int i = 0; i < foundRows.Length; i++)
                                    {
                                        idnatColigada = foundRows[i]["CODCOLIGADA"].ToString();
                                        idnat = foundRows[i]["IDNAT"].ToString();
                                    }
                                }
                                if (idnatColigada == "" || string.IsNullOrEmpty(idnatColigada))
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'CFOP " + dados.Cfop + " NÃO ENCONTRADA NO SISTEMA', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            oReader.Close();
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui53");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                }
                                fLog(arq.Name, "Busca dados Natureza Inversa");
                                dataDados = buscaDados(arq.Name, "CODCOLIGADA = " + idnatColigada + " AND IDNATORIGEM = '" + idnat + "'", "MovNaturezaColabData", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    expression = "IDNATORIGEM = '" + idnat + "'";
                                    sort = "CODCOLIGADA ASC";

                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression, sort);
                                    for (int i = 0; i < foundRows.Length; i++)
                                    {
                                        idnatInversa.Add(foundRows[i]["IDNATINVERSA"].ToString());
                                    }
                                }
                                if (idnatInversa.Count == 0)
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'NENHUMA NATUREZA INVERSA ENCONTRADA PARA A CFOP: " + dados.Cfop + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui54");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                }
                                fLog(arq.Name, "Busca dados Natureza para inserção");
                                foreach (string item in idnatInversa)
                                {
                                    try
                                    {
                                        dataDados = buscaDados(arq.Name, "DCFOP.CODCOLIGADA = " + idnatColigada + "  AND ALIQICMS = " + dados.DAliqIcms.ToString().Replace(",", ".") + " AND DCFOP.IDNAT = " + item.ToString(), "FisNaturezaData", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataDados.Tables.Count > 0)
                                        {
                                            dt = dataDados.Tables[0];
                                            linha = dt.Rows[0];
                                            dados.Idnat = linha["IDNAT"].ToString();
                                            break;
                                        }
                                    }
                                    catch (Exception ex)
                                    { }
                                }
                                if (dados.Idnat == "" || string.IsNullOrEmpty(dados.Idnat))
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        fLog(arq.Name, "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'NENHUMA NATUREZA INVERSA ATENDE AS REGRAS DE ICMS DO CTE', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082, '" + dados.SCodSTIcms + "', '" + dados.DAliqIcms.ToString().Replace(",", ".") + "',0,0,'1')");

                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', 'NENHUMA NATUREZA INVERSA ATENDE AS REGRAS DE ICMS DO CTE', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082, '" + dados.SCodSTIcms + "', '" + dados.DAliqIcms.ToString().Replace(",", ".") + "',0,0,'1')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            oReader.Close();
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui55: " +ex2.Message);
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            //File.Delete(sCriticados & "\" & arq.Name)
                                            //File.Copy(caminho & "\" & arq.Name, sCriticados & "\" & arq.Name)
                                            //File.Delete(caminho & "\" & arq.Name)
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                }
                                if (this.bAtivaLog) { fLog(arq.Name, "Busca demais dados"); }
                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:cMunIni", ns2);
                                dados.MunicipioIni = node.InnerXml.ToString().Substring(2, 5);

                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:cMunFim", ns2);
                                dados.MunicipioFim = node.InnerXml.ToString().Substring(2, 5);

                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:UFIni", ns2);
                                dados.UfIni = node.InnerXml.ToString();

                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:UFFim", ns2);
                                dados.UfFim = node.InnerXml.ToString();

                                node = xpathNav.SelectSingleNode("//cte:infCte/cte:ide/cte:serie", ns2);
                                dados.Serie = node.InnerXml.ToString();

                                //node = xpathNav.SelectSingleNode("//cte:infCte/cte:compl/cte:xObs", ns2);
                                //dados.Obs = node.InnerXml.ToString();

                                //node = xpathNav.SelectSingleNode("//cte:infCte/cte:compl/cte:ObsCont/cte:xTexto", ns2);
                                //dados.Obs = dados.Obs + node.InnerXml.ToString();
                            }
                            catch (Exception)
                            { }

                            if (xmlErro)
                            {
                                if (this.bAtivaLog) { fLog(arq.Name, "Tem erro"); }
                                if (oReader != null)
                                {
                                    oReader.Close();
                                }
                                atualizaCritica(arq.Name);
                                continue;
                            }
                            else
                            {
                                dados.CodTdo = codDocCte;
                                string resultado = "";
                                resultado = PopulaTabelasCte(dados, e);

                                if (resultado.Substring(0, dados.SColigada.Length) == dados.SColigada & resultado.Substring(dados.SColigada.Length, 1) == ";")
                                {

                                    oReader.Close();
                                    File.Delete(sProcessado + "\\" + arq.Name);
                                    File.Copy(caminho + "\\" + arq.Name, sProcessado + "\\" + arq.Name);
                                    File.Delete(caminho + "\\" + arq.Name);
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {

                                        SqlCommand command = new SqlCommand("DELETE FROM ZCRITICAXML WHERE NOME_XML = '" + arq.Name + "'", connection);
                                        try
                                        {

                                            connection.Open();
                                            command.ExecuteReader();
                                        }
                                        catch (Exception)
                                        {
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                }
                                else
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {

                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dados.SColigada + "','" + dados.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dados.SDataEmissao + "', 121), 'CMP','" + dados.SCodCfoEmi + "', '" + dados.CnpjEmi + "','" + dados.NomeEmi + "', '" + resultado.ToString().Substring(0, 230) + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'1')", connection);
                                        try
                                        {

                                            connection.Open();
                                            command.ExecuteReader();
                                            oReader.Close();
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui56");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (nfe)
                    {
                        vinculado = true;
                        aguardaVex = false;
                        ignoraPreco = false;
                        itensNfe item = new itensNfe();
                        dadosNfe dadosNF = new dadosNfe();
                        dadosVex dadosV = new dadosVex();
                        dadosSenior dadosS = new dadosSenior();
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Inicia Dados NFE");
                            fLog(arq.Name, "Emissor");
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CNPJ", ns);
                        dadosNF.CnpjEmi = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:xNome", ns);
                        dadosNF.NomeEmi = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Destinatario");
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:CNPJ", ns);
                        dadosNF.CnpjDest = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:dest/nfe:xNome", ns);
                        dadosNF.NomeDest = node.InnerXml.ToString(); //'Cnpj do Emitente da NF
                        node = xpathNav.SelectSingleNode("//nfe:infNFe", ns);
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "ChaveAcesso");
                        }
                        dadosNF.ChaveAcesso = node.GetAttribute("Id", "").ToString();       //Chave de acesso da NF-e
                        dadosNF.ChaveAcesso = dadosNF.ChaveAcesso.Replace("NFe", "");

                        using (SqlConnection connection = dbt.GetConnection())
                        {
                            string nomeArquivo;
                            SqlCommand command = new SqlCommand("SELECT DISTINCT NOME_XML FROM ZCRITICAXML(NOLOCK) WHERE CHAVEACESSO = '" + dadosNF.ChaveAcesso + "'", connection);
                            try
                            {
                                fLog(arq.Name, "selqct");
                                connection.Open();
                                SqlDataReader dr;
                                dr = command.ExecuteReader();
                                if (dr.HasRows)
                                {
                                    while (dr.Read())
                                    {
                                        fLog(arq.Name, "tem linha");
                                        nomeArquivo = dr[0].ToString();
                                        if (nomeArquivo != arq.Name)
                                        {
                                            fLog(arq.Name, "esta duplicado");
                                            duplicado = true;
                                        }
                                    }
                                    dr.Close();
                                }

                            }
                            catch (Exception ex)
                            {
                                fLog(arq.Name, "aqui57");
                                fLog(arq.Name, ex.Message);
                                command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                command.ExecuteReader();
                                this.oReader.Close();
                                atualizaCritica(arq.Name);
                                continue;
                            }
                        }
                        if (duplicado)
                        {
                            if (this.bAtivaLog)
                            {
                                fLog(arq.Name, "Enviado para pasta Criticado XML Duplicado");
                            }
                            oReader.Close();
                            File.Delete(sCriticados + "\\" + arq.Name);
                            File.Copy(caminho + "\\" + arq.Name, sCriticados + "\\" + arq.Name);
                            File.Delete(caminho + "\\" + arq.Name);
                            atualizaCritica(arq.Name);
                            continue;
                        }
                        using (SqlConnection connection = dbt.GetConnection())
                        {
                            SqlCommand command = new SqlCommand("SELECT DISTINCT CHAVEACESSO FROM STATUS_NOTA WHERE CHAVEACESSO = '" + dadosNF.ChaveAcesso + "' AND STATUSNOTA = 7", connection);
                            try
                            {
                                fLog(arq.Name, "selqctSTATUSIGNORADA");
                                connection.Open();
                                SqlDataReader dr;
                                dr = command.ExecuteReader();
                                if (dr.HasRows)
                                {
                                    ignorada = true;
                                }

                            }
                            catch (Exception ex)
                            {
                                fLog(arq.Name, "aqui57erro ignorada");
                                fLog(arq.Name, ex.Message);
                                command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                command.ExecuteReader();
                                this.oReader.Close();
                                continue;
                            }
                        }
                        if (ignorada)
                        {
                            if (this.bAtivaLog)
                            {
                                fLog(arq.Name, "Enviado para pasta Ignoradas");
                            }
                            oReader.Close();
                            File.Delete(sIgnorada + "\\" + arq.Name);
                            File.Copy(caminho + "\\" + arq.Name, sIgnorada + "\\" + arq.Name);
                            File.Delete(caminho + "\\" + arq.Name);
                            atualizaCritica(arq.Name);
                            continue;
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:nNF", ns);
                        dadosNF.SNumeromov = node.InnerXml.ToString(); //Numero do Movimento 
                        dadosNF.SegundoNumero = node.InnerXml.ToString();
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vNF", ns);
                        dadosNF.TotalNf = node.InnerXml.ToString().Replace(".", ",");
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vProd", ns);
                        dadosS.Vlrprodutos = node.InnerXml.ToString().Replace(".", ",");
                        dadosS.Vlrtotal = dadosNF.TotalNf;
                       
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:modFrete", ns);
             
                        if (node != null)
                        {
                            string auxVprod = node.ToString();
                            switch (auxVprod)
                            {
                                case "0":
                                    dadosS.Ciffob = "1";
                                    break;
                                case "1":
                                    dadosS.Ciffob = "2";
                                    break;
                                case "2":
                                    dadosS.Ciffob = "4";
                                    break;
                                default:
                                    dadosS.Ciffob = "5";
                                    break;

                            }
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:transp/nfe:vol/nfe:pesoL", ns);
                        if(node != null)
                        {
                            dadosS.Pesoliquido = node.ToString().Replace('.', ',');
                        }
                        

                        fLog(arq.Name, "12012023-1");
                        enviavex = buscaEnviaVex(dadosNF.ChaveAcesso, arq.Name);
                        fLog(arq.Name, "12012023-2");
                        enviavexnovo = buscaEnviaVexNovo(dadosNF.ChaveAcesso, arq.Name);
                        fLog(arq.Name, "12012023-3");
                        //ignoraPreco = buscaIgnoraPreco(dadosNF.ChaveAcesso, arq.Name);
                        fLog(arq.Name, "Aguarda Vex");
                        /*bool travaAguardaVex = false;
                        bool travaAguardaVex = aguardaVexMetodo(dadosNF.ChaveAcesso, arq.Name);
                        if (travaAguardaVex)
                        {
                            fLog(arq.Name, "Aguarda Vex");
                            atualizaCritica(arq.Name);
                            continue;
                        }*/
                        if (this.bAtivaLog)
                        {
                            fLog(arq.Name, "Data Emissão");
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:dhEmi", ns);
                        dadosNF.SDataEmissao = node.InnerXml.ToString().Substring(0, 10);
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:natOp", ns);

                        try
                        {
                            dadosS.Descroper = node.InnerXml.ToString().Substring(0, 10);
                        }
                        catch
                        {
                            dadosS.Descroper = node.InnerXml.ToString();
                        }
                        
                        cnpjEmpresa = null;
                        long aux = long.Parse(dadosNF.CnpjDest);
                        string RaizCnpj = string.Format(@"{0:00\.000\.000\/0000\-00}", aux);
                        try
                        {
                            RaizCnpj = RaizCnpj.Substring(0, RaizCnpj.IndexOf("/"));
                        }
                        catch
                        {
                            RaizCnpj = RaizCnpj.Substring(0, 8);
                        }
                        //le os parametros pelo cnpj encontrado
                        using (SqlConnection connection = dbt.GetConnection())
                        {
                            SqlCommand command = new SqlCommand("SELECT CNPJ,MOVCTE,IDPRDCTE,TAMNUMEROMOV,CODTDOCTE,CODTDONFE,COLTRANSP,CODTRANSP,CODLOCCTE,CODLOCNFE,CODCOLIGADA,MOVNFS,IDNAT,IDNATEXT,SERIENFS,CODTDONFS, TMVORIGEM_NFE, TMVORIGEM_NFS, TMVDESTINO_NFE, TMVDESTINO_NFS, APPROVAL_VALUE_LOW, APPROVAL_VALUE_HIGH FROM ZPARAMETROS(NOLOCK) WHERE CNPJ LIKE '%" + RaizCnpj + "%'", connection);
                            try
                            {
                                fLog(arq.Name, "12012023-4");
                                connection.Open();
                                SqlDataReader dr;
                                fLog(arq.Name, "12012023-6");
                                dr = command.ExecuteReader();
                                if (dr.HasRows)
                                {
                                    while (dr.Read())
                                    {
                                        fLog(arq.Name, "12012023-5");
                                        movCte = dr[1].ToString();
                                        idprdCte = int.Parse(dr[2].ToString());
                                        tamNumeroMov = int.Parse(dr[3].ToString());
                                        codDocCte = dr[4].ToString();
                                        codDocNfe = dr[5].ToString();
                                        colTransp = dr[6].ToString();
                                        codTransp = dr[7].ToString();
                                        codLocCte = dr[8].ToString();
                                        codLocNfe = dr[9].ToString();
                                        sCodColigada = int.Parse(dr[10].ToString());
                                        movNfs = dr[11].ToString();
                                        idNatNfsInt = dr[12].ToString();
                                        idNatNfsExt = dr[13].ToString();
                                        serieNF = dr[14].ToString();
                                        codDocNfs = dr[15].ToString();
                                        tmvorignfe = dr[16].ToString();
                                        tmvorignfs = dr[17].ToString();
                                        tmvdestnfe = dr[18].ToString();
                                        tmvdestnfs = dr[19].ToString();
                                        minAp = dr.GetDecimal(20);
                                        maxAp = dr.GetDecimal(21);
                                        enviarVex = true;
                                        fLog(arq.Name, "12012023-7");
                                    }
                                }
                                dr.Close();
                            }
                            catch (Exception ex)
                            {
                                fLog(arq.Name, ex.Message);
                                command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('DADOS INICIAIS', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                command.ExecuteReader();
                                // this.oReader.Close();
                            }
                        }

                        //esta errado verificar
                        cnpjEmpresa = ResultadoSQL("SELECT CNPJ FROM ZPARAMETROS(NOLOCK) WHERE CNPJ LIKE ('%" + RaizCnpj + "%')", 0);
                        //pega a filial e a coligada
                        if (dadosNF.CnpjDest == cnpjEmpresa.Replace(".", "").Replace("/", "").Replace("-", ""))
                        {
                            dataDados = new DataSet();
                            dataDados = validaCnpjEmpresa(arq.Name);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables[0];
                                expression = "CGC = '" + cnpjEmpresa + "'";
                                if (this.bAtivaLog)
                                {
                                    fLog(arq.Name, expression);
                                }
                                // Use the Select method to find all rows matching the filter.
                                foundRows = dt.Select(expression);
                                for (int i = 0; i < foundRows.Length; i++)
                                {

                                    dadosNF.SColigada = foundRows[i]["CODCOLIGADA"].ToString();
                                    dadosNF.SFilial = foundRows[i]["CODFILIAL"].ToString();
                                }

                                if (enviarVex && dadosNF.SFilial != "" && !string.IsNullOrEmpty(dadosNF.SFilial))
                                {
                                    try
                                    {
                                        dataDados = buscaDadosRecord(arq.Name, dadosNF.SColigada + ";" + dadosNF.SFilial, "FisFilialDataBR", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataDados.Tables.Count > 0)
                                        {
                                            dt = dataDados.Tables["GFILIAL"];
                                            expression = "CODFILIAL = '" + dadosNF.SFilial + "'";
                                            if (this.bAtivaLog)
                                            {
                                                fLog(arq.Name, expression);
                                            }
                                            sort = "CODFILIAL DESC";

                                            //Use the Select method to find all rows matching the filter.F
                                            foundRows = dt.Select(expression, sort);
                                            for (int j = 0; j < foundRows.Length; j++)
                                            {
                                                try { dadosV.DestCEP = foundRows[j]["CEP"].ToString(); } catch { }
                                                try { dadosV.DestCNPJ = foundRows[j]["CGC"].ToString(); } catch { }
                                                try { dadosV.DestCodigoIBGE = int.Parse(foundRows[j]["CODMUNICIPIO"].ToString()); } catch { }
                                                try { dadosV.DestComplemento = foundRows[j]["COMPLEMENTO"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.DestEndereco = foundRows[j]["RUA"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.DestFantasia = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.DestIE = foundRows[j]["INSCRICAOESTADUAL"].ToString(); } catch { }
                                                try { dadosV.DestNumero = foundRows[j]["NUMERO"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.DestRazaoSocial = foundRows[j]["NOME"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.DestTipoDeEndereco = foundRows[j]["TIPORUA"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.DestUF = foundRows[j]["ESTADO"].ToString().Replace("'", ""); } catch { }

                                                try { dadosS.Cep_Dest = foundRows[j]["CEP"].ToString(); } catch { }
                                                try { dadosS.Cnpj_Dest = foundRows[j]["CGC"].ToString(); } catch { }
                                                try { dadosS.Cnpj_Depositante = foundRows[j]["CGC"].ToString(); } catch { }
                                                try { dadosS.Cnpj_Unidade = foundRows[j]["CGC"].ToString(); } catch { }
                                                try { dadosS.Endereco_Dest = foundRows[j]["RUA"].ToString(); } catch { }
                                                try { dadosS.Estado_Dest = foundRows[j]["ESTADO"].ToString(); } catch { }
                                                try { dadosS.Nome_Dest = foundRows[j]["NOME"].ToString(); } catch { }
                                                try { dadosS.Nome_Transp = foundRows[j]["NOME"].ToString(); } catch { }
                                                try { dadosS.Cnpj_Transp = foundRows[j]["CGC"].ToString(); } catch { }
                                                try { dadosS.Bairro_Dest = foundRows[j]["BAIRRO"].ToString(); } catch { }
                                                try { dadosS.Cidade_Dest = foundRows[j]["CIDADE"].ToString(); } catch { }
                                            }
                                        }
                                    }
                                    catch (Exception ex) { }
                                }

                            }
                        }
                        if (dadosNF.SColigada == "" || string.IsNullOrEmpty(dadosNF.SColigada))
                        {
                            oReader.Close();
                            atualizaCritica(arq.Name);
                            continue;
                        }
                        //verifica se a nota já foi importada
                        if (fvalXmlProcessadoTransf(arq.Name, dadosNF.SColigada, dadosNF.ChaveAcesso, "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC))
                        {
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("INSERT INTO ZLOGEVENTOSXML (NOME_XML, DATAEMISSAO, SETOR, USUARIO, EVENTO, CRITICA) VALUES ('" + arq.Name + "',convert(datetime, '" + String.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), 'FIS', '" + sCodUsuario + "', 'I', 'XML TRANSFERIDO PARA A PASTA CRITICADOS, O MESMO JA FOI LANCADO NO SISTEMA.')", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception ex2)
                                {
                                    fLog(arq.Name, "aqui59");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    atualizaCritica(arq.Name);
                                    continue;
                                }
                            }
                            /*
                            oReader.Close();
                            if (this.bAtivaLog)
                            {
                                fLog(arq.Name, "Enviado para pasta Criticado");
                            }
                            File.Delete(sCriticados + "\\" + arq.Name);
                            File.Copy(caminho + "\\" + arq.Name, sCriticados + "\\" + arq.Name);
                            File.Delete(caminho + "\\" + arq.Name);
                            */
                            string pedidosInclusos = "";
                            oReader.Close();
                            File.Delete(sProcessado + "\\" + arq.Name);
                            File.Copy(caminho + "\\" + arq.Name, sProcessado + "\\" + arq.Name);
                            File.Delete(caminho + "\\" + arq.Name);

                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("SELECT DISTINCT NUMEROMOV_ASS FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNF.ChaveAcesso + "'", connection);
                                try
                                {
                                    connection.Open();
                                    SqlDataReader dr;
                                    dr = command.ExecuteReader();
                                    if (dr.HasRows)
                                    {
                                        while (dr.Read())
                                        {
                                            if (pedidosInclusos != "")
                                            {
                                                pedidosInclusos = pedidosInclusos + ", ";
                                            }
                                            pedidosInclusos = pedidosInclusos = dr[0].ToString();
                                        }
                                    }
                                    dr.Close();

                                }
                                catch (Exception ex)
                                {
                                    fLog(arq.Name, "erro busca pedidos");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                }
                            }
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("DELETE FROM ZCRITICAXML WHERE NOME_XML = '" + arq.Name + "' DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNF.ChaveAcesso + "'", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();

                                    fLog("", "");
                                    fLog("=========================================", "");
                                    fLog("(NFE) " + arq.Name, ": JÁ IMPORTADA!");
                                    fLog("=========================================", "");
                                }
                                catch (Exception)
                                {
                                    atualizaCritica(arq.Name);
                                    continue;
                                }
                            }
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("DELETE FROM ZXMLPGTO WHERE NOME_XML = '" + arq.Name + "'", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception)
                                {
                                    atualizaCritica(arq.Name);
                                    continue;
                                }
                            }
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON, PEDIDO) VALUES('" + dadosNF.ChaveAcesso + "', 5,'" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE(), '" + pedidosInclusos + "')", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                    xmlErro = true;
                                }
                                catch (Exception ex2)
                                {
                                    fLog(arq.Name, "aqui65");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                    atualizaCritica(arq.Name);
                                    continue;
                                }
                            }
                            atualizaCritica(arq.Name);
                            continue;
                        }
                        else
                        {
                            //pega os dados do fornecedor
                            cnpjAux = Convert.ToUInt64(dadosNF.CnpjEmi).ToString(@"00\.000\.000\/0000\-00");
                            dataDados = new DataSet();
                            dataDados = buscaDados(arq.Name, "CGCCFO = '" + cnpjAux + "' AND (FCFO.CODCOLIGADA = 0 OR FCFO.CODCOLIGADA =  " + dadosNF.SColigada + ") AND ATIVO = 1", "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables[0];
                                expression = "CGCCFO = '" + cnpjAux + "'";
                                if (this.bAtivaLog)
                                {
                                    fLog(arq.Name, expression);
                                }
                                sort = "CODCOLIGADA ASC";
                                foundRows = dt.Select(expression, sort);
                                for (int i = 0; i < foundRows.Length; i++)
                                {
                                    dadosNF.SColCfoEmi = foundRows[i]["CODCOLIGADA"].ToString();
                                    dadosNF.SCodCfoEmi = foundRows[i]["CODCFO"].ToString();
                                    dadosNF.EstadoNota = foundRows[i]["CODETD"].ToString();
                                }
                                if (enviarVex && dadosNF.SColCfoEmi != "" && !string.IsNullOrEmpty(dadosNF.SCodCfoEmi))
                                {
                                    try
                                    {
                                        dataDados = buscaDadosRecord(arq.Name, dadosNF.SColCfoEmi + ";" + dadosNF.SCodCfoEmi, "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataDados.Tables.Count > 0)
                                        {
                                            dt = dataDados.Tables["FCFO"];
                                            expression = "CODCFO = '" + dadosNF.SCodCfoEmi + "'";
                                            if (this.bAtivaLog)
                                            {
                                                fLog(arq.Name, expression);
                                            }
                                            sort = "CODCFO DESC";

                                            //Use the Select method to find all rows matching the filter.F
                                            foundRows = dt.Select(expression, sort);
                                            for (int j = 0; j < foundRows.Length; j++)
                                            {

                                                try { dadosNF.Idcfofiscal = foundRows[j]["IDCFOFISCAL"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemCEP = foundRows[j]["CEP"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemCNPJ = foundRows[j]["CGCCFO"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemCodigoIBGE = int.Parse(foundRows[j]["CODMUNICIPIO"].ToString()); } catch { }
                                                try { dadosV.RemComplemento = foundRows[j]["COMPLEMENTO"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemEndereco = foundRows[j]["RUA"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemFantasia = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemIE = foundRows[j]["INSCRESTADUAL"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemNumero = foundRows[j]["NUMERO"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemRazaoSocial = foundRows[j]["NOME"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemTipoDeEndereco = foundRows[j]["TIPORUA"].ToString().Replace("'", ""); } catch { }
                                                try { dadosV.RemUF = foundRows[j]["CODETD"].ToString().Replace("'", ""); } catch { }


                                                try { dadosS.Cnpj_Emitente = foundRows[j]["CGCCFO"].ToString(); } catch { }
                                                try { dadosS.Inscrestadual_Emitente = foundRows[j]["INSCRESTADUAL"].ToString(); } catch { }
                                                try { dadosS.Nome_Emit = foundRows[j]["NOME"].ToString(); } catch { }
                                                try { dadosS.Fantasia_Emit = foundRows[j]["NOMEFANTASIA"].ToString(); } catch { }

                                            }
                                        }
                                    }
                                    catch (Exception ex) { }
                                }
                            }
                            if (dadosNF.SColCfoEmi == "" || string.IsNullOrEmpty(dadosNF.SColCfoEmi))
                            {
                                enviavex = false;
                                enviavexnovo = false;
                                //fornecedor não encontrado
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "','" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'CLIENTE / FORNECEDOR NAO CADASTRADO NO SISTEMA', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                        xmlErro = true;
                                    }
                                    catch (Exception ex2)
                                    {
                                        fLog(arq.Name, "aqui60");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        oReader.Close();
                                        atualizaCritica(arq.Name);
                                        continue;
                                    }
                                }
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 1,'CMP','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                        xmlErro = true;
                                    }
                                    catch (Exception ex2)
                                    {
                                        fLog(arq.Name, "aqui62");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        oReader.Close();
                                        atualizaCritica(arq.Name);
                                        continue;
                                    }
                                }

                            }
                            else
                            {
                                try
                                {
                                    //Busca Centro de Custo do fornecedor
                                    dataDados = new DataSet();
                                    dataDados = buscaDados(arq.Name, "FCFODEF.CODCOLCFO = " + dadosNF.SColCfoEmi + " AND FCFODEF.CODCOLIGADA =  " + dadosNF.SColigada + " AND FCFODEF.CODCCUSTO IS NOT NULL AND FCFODEF.CODCFO = '" + dadosNF.SCodCfoEmi + "'", "FinCFODEFDataBR", sCodUsuarioTBC, sSenhaTBC);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        dt = dataDados.Tables[0];
                                        linha = dt.Rows[0];
                                        dadosNF.SCodccusto = linha["CODCCUSTO"].ToString();
                                    }
                                    else
                                    {
                                        using (SqlConnection connection = dbt.GetConnection())
                                        {
                                            SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "','" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'FORNECEDOR NÃO POSSUI CENTRO DE CUSTO DEFAULT', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')", connection);
                                            try
                                            {
                                                connection.Open();
                                                command.ExecuteReader();
                                                xmlErro = true;
                                            }
                                            catch (Exception ex2)
                                            {
                                                fLog(arq.Name, "aqui63");
                                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                command.ExecuteReader();
                                                oReader.Close();
                                                atualizaCritica(arq.Name);
                                                continue;
                                            }
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    xmlErro = true;
                                    if (this.bAtivaLog)
                                    {
                                        fLog(arq.Name, "Tem erro linha 2099");
                                    }
                                }
                                /* BUSCAR FORMA DE PAGAMENTO DO PEDIDO
                                try
                                {
                                    //Busca Forma de Pagamento default
                                    dataDados = new DataSet(); dataDados = buscaDados(arq.Name, "TCPGFCFO.CODCFO = '" + dadosNF.SCodCfoEmi + "' AND TCPGFCFO.CODCOLCFO = " + dadosNF.SColCfoEmi + " AND TCPGFCFO.CODCOLIGADA =  " + dadosNF.SColigada + " AND TCPGFCFO.DEFAULTCOMPRA = 1", "MovCPGFCFOData", sCodUsuarioTBC, sSenhaTBC);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        dt = dataDados.Tables[0];
                                        linha = dt.Rows[0];
                                        dadosNF.CodCpg = linha["CODCPGCOMPRA"].ToString();
                                    }
                                    else
                                    {
                                        //busca condição de pagamento informada na tela de critica
                                        using (SqlConnection connection = dbt.GetConnection())
                                        {
                                            SqlCommand command = new SqlCommand("SELECT CONDPGTO FROM ZXMLPGTO(NOLOCK) WHERE NOME_XML = '" + arq.Name + "'", connection);
                                            try
                                            {
                                                connection.Open();
                                                SqlDataReader dr;
                                                dr = command.ExecuteReader();
                                                if (dr.HasRows)
                                                {
                                                    while (dr.Read())
                                                    {
                                                        dadosNF.CodCpg = dr[0].ToString();
                                                    }
                                                }
                                                dr.Close();

                                            }
                                            catch (Exception ex)
                                            {
                                                command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                                command.ExecuteReader();
                                                this.oReader.Close();
                                            }
                                        }
                                        if (dadosNF.CodCpg == "" || string.IsNullOrEmpty(dadosNF.CodCpg))
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'FORNECEDOR NÃO POSSUI CONDIÇÃO DE PAGAMENTO DE COMPRA DEFAULT', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2')", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    xmlErro = true;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    xmlErro = true;
                                    fLog(arq.Name, "Tem erro linha 2164");
                                }*/
                                //preenche demais dados
                                //dadosNF.SCodLoc = codLocNfe;
                                dadosNF.CodTdo = codDocNfe;
                                dadosNF.CodTra = codTransp;//buscaar da nota depois
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:nNF", ns);
                                dadosNF.SNumeromov = node.InnerXml.ToString(); //Numero do Movimento 
                                dadosNF.SegundoNumero = node.InnerXml.ToString();
                                dadosNF.Obs = "COMPRA DE MERCADORIAS REF.: " + node.InnerXml.ToString();
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:ide/nfe:serie", ns);
                                dadosNF.Serie = node.InnerXml.ToString(); //Numero do Movimento 
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vFrete", ns);
                                dadosNF.VFrete = node.InnerXml.ToString().Replace(".", ",");
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vSeg", ns);
                                dadosNF.VSeguro = node.InnerXml.ToString().Replace(".", ",");
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vDesc", ns);
                                dadosNF.VDesconto = node.InnerXml.ToString().Replace(".", ",");
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:total/nfe:ICMSTot/nfe:vOutro", ns);
                                dadosNF.VOutros = node.InnerXml.ToString().Replace(".", ",");
                                if (enviarVex)
                                {
                                    try { dadosV.Chave = dadosNF.ChaveAcesso; } catch { }
                                    try { dadosV.DataEmissaoNf = dadosNF.SDataEmissao; } catch { }
                                    try { dadosV.NumeroNfe = dadosNF.SNumeromov; } catch { }
                                    try { dadosV.Serie = dadosNF.Serie; } catch { }
                                    try { dadosS.Data_Emissao = dadosNF.SDataEmissao; } catch { }
                                    try { dadosS.CodigoInterno = dadosNF.SNumeromov; } catch { }
                                    try { dadosS.NumPedido = dadosNF.SNumeromov; } catch { }
                                    try { dadosS.Chaveacessonfe = dadosNF.ChaveAcesso; } catch { }
                                }
                                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:emit/nfe:CRT", ns);
                                dadosNF.Crt = int.Parse(node.InnerXml.ToString());
                                //caso tenha erro, não continua
                                if (xmlErro)
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 1,'CMP','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                            xmlErro = true;
                                        }
                                        catch (Exception ex2)
                                        {
                                            fLog(arq.Name, "aqui64");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            atualizaCritica(arq.Name);
                                            continue;
                                        }
                                    }
                                    fLog(arq.Name, "Tem erro");
                                    if (oReader != null)
                                    {
                                        oReader.Close();
                                    }
                                    atualizaCritica(arq.Name);
                                    continue;
                                }
                                else
                                {
                                    string codColResult, pontoEVirgula;
                                    xmlErro = false;
                                    dadosNF.MovNfe = "";
                                    bool xmlErroVex = false;
                                    if (xmlErro)
                                    {
                                        fLog(arq.Name, "Já está com erro antes do item");
                                    }
                                    xmlErro = LeItensNfe(caminho, arq, xmlDoc, e, dadosNF, dadosV, dadosS);
                                    
                                    if (xmlErro)
                                    {
                                        fLog(arq.Name, "Erro depois do item");
                                    }
                                    fLog(arq.Name, "chegou 1");
                                    temRetorno = temRetornoVex(dadosNF.ChaveAcesso, arq.Name);
                                    //temRetorno = false;
                                    if (temRetorno)
                                    {
                                        /*
                                        fLog(arq.Name, "entrou temretorno");
                                        xmlErroVex = quantidadeVex(caminho, arq, xmlDoc, dadosNF);
                                        if (xmlErroVex)
                                        {
                                            fLog(arq.Name, "Erro apos quantidadeVex");
                                        }
                                        */
                                    }
                                    else { xmlErro = true; }
                                    if (xmlErroVex == true || xmlErro == true)
                                    {
                                        xmlErro = true;
                                    }
                                    if (enviarVex && enviavex && vinculado)
                                    {
                                        string resultadoVex = "";
                                        resultadoVex = PopulaVex(dadosV, e);
                                        fLog(arq.Name, resultadoVex);
                                        if (!resultadoVex.Contains("Recebimento Cadastrado Com Sucesso") && !resultadoVex.Contains("Pedido de entrada já existente") && !resultadoVex.Contains("NF de entrada já existente"))
                                        {
                                            /*
                                            resultadoVex = resultadoVex.Replace("'", "");
                                            string insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'Erro Envio Vex: " + resultadoVex.ToString().Substring(0, 230) + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')";
                                            string insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                            CriticarXML(insertCritica, insertErrosAplic);
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 1,'CMP','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    xmlErro = true;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    fLog(arq.Name, "aqui66");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    atualizaCritica(arq.Name);
                                                    continue;
                                                }
                                            }
                                            */
                                        }
                                        else
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM REENVIOVEX WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO REENVIOVEX(CHAVEACESSO, REENVIO) VALUES('" + dadosNF.ChaveAcesso + "', 0)", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    xmlErro = true;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    fLog(arq.Name, "aqui66");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    atualizaCritica(arq.Name);
                                                    continue;
                                                }
                                            }

                                        }
                                        //dataDados = new DataSet();
                                        //dataDados = buscaDados(arq.Name, "CHAVEACESSONFE = '" + dadosNF.ChaveAcesso + "'", "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                                        //if (dataDados == null || dataDados.Tables.Count <= 0)
                                        //{

                                        //}
                                        //else
                                        //{
                                        //fLog(arq.Name, "chegou 55");
                                        //}
                                    }
                                    if (enviarVex && enviavexnovo && vinculado)
                                    {
                                        string resultadoVex = "";
                                        string reultadoSenior = "";
                                        resultadoVex = PopulaVexNovo(dadosV, e);
                                        try
                                        {
                                            populaSenior(dadosS);
                                        }
                                        catch
                                        {

                                        }
                                        fLog(arq.Name, resultadoVex);
                                        if (!resultadoVex.Contains("Sucesso") && !resultadoVex.Contains("já existe na base"))
                                        {
                                            resultadoVex = resultadoVex.Replace("'", "");
                                            string insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'Erro Envio Vex: " + resultadoVex.ToString().Substring(0, 230) + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')";
                                            string insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                            CriticarXML(insertCritica, insertErrosAplic);
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 1,'CMP','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    xmlErro = true;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    fLog(arq.Name, "aqui66");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    atualizaCritica(arq.Name);
                                                    continue;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM REENVIOVEXNOVO WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO REENVIOVEXNOVO(CHAVEACESSO, REENVIO) VALUES('" + dadosNF.ChaveAcesso + "', 0)", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                    xmlErro = true;
                                                }
                                                catch (Exception ex2)
                                                {
                                                    fLog(arq.Name, "aqui66");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    atualizaCritica(arq.Name);
                                                    continue;
                                                }
                                            }

                                        }
                                        //dataDados = new DataSet();
                                        //dataDados = buscaDados(arq.Name, "CHAVEACESSONFE = '" + dadosNF.ChaveAcesso + "'", "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                                        //if (dataDados == null || dataDados.Tables.Count <= 0)
                                        //{

                                        //}
                                        //else
                                        //{
                                        //fLog(arq.Name, "chegou 55");
                                        //}
                                    }

                                    if (xmlErro)
                                    {
                                        fLog(arq.Name, "Insere Status TESTANDOAQUI");
                                        try
                                        {

                                            insereStatus(arq.Name, dadosNF);
                                        }
                                        catch (Exception ex) { fLog(arq.Name, "catch insere status " + ex.Message); }

                                        fLog(arq.Name, "Tem erro item");
                                        if (oReader != null)
                                        {
                                            oReader.Close();
                                        }
                                        atualizaCritica(arq.Name);
                                        continue;
                                    }
                                    else
                                    {
                                        string resultado = "";
                                        fLog(arq.Name, "chegou 50");
                                        if (dadosNF.Itens[0].PedidoDeCompra == null)
                                        {
                                            dadosNF.Idmov = -1;
                                            resultado = PopulaTabelasNfe(dadosNF, e);
                                        }
                                        else if (temLote)
                                        {
                                            fLog(arq.Name, "chegou 51");
                                            dadosNF.Idmov = -1;
                                            resultado = PopulaTabelasNfe(dadosNF, e);
                                            AtualizarTmovRelac(dadosNF, resultado);
                                        }
                                        else
                                        {
                                            fLog(arq.Name, "chegou 52");
                                            try
                                            {
                                                verificaIdnatPedido(arq.Name, dadosNF, 1);
                                            }
                                            catch (Exception ex) { fLog(arq.Name, "PED PRE: " + ex.Message); }
                                            resultado = processarXMLNFE(dadosNF);
                                            try
                                            {
                                                verificaIdnatPedido(arq.Name, dadosNF, 0);
                                            }
                                            catch (Exception ex) { fLog(arq.Name, "PED POS: " + ex.Message); }
                                            if (resultado != "1")
                                            {
                                                resultado = resultado.Replace("'", "");
                                                string insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', '" + resultado.ToString().Substring(0, 230) + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')";
                                                string insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                                CriticarXML(insertCritica, insertErrosAplic);
                                                insereStatus(arq.Name, dadosNF);
                                            }
                                            else
                                            {
                                                excluir = false;
                                                //atualização pos entrada
                                                fLog(arq.Name, "chegou 53");
                                                dataDados = new DataSet();
                                                fLog(arq.Name, "chegou 54");
                                                dataDados = buscaDados(arq.Name, "CHAVEACESSONFE = '" + dadosNF.ChaveAcesso + "' AND TMOV.CODCOLIGADA = " + dadosNF.SColigada, "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC);
                                                if (dataDados.Tables.Count > 0)
                                                {
                                                    int idmovAux = 0;
                                                    dt = dataDados.Tables[0];
                                                    expression = "CODCOLIGADA = " + dadosNF.SColigada;
                                                    fLog(arq.Name, expression);
                                                    sort = "IDMOV ASC";

                                                    // Use the Select method to find all rows matching the filter.
                                                    foundRows = dt.Select(expression, sort);
                                                    for (int j = 0; j < foundRows.Length; j++)
                                                    {
                                                        dadosNF.Idmov = int.Parse(foundRows[j]["IDMOV"].ToString());
                                                        dadosNF.MovNfe = foundRows[j]["CODTMV"].ToString();
                                                    }
                                                    dataDados = buscaDadosRecord(arq.Name, dadosNF.SColigada + ";" + dadosNF.Idmov, "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC);
                                                    if (dataDados.Tables.Count > 0)
                                                    {
                                                        fLog(arq.Name, "testeexiste1");
                                                        string nseqAux = "";
                                                        string expressionItem = "";
                                                        DataRow[] foundRowsItem;
                                                        DataTable dt2 = new DataTable();
                                                        DataTable dtItem = new DataTable();
                                                        dtItem = dataDados.Tables["TITMMOVRELAC"];
                                                        fLog(arq.Name, "testeexiste2");
                                                        foreach (itensNfe itemTeste in dadosNF.Itens)
                                                        {
                                                            fLog(arq.Name, "testeexiste3");
                                                            expressionItem = "IDMOVORIGEM = '" + itemTeste.Idmov + "' AND NSEQITMMOVORIGEM = '" + itemTeste.Nseq + "'";
                                                            foundRowsItem = dtItem.Select(expressionItem);
                                                            if (foundRowsItem.Length == 0)
                                                            {
                                                                fLog(arq.Name, "NSEQORIGEM NÃO ENCONTRADO: " + itemTeste.Nseq + " DO MOVIMENTO ORIGEM: = " + itemTeste.Idmov);
                                                                excluir = true;
                                                                excluir = false;
                                                            }
                                                            else
                                                            {
                                                                fLog(arq.Name, "testeexiste4");
                                                                for (int jj = 0; jj < foundRowsItem.Length; jj++)
                                                                {
                                                                    fLog(arq.Name, "testeexiste4");
                                                                    itemTeste.Nseq = int.Parse(foundRowsItem[jj]["NSEQITMMOVDESTINO"].ToString());
                                                                }
                                                            }
                                                        }

                                                    }
                                                    if (!excluir)
                                                    {
                                                        resultado = PopulaTabelasNfe(dadosNF, e);
                                                    }
                                                }
                                            }
                                            if (excluir)
                                            {
                                                fLog(arq.Name, "Movimento deverá ser excluído");
                                                //excluirMovimento();
                                                string insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', 'UM DOS ITENS NÃO FOI INCLUIDO, POR FAVOR DELETAR A NOTA', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')";
                                                string insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                                CriticarXML(insertCritica, insertErrosAplic);
                                                insereStatus(arq.Name, dadosNF);
                                                insereStatus(arq.Name, dadosNF);
                                            }
                                            else
                                            {
                                                try
                                                {
                                                    codColResult = resultado.Substring(0, dadosNF.SColigada.Length);
                                                    pontoEVirgula = resultado.Substring(dadosNF.SColigada.Length, 1);
                                                }
                                                catch
                                                {
                                                    codColResult = "";
                                                    pontoEVirgula = "";
                                                }
                                                if ((codColResult == dadosNF.SColigada & pontoEVirgula == ";") || resultado == "1")
                                                {
                                                    string pedidosInclusos = "";
                                                    oReader.Close();
                                                    File.Delete(sProcessado + "\\" + arq.Name);
                                                    File.Copy(caminho + "\\" + arq.Name, sProcessado + "\\" + arq.Name);
                                                    File.Delete(caminho + "\\" + arq.Name);

                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        SqlCommand command = new SqlCommand("SELECT DISTINCT NUMEROMOV_ASS FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNF.ChaveAcesso + "'", connection);
                                                        try
                                                        {
                                                            connection.Open();
                                                            SqlDataReader dr;
                                                            dr = command.ExecuteReader();
                                                            if (dr.HasRows)
                                                            {
                                                                while (dr.Read())
                                                                {
                                                                    if (pedidosInclusos != "")
                                                                    {
                                                                        pedidosInclusos = pedidosInclusos + ", ";
                                                                    }
                                                                    pedidosInclusos = pedidosInclusos = dr[0].ToString();
                                                                }
                                                            }
                                                            dr.Close();

                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            fLog(arq.Name, "erro busca pedidos");
                                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')", connection);
                                                            command.ExecuteReader();
                                                            oReader.Close();
                                                        }
                                                    }
                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        SqlCommand command = new SqlCommand("DELETE FROM ZCRITICAXML WHERE NOME_XML = '" + arq.Name + "' DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNF.ChaveAcesso + "'", connection);
                                                        try
                                                        {
                                                            connection.Open();
                                                            command.ExecuteReader();

                                                            fLog("", "");
                                                            fLog("=========================================", "");
                                                            fLog("(NFE) " + arq.Name, ": PROCESSADO COM SUCESSO!");
                                                            fLog("=========================================", "");
                                                        }
                                                        catch (Exception)
                                                        {
                                                            atualizaCritica(arq.Name);
                                                            continue;
                                                        }
                                                    }
                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        SqlCommand command = new SqlCommand("DELETE FROM ZXMLPGTO WHERE NOME_XML = '" + arq.Name + "'", connection);
                                                        try
                                                        {
                                                            connection.Open();
                                                            command.ExecuteReader();
                                                        }
                                                        catch (Exception)
                                                        {
                                                            atualizaCritica(arq.Name);
                                                            continue;
                                                        }
                                                    }
                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON, PEDIDO) VALUES('" + dadosNF.ChaveAcesso + "', 5,'" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE(), '" + pedidosInclusos + "')", connection);
                                                        try
                                                        {
                                                            connection.Open();
                                                            command.ExecuteReader();
                                                            xmlErro = true;
                                                        }
                                                        catch (Exception ex2)
                                                        {
                                                            fLog(arq.Name, "aqui65");
                                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                                                            command.ExecuteReader();
                                                            oReader.Close();
                                                            atualizaCritica(arq.Name);
                                                            continue;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    resultado = resultado.Replace("'", "");
                                                    try { fLog(arq.Name, "Excluir: " + resultado.ToString()); } catch { }
                                                    try { excluirMovimento(arq.Name, resultado.ToString().Substring(0, 230)); } catch(Exception ex) { fLog(arq.Name, "erro excluir" + ex.Message); }
                                                    string insertCritica = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, CHAVEACESSO) VALUES ('" + dadosNF.SColigada + "', '" + dadosNF.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121), 'CMP','" + dadosNF.SCodCfoEmi + "', '" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi + "', '" + resultado.ToString().Substring(0, 230) + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + dadosNF.ChaveAcesso + "')";
                                                    string insertErrosAplic = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '";
                                                    CriticarXML(insertCritica, insertErrosAplic);
                                                    insereStatus(arq.Name, dadosNF);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {

                        if (oReader == null)
                        {
                            oReader.Close();
                        }
                        if (bAtivaLog)
                        {
                            fLog(arq.Name, "Arquivo Movido para Pasta Manual");
                            File.Delete(sManual + "\\" + arq.Name);
                            File.Copy(caminho + "\\" + arq.Name, sManual + "\\" + arq.Name);
                            File.Delete(caminho + "\\" + arq.Name);
                        }
                        atualizaCritica(arq.Name);
                        continue;
                    }
                }
                catch (SqlException sqlEx)
                {
                    if (bAtivaLog)
                    {
                        fLog(arq.Name, "catch linha 1461 " + sqlEx.Message);
                    }

                    if (oReader != null)
                    {
                        oReader.Close();
                    }
                    switch (sqlEx.Number)
                    {
                        case -1:
                            fLog(arq.Name, "Erro de Conexão com o Banco de Dados");
                            break;
                        case 2:
                            fLog(arq.Name, "Erro de Conexão com o Banco de Dados");
                            break;
                        case 53:
                            fLog(arq.Name, "Erro de Conexão com o Banco de Dados");
                            break;


                        default:
                            fLog(arq.Name, "Mensagem de Erro " + sqlEx.Message.Replace("'", "") + " ErrorNumber: " + sqlEx.Number);
                            break;

                    }
                    atualizaCritica(arq.Name);
                    continue;
                }
                catch (Exception ex)
                {
                    if (bAtivaLog)
                    {
                        fLog(arq.Name, "catch linha 1481 " + ex.Message);
                    }

                    if (oReader != null)
                    {
                        oReader.Close();
                    }
                    if (!ex.Message.Contains("Could not find file"))
                    {
                        if (!ex.Message.Contains("Timeout expired")) { }
                    }
                    atualizaCritica(arq.Name);
                    continue;
                }
                fLog(arq.Name, "ultima linha");
                atualizaCritica(arq.Name);
            }
            tempo.Enabled = true;
        }
        private void atualizaCritica(string nomeArquivo)
        {
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXML(NOLOCK) WHERE FLAG_STATUS IS NULL AND NOME_XML = '" + nomeArquivo + "'", connection);
                connection.Open();
                SqlDataReader dr;
                dr = command.ExecuteReader();
                if (dr.HasRows)
                {
                    using (SqlConnection connection2 = dbt.GetConnection())
                    {
                        SqlCommand cmd2 = new SqlCommand("UPDATE ZCRITICAXML SET FLAG_STATUS = 'E' WHERE FLAG_STATUS IS NULL AND NOME_XML = '" + nomeArquivo + "'", connection2);
                        try
                        {
                            connection2.Open();
                            SqlDataReader dr2;
                            dr2 = cmd2.ExecuteReader();
                            this.oReader.Close();
                        }
                        catch { }
                    }

                }
            }
        }
        private bool buscaEnviaVex(string chaveAcesso, string name)
        {
            bool enviar = false;
            bool existe = false;

            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT REENVIO FROM REENVIOVEX(NOLOCK) WHERE CHAVEACESSO = '" + chaveAcesso + "'", connection);
                try
                {
                    connection.Open();
                    SqlDataReader dr;
                    dr = command.ExecuteReader();
                    if (dr.HasRows)
                    {
                        existe = true;
                        while (dr.Read())
                        {
                            if (dr[0].ToString() == "1")
                            {
                                enviar = true;
                            }
                            else { enviar = false; }
                        }
                    }
                    else
                    {
                        existe = false;
                    }
                    dr.Close();

                }
                catch (Exception ex)
                {
                    fLog(name, "erroenviavexbuscar " + ex.Message);
                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                    command.ExecuteReader();
                    this.oReader.Close();
                    enviar = false;
                    return enviar;
                }
            }

            if (!existe)
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    fLog(name, "Não existe, inserir reenviovex");
                    fLog(name, "INSERT INTO REENVIOVEX(REENVIO, CHAVEACESSO) VALUES(1,'" + chaveAcesso + "')");
                    enviar = true;
                    SqlCommand command = new SqlCommand("INSERT INTO REENVIOVEX(REENVIO, CHAVEACESSO) VALUES(1,'" + chaveAcesso + "')", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                        fLog(name, "aqui67");
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                        enviar = false;
                        return enviar;
                    }

                }

            }
            return enviar;
        }


        private bool buscaEnviaVexNovo(string chaveAcesso, string name)
        {
            bool enviar = false;
            bool existe = false;

            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT REENVIO FROM REENVIOVEXNOVO(NOLOCK) WHERE CHAVEACESSO = '" + chaveAcesso + "'", connection);
                try
                {
                    connection.Open();
                    SqlDataReader dr;
                    dr = command.ExecuteReader();
                    if (dr.HasRows)
                    {
                        existe = true;
                        while (dr.Read())
                        {
                            if (dr[0].ToString() == "1")
                            {
                                enviar = true;
                            }
                            else { enviar = false; }
                        }
                    }
                    else
                    {
                        existe = false;
                    }
                    dr.Close();

                }
                catch (Exception ex)
                {
                    fLog(name, "erroenviavexbuscarNOVO " + ex.Message);
                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                    command.ExecuteReader();
                    this.oReader.Close();
                    enviar = false;
                    return enviar;
                }
            }

            if (!existe)
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    fLog(name, "Não existe, inserir reenviovex");
                    fLog(name, "INSERT INTO REENVIOVEXNOVO(REENVIO, CHAVEACESSO) VALUES(1,'" + chaveAcesso + "')");
                    enviar = true;
                    SqlCommand command = new SqlCommand("INSERT INTO REENVIOVEXNOVO(REENVIO, CHAVEACESSO) VALUES(1,'" + chaveAcesso + "')", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                        fLog(name, "aqui67");
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                        enviar = false;
                        return enviar;
                    }

                }

            }
            return enviar;
        }

        private bool buscaIgnoraPreco(string chaveAcesso, string name)
        {
            bool ignora = false;
            bool existe = false;

            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT IGNORA FROM IGNORA_PEDIDO(NOLOCK) WHERE CHAVEACESSO = '" + chaveAcesso + "'", connection);
                try
                {
                    connection.Open();
                    SqlDataReader dr;
                    dr = command.ExecuteReader();
                    if (dr.HasRows)
                    {
                        existe = true;
                        while (dr.Read())
                        {
                            if (dr[0].ToString() == "1")
                            {
                                ignora = true;
                            }
                            else { ignora = false; }
                        }
                    }
                    else
                    {
                        existe = false;
                    }
                    dr.Close();

                }
                catch (Exception ex)
                {
                    fLog(name, "errobuscaignora " + ex.Message);
                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                    command.ExecuteReader();
                    this.oReader.Close();
                    ignora = false;
                    return ignora;
                }
            }

            if (!existe)
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    fLog(name, "Não existe, inserir ignora_pedido");
                    fLog(name, "INSERT INTO IGNORA_PEDIDO(IGNORA, CHAVEACESSO) VALUES(0,'" + chaveAcesso + "')");
                    ignora = false;
                    SqlCommand command = new SqlCommand("INSERT INTO IGNORA_PEDIDO(IGNORA, CHAVEACESSO) VALUES(1,'" + chaveAcesso + "')", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                        fLog(name, "aqui67");
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                        ignora = false;
                        return ignora;
                    }

                }

            }
            return ignora;
        }

        private void verificaIdnatPedido(string name, dadosNfe dadosNF, int insert)
        {
            string codcfoNota = dadosNF.SCodCfoEmi;
            string idnat1 = "";
            string idnat2 = "";
            int idmovAtual = 0;
            DataTable dtPedidos = new DataTable();
            dtPedidos.Columns.Add("IDMOV", typeof(int));
            dtPedidos.Columns.Add("NSEQ", typeof(int));
            dtPedidos.Columns.Add("IDNAT", typeof(int));
            dtPedidos.Columns.Add("IDNATPEDIDO", typeof(int));
            foreach (itensNfe item in dadosNF.Itens)
            {
                if (item.Idnat != item.IdnatPedido)
                {
                    dtPedidos.Rows.Add(item.Idmov, item.Nseq, int.Parse(item.Idnat), int.Parse(item.IdnatPedido));
                }
                if (insert == 1)
                {
                    if (idmovAtual != item.Idmov)
                    {
                        if (item.CodcfoAssossiado != codcfoNota)
                        {
                            DataSet retorno = new DataSet();
                            retorno = buscaDadosConsulta(name, "CODCFO=" + codcfoNota + ";IDMOV=" + item.Idmov.ToString(), sCodUsuarioTBC, sSenhaTBC, "T", "ATTPEDCFO");
                        }
                        if (item.EstadoPedido != dadosNF.EstadoNota)
                        {
                            if (dadosNF.EstadoNota == "SP")
                            {
                                idnat1 = "14";
                                idnat2 = "870";
                            }
                            else
                            {
                                idnat1 = "19";
                                idnat2 = "967";
                            }
                            DataSet retorno = new DataSet();
                            retorno = buscaDadosConsulta(name, "IDNAT=" + idnat1 + ";IDNAT2=" + idnat2 + ";IDMOV=" + item.Idmov.ToString(), sCodUsuarioTBC, sSenhaTBC, "T", "ATTPEDIDNAT");
                        }
                    }
                    
                }
                else
                {
                    if (idmovAtual != item.Idmov)
                    {
                        if (item.CodcfoAssossiado != codcfoNota)
                        {
                            DataSet retorno = new DataSet();
                            retorno = buscaDadosConsulta(name, "CODCFO=" + item.CodcfoAssossiado + ";IDMOV=" + item.Idmov.ToString(), sCodUsuarioTBC, sSenhaTBC, "T", "ATTPEDCFO");
                        }
                        if (item.EstadoPedido != dadosNF.EstadoNota)
                        {
                            if (item.EstadoPedido == "SP")
                            {
                                idnat1 = "14";
                                idnat2 = "870";
                            }
                            else
                            {
                                idnat1 = "19";
                                idnat2 = "967";
                            }
                            DataSet retorno = new DataSet();
                            retorno = buscaDadosConsulta(name, "IDNAT=" + idnat1 + ";IDNAT2=" + idnat2 + ";IDMOV=" + item.Idmov.ToString(), sCodUsuarioTBC, sSenhaTBC, "T", "ATTPEDIDNAT");
                        }
                    }
                }
                idmovAtual = item.Idmov;
            }
            if (dtPedidos.Rows.Count > 0)
            {
                foreach (DataRow row in dtPedidos.Rows)
                {
                    DataSet retorno = new DataSet();
                    if (insert == 1)
                    {
                        retorno = buscaDadosConsulta(name, "IDNAT=" + row["IDNAT"].ToString() + ";IDMOV=" + row["IDMOV"].ToString() + ";NSEQ=" + row["NSEQ"].ToString(), sCodUsuarioTBC, sSenhaTBC, "T", "ATTPED");
                    }
                    else
                    {
                        retorno = buscaDadosConsulta(name, "IDNAT=" + row["IDNATPEDIDO"].ToString() + ";IDMOV=" + row["IDMOV"].ToString() + ";NSEQ=" + row["NSEQ"].ToString(), sCodUsuarioTBC, sSenhaTBC, "T", "ATTPED");
                    }
                    
                }
                
            }
        }

        private void insereStatus(string name, dadosNfe dadosNF)
        {
            
            bool temCritica = false;
            string setor = "";
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS),CASE WHEN CRITICA LIKE 'ITEM PENDENTE DE AP%' THEN 'APROV' ELSE SETOR END SETOR FROM ZCRITICAXML(NOLOCK) WHERE NOME_XML = '" + name + "'", connection);
                try
                {
            
                    connection.Open();
                    SqlDataReader dr;
                    dr = command.ExecuteReader();
                    if (dr.HasRows)
                    {
            
                        temCritica = true;
                        while (dr.Read())
                        {
                            if (setor != "")
                            {
                                setor = setor + ", ";
                            }
                            setor = setor + dr["SETOR"].ToString();
                        }
                    }


                }
                catch (Exception ex)
                {
                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                    command.ExecuteReader();
                }
            }
            if (temRetorno == false && temCritica)
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    
                    SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 2,'" + setor + "','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                    
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                    }
                }
            }
            else if(temRetorno == true && temCritica)
            {
                
                using (SqlConnection connection = dbt.GetConnection())
                {
                    SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 4,'" + setor + "','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                    }
                }
            }
            else if(temRetorno == false && temCritica == false)
            {
                
                using (SqlConnection connection = dbt.GetConnection())
                {
                    SqlCommand command = new SqlCommand("DELETE FROM STATUS_NOTA WHERE CHAVEACESSO LIKE '%" + dadosNF.ChaveAcesso + "%' INSERT INTO STATUS_NOTA(CHAVEACESSO, STATUSNOTA,SETOR, NUMERONF, CNPJ, NOMEEMI, DATAEMISSAO, VALORTOTAL, RECCREATEDON) VALUES('" + dadosNF.ChaveAcesso + "', 3,'" + setor + "','" + dadosNF.SNumeromov + "','" + dadosNF.CnpjEmi + "','" + dadosNF.NomeEmi.Replace("'", "") + "', convert(datetime, '" + dadosNF.SDataEmissao + "', 121)," + dadosNF.TotalNf.Replace(".", "").Replace(",", ".") + ", GETDATE())", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                    }
                }
            }
        }

        private bool aguardaVexMetodo(string chaveAcesso, string arqName)
        {
            bool aguarda = false;
            using (SqlConnection connection = dbt.GetConnection())
            {

                SqlCommand command = new SqlCommand("SELECT STATUSNOTA FROM STATUS_NOTA(NOLOCK) WHERE CHAVEACESSO = '" + chaveAcesso + "'", connection);
                try
                {
                    connection.Open();
                    SqlDataReader dr;
                    dr = command.ExecuteReader();
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            string statusNota = "";
                            statusNota = dr["STATUSNOTA"].ToString();
                            fLog(arqName, "Aguarda Vex" + statusNota.ToString());
                            if (statusNota != "3")
                            {
                                aguarda = false;
                            }
                            else
                            {
                                DataSet dataDados = new DataSet();
                                dataDados = buscaDados(arqName, "CHAVEACESSONFE = '" + chaveAcesso + "'", "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {

                                    try
                                    {
                                        string quantidadePedido = "";
                                        DataTable dt = dataDados.Tables[0];
                                        string expression = "CHAVEACESSONFE = '" + chaveAcesso + "'";
                                        string sort = "CHAVEACESSONFE ASC";
                                        // Use the Select method to find all rows matching the filter.
                                        DataRow[] foundRows = dt.Select(expression, sort);
                                        for (int j = 0; j < foundRows.Length; j++)
                                        {
                                            try
                                            {
                                                quantidadePedido = foundRows[j]["QTDEPRD"].ToString();
                                            }
                                            catch
                                            {
                                                aguardaVex = true;
                                                break;
                                            }
                                            if (quantidadePedido == "" || string.IsNullOrEmpty(quantidadePedido))
                                            {
                                                aguardaVex = true;
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        aguardaVex = false;
                                    }
                                }
                                else
                                {
                                    fLog(arqName, "Aguarda Vex Nenhum Registro Metadados");
                                    //incluir no zmd
                                    aguarda = false;
                                }
                            }
                        }
                    }
                    else
                    {
                        fLog(arqName, "Aguarda Vex Nenhuma Linha de Status");
                        aguarda = false;
                    }
                    dr.Close();

                }
                catch (Exception ex)
                {
                    fLog(arqName, ex.Message);
                    aguarda = false;
                    // this.oReader.Close();
                }
            }
            return aguarda;
        }

        private bool temRetornoVex(string chaveAcesso, string arqName)
        {
            bool temRetorno = true;
            try
            {
                DataSet dataDados = new DataSet();
                dataDados = buscaDados(arqName, "CHAVEACESSONFE = '" + chaveAcesso + "'", "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                if (dataDados.Tables.Count > 0)
                {

                    try
                    {
                        fLog(arqName, "encontrou zmd no temretorno");
                        string quantidadePedido = "";
                        DataTable dt = dataDados.Tables[0];
                        string expression = "CHAVEACESSONFE = '" + chaveAcesso + "'";
                        string sort = "CHAVEACESSONFE ASC";
                        // Use the Select method to find all rows matching the filter.
                        DataRow[] foundRows = dt.Select(expression, sort);
                        for (int j = 0; j < foundRows.Length; j++)
                        {
                            try
                            {
                                quantidadePedido = foundRows[j]["QTDEPRD"].ToString();
                                fLog(arqName, "ACHOU: " + quantidadePedido);
                            }
                            catch (Exception ex)
                            {
                                fLog(arqName, "catch1 " + ex.Message);
                                temRetorno = false;
                                break;
                            }
                            if (quantidadePedido == "" || string.IsNullOrEmpty(quantidadePedido))
                            {
                                temRetorno = false;
                                fLog(arqName, "null ou vazio");
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        temRetorno = false;
                    }
                }
                else
                {
                    fLog(arqName, "Aguarda Vex Nenhum Registro Metadados");
                    //incluir no zmd
                    temRetorno = false;
                }
            }
            catch (Exception ex)
            {
                fLog(arqName, "erro geral " + ex.Message);
                temRetorno = false;
            }
            fLog(arqName, "retorna: " + temRetorno.ToString());
            return temRetorno;
        }
        /*
        private bool quantidadeVex(string caminho, System.IO.FileInfo arq, XmlDocument xmlDoc, dadosNfe dadosNf)
        {
            bool bVal = false;
            try
            {
                int i = 1;
                string idprd = "";
                string unVex = "";
                string unRm = "";
                int quantidade;
                foreach (itensNfe item in dadosNf.Itens)
                {
                    using (SqlConnection connection = dbt.GetConnection())
                    {
                        fLog(arq.Name, "SELECT ZXMLTRANSFERENCIA.IDPRD, SUM(ISNULL(QTDE_MAISBRASIL,0) + ISNULL(QTDE_ALIMENTAR,0) + ISNULL(QTDE_VAIVAREJO,0)) QUANTIDADE, ZUNIDADE.UNRM, ZUNIDADE.UNVEX FROM ZUNIDADE(NOLOCK) JOIN ZXMLTRANSFERENCIA(NOLOCK) ON ZUNIDADE.IDPRD = ZXMLTRANSFERENCIA.IDPRD WHERE CODCFO = '" + dadosNf.SCodCfoEmi + "' AND NOME_XML = '" + arq.Name + "' AND UNCFO = '" + item.UnidadeNota + "' AND ZXMLTRANSFERENCIA.IDPRD = " + item.Idprd + " GROUP BY ZXMLTRANSFERENCIA.IDPRD, ZUNIDADE.UNRM, ZUNIDADE.UNVEX");
                        SqlCommand command = new SqlCommand("SELECT ZXMLTRANSFERENCIA.IDPRD, SUM(ISNULL(QTDE_MAISBRASIL,0) + ISNULL(QTDE_ALIMENTAR,0) + ISNULL(QTDE_VAIVAREJO,0)) QUANTIDADE, ZUNIDADE.UNRM, ZUNIDADE.UNVEX FROM ZUNIDADE(NOLOCK) JOIN ZXMLTRANSFERENCIA(NOLOCK) ON ZUNIDADE.IDPRD = ZXMLTRANSFERENCIA.IDPRD WHERE CODCFO = '" + dadosNf.SCodCfoEmi + "' AND NOME_XML = '" + arq.Name + "' AND UNCFO = '" + item.UnidadeNota + "' AND ZXMLTRANSFERENCIA.IDPRD = " + item.Idprd + " GROUP BY ZXMLTRANSFERENCIA.IDPRD, ZUNIDADE.UNRM, ZUNIDADE.UNVEX", connection);
                        try
                        {

                            connection.Open();
                            SqlDataReader dr;
                            dr = command.ExecuteReader();
                            if (dr.HasRows)
                            {
                                while (dr.Read())
                                {
                                    try
                                    {
                                        idprd = dr[0].ToString();
                                        unRm = dr[2].ToString();
                                        unVex = dr[3].ToString();
                                    }
                                    catch { idprd = ""; }
                                    try
                                    {
                                        quantidade = int.Parse(dr[1].ToString());
                                    }
                                    catch { quantidade = 0; }
                                    DataSet dataDados = new DataSet();
                                    dataDados = buscaDados(arq.Name, "CHAVEACESSONFE = '" + dadosNf.ChaveAcesso + "' AND CODIGOPRD = '" + item.CodigoPrd + "'", "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        try
                                        {
                                            try
                                            {
                                                double fator = 1;
                                                double quantidadePedido = 0;
                                                DataTable dt = dataDados.Tables[0];
                                                string expression = "CHAVEACESSONFE = '" + dadosNf.ChaveAcesso + "'";
                                                string sort = "CHAVEACESSONFE ASC";
                                                // Use the Select method to find all rows matching the filter.
                                                DataRow[] foundRows = dt.Select(expression, sort);
                                                for (int j = 0; j < foundRows.Length; j++)
                                                {
                                                    try
                                                    {
                                                        quantidadePedido = double.Parse(foundRows[j]["QTDEPRD"].ToString().Replace(".", ","));

                                                    }
                                                    catch { quantidadePedido = 0; }
                                                    try
                                                    {
                                                        fator = double.Parse(foundRows[j]["FATORCONVERSAO"].ToString().Replace(".", ","));
                                                        if (fator <= 0)
                                                        {
                                                            fator = 1;
                                                        }
                                                    }
                                                    catch { fator = 1; }
                                                    if (quantidadePedido > 0)
                                                    {
                                                        string fatorRm = "";
                                                        string fatorVex = "";
                                                        dataDados = buscaDadosRecord(arq.Name, item.CodUnd, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                                        if (dataDados.Tables.Count > 0)
                                                        {
                                                            dt = dataDados.Tables["TUND"];
                                                            expression = "CODUND = '" + item.CodUnd + "'";
                                                            fLog(arq.Name, expression);
                                                            sort = "CODUND DESC";

                                                            //Use the Select method to find all rows matching the filter.F
                                                            foundRows = dt.Select(expression, sort);
                                                            for (int l = 0; l < foundRows.Length; l++)
                                                            {
                                                                item.FatorNf = foundRows[l]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                                            }
                                                        }
                                                        dataDados = buscaDadosRecord(arq.Name, unVex, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                                        if (dataDados.Tables.Count > 0)
                                                        {
                                                            dt = dataDados.Tables["TUND"];
                                                            expression = "CODUND = '" + unVex + "'";
                                                            fLog(arq.Name, expression);
                                                            sort = "CODUND DESC";

                                                            //Use the Select method to find all rows matching the filter.F
                                                            foundRows = dt.Select(expression, sort);
                                                            for (int y = 0; y < foundRows.Length; y++)
                                                            {
                                                                fatorVex = foundRows[y]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                                                double auxQuant = double.Parse(item.FatorNf);
                                                                double auxQuantVex = double.Parse(fatorVex);
                                                                try { quantidadePedido = quantidadePedido * auxQuantVex / auxQuant; } catch { quantidadePedido = 0; }
                                                            }
                                                        }
                                                        if (quantidadePedido != quantidade && quantidadePedido > 0)
                                                        {
                                                            bVal = true;
                                                            using (SqlConnection connection2 = dbt.GetConnection())
                                                            {
                                                                //verificar;
                                                                SqlCommand command2 = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'QUANTIDADE PASSADA PELA VEX: " + quantidadePedido.ToString() + " É DIFERENTE DA QUANTIDADE TOTAL DE TRANSFERÊNCIA: " + quantidade.ToString() + " DO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')", connection2);
                                                                try
                                                                {
                                                                    connection2.Open();
                                                                    command2.ExecuteReader();
                                                                }
                                                                catch (Exception exItens)
                                                                {
                                                                    fLog(arq.Name, "aqui1");
                                                                    command2 = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection2);
                                                                    command2.ExecuteReader();
                                                                    oReader.Close();
                                                                    throw new Exception(exItens.Message);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        fLog(arq.Name, "quantidadepedido = 0");
                                                        bVal = true;
                                                        aguardaVex = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                fLog(arq.Name, ex.Message);
                                                bVal = true;
                                                aguardaVex = true;
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            fLog(arq.Name, ex.Message);
                                            bVal = true;
                                        }
                                    }
                                    else
                                    {
                                        using (SqlConnection connection2 = dbt.GetConnection())
                                        {
                                            fLog(arq.Name, "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'O IDPRD: " + item.Idprd + " NÃO ENCONTRADO NA TABELA DE RETORNO VEX,'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')");
                                            //verificar;
                                            SqlCommand command2 = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'O IDPRD: " + item.Idprd + " NÃO ENCONTRADO NA TABELA DE RETORNO VEX','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')", connection2);
                                            try
                                            {
                                                connection2.Open();
                                                command2.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui1");
                                                command2 = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection2);
                                                command2.ExecuteReader();
                                                oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }
                                        }
                                        bVal = true;
                                    }
                                }
                            }
                            else
                            {
                                bVal = true;
                            }
                            dr.Close();

                        }
                        catch (Exception ex)
                        {
                            fLog(arq.Name, "aqui2");
                            command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                            command.ExecuteReader();
                            bVal = true;
                        }
                    }
                    i++;
                }
            }
            catch (Exception ex) { bVal = true; fLog(arq.Name, "aqui2"); }
            return bVal;
        }
        private bool quantidadeVexOld2(string caminho, System.IO.FileInfo arq, XmlDocument xmlDoc, dadosNfe dadosNf)
        {
            bool bVal = false;
            try
            {
                int i = 1;
                string idprd = "";
                string unVex = "";
                string unRm = "";
                int quantidade;
                foreach (itensNfe item in dadosNf.Itens)
                {
                    using (SqlConnection connection = dbt.GetConnection())
                    {
                        fLog(arq.Name, "SELECT ZXMLTRANSFERENCIA.IDPRD, SUM(ISNULL(QTDE_MAISBRASIL,0) + ISNULL(QTDE_ALIMENTAR,0) + ISNULL(QTDE_VAIVAREJO,0)) QUANTIDADE, ZUNIDADE.UNRM, ZUNIDADE.UNVEX FROM ZUNIDADE(NOLOCK) JOIN ZXMLTRANSFERENCIA(NOLOCK) ON ZUNIDADE.IDPRD = ZXMLTRANSFERENCIA.IDPRD WHERE CODCFO = '" + dadosNf.SCodCfoEmi + "' AND NOME_XML = '" + arq.Name + "' AND UNCFO = '" + item.UnidadeNota + "' AND ZXMLTRANSFERENCIA.IDPRD = " + item.Idprd + " GROUP BY ZXMLTRANSFERENCIA.IDPRD, ZUNIDADE.UNRM, ZUNIDADE.UNVEX");
                        SqlCommand command = new SqlCommand("SELECT ZXMLTRANSFERENCIA.IDPRD, SUM(ISNULL(QTDE_MAISBRASIL,0) + ISNULL(QTDE_ALIMENTAR,0) + ISNULL(QTDE_VAIVAREJO,0)) QUANTIDADE, ZUNIDADE.UNRM, ZUNIDADE.UNVEX FROM ZUNIDADE(NOLOCK) JOIN ZXMLTRANSFERENCIA(NOLOCK) ON ZUNIDADE.IDPRD = ZXMLTRANSFERENCIA.IDPRD WHERE CODCFO = '" + dadosNf.SCodCfoEmi + "' AND NOME_XML = '" + arq.Name + "' AND UNCFO = '" + item.UnidadeNota + "' AND ZXMLTRANSFERENCIA.IDPRD = " + item.Idprd + " GROUP BY ZXMLTRANSFERENCIA.IDPRD, ZUNIDADE.UNRM, ZUNIDADE.UNVEX", connection);
                        try
                        {

                            connection.Open();
                            SqlDataReader dr;
                            dr = command.ExecuteReader();
                            if (dr.HasRows)
                            {
                                while (dr.Read())
                                {
                                    try
                                    {
                                        idprd = dr[0].ToString();
                                        unRm = dr[2].ToString();
                                        unVex = dr[3].ToString();
                                    }
                                    catch { idprd = ""; }
                                    try
                                    {
                                        quantidade = int.Parse(dr[1].ToString());
                                    }
                                    catch { quantidade = 0; }
                                    DataSet dataDados = new DataSet();
                                    dataDados = buscaDados(arq.Name, "CHAVEACESSONFE = '" + dadosNf.ChaveAcesso + "' AND IDPRD = " + idprd, "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        try
                                        {
                                            try
                                            {
                                                double fator = 1;
                                                double quantidadePedido = 0;
                                                DataTable dt = dataDados.Tables[0];
                                                string expression = "CHAVEACESSONFE = '" + dadosNf.ChaveAcesso + "'";
                                                string sort = "CHAVEACESSONFE ASC";
                                                // Use the Select method to find all rows matching the filter.
                                                DataRow[] foundRows = dt.Select(expression, sort);
                                                for (int j = 0; j < foundRows.Length; j++)
                                                {
                                                    try
                                                    {
                                                        quantidadePedido = double.Parse(foundRows[j]["QTDEPRD"].ToString().Replace(".", ","));

                                                    }
                                                    catch { quantidadePedido = 0; }
                                                    try
                                                    {
                                                        fator = double.Parse(foundRows[j]["FATORCONVERSAO"].ToString().Replace(".", ","));
                                                        if (fator <= 0)
                                                        {
                                                            fator = 1;
                                                        }
                                                    }
                                                    catch { fator = 1; }
                                                    if (quantidadePedido > 0)
                                                    {
                                                        string fatorRm = "";
                                                        string fatorVex = "";
                                                        dataDados = buscaDadosRecord(arq.Name, item.CodUnd, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                                        if (dataDados.Tables.Count > 0)
                                                        {
                                                            dt = dataDados.Tables["TUND"];
                                                            expression = "CODUND = '" + item.CodUnd + "'";
                                                            fLog(arq.Name, expression);
                                                            sort = "CODUND DESC";

                                                            //Use the Select method to find all rows matching the filter.F
                                                            foundRows = dt.Select(expression, sort);
                                                            for (int l = 0; l < foundRows.Length; l++)
                                                            {
                                                                item.FatorNf = foundRows[l]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                                            }
                                                        }
                                                        dataDados = buscaDadosRecord(arq.Name, unVex, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                                        if (dataDados.Tables.Count > 0)
                                                        {
                                                            dt = dataDados.Tables["TUND"];
                                                            expression = "CODUND = '" + unVex + "'";
                                                            fLog(arq.Name, expression);
                                                            sort = "CODUND DESC";

                                                            //Use the Select method to find all rows matching the filter.F
                                                            foundRows = dt.Select(expression, sort);
                                                            for (int y = 0; y < foundRows.Length; y++)
                                                            {
                                                                fatorVex = foundRows[y]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                                                double auxQuant = double.Parse(item.FatorNf);
                                                                double auxQuantVex = double.Parse(fatorVex);
                                                                try { quantidadePedido = quantidadePedido * auxQuantVex / auxQuant; } catch { quantidadePedido = 0; }
                                                            }
                                                        }
                                                        if (quantidadePedido != quantidade && quantidadePedido > 0)
                                                        {
                                                            bVal = true;
                                                            using (SqlConnection connection2 = dbt.GetConnection())
                                                            {
                                                                //verificar;
                                                                SqlCommand command2 = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'QUANTIDADE PASSADA PELA VEX: " + quantidadePedido.ToString() + " É DIFERENTE DA QUANTIDADE TOTAL DE TRANSFERÊNCIA: " + quantidade.ToString() + " DO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')", connection2);
                                                                try
                                                                {
                                                                    connection2.Open();
                                                                    command2.ExecuteReader();
                                                                }
                                                                catch (Exception exItens)
                                                                {
                                                                    fLog(arq.Name, "aqui1");
                                                                    command2 = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection2);
                                                                    command2.ExecuteReader();
                                                                    oReader.Close();
                                                                    throw new Exception(exItens.Message);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        fLog(arq.Name, "quantidadepedido = 0");
                                                        bVal = true;
                                                        aguardaVex = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                fLog(arq.Name, ex.Message);
                                                bVal = true;
                                                aguardaVex = true;
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            fLog(arq.Name, ex.Message);
                                            bVal = true;
                                        }
                                    }
                                    else
                                    {
                                        using (SqlConnection connection2 = dbt.GetConnection())
                                        {
                                            fLog(arq.Name, "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'O IDPRD: " + item.Idprd + " NÃO ENCONTRADO NA TABELA DE RETORNO VEX,'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')");
                                            //verificar;
                                            SqlCommand command2 = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'O IDPRD: " + item.Idprd + " NÃO ENCONTRADO NA TABELA DE RETORNO VEX','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')", connection2);
                                            try
                                            {
                                                connection2.Open();
                                                command2.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui1");
                                                command2 = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection2);
                                                command2.ExecuteReader();
                                                oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }
                                        }
                                        bVal = true;
                                    }
                                }
                            }
                            else
                            {
                                bVal = true;
                            }
                            dr.Close();

                        }
                        catch (Exception ex)
                        {
                            fLog(arq.Name, "aqui2");
                            command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                            command.ExecuteReader();
                            bVal = true;
                        }
                    }
                    i++;
                }
            }
            catch (Exception ex) { bVal = true; fLog(arq.Name, "aqui2"); }
            return bVal;
        }

        private bool quantidadeVexOld(string caminho, System.IO.FileInfo arq, XmlDocument xmlDoc, dadosNfe dadosNf)
        {
            bool bVal = false;
            try
            {
                int i = 1;
                string idprd = "";
                string unVex = "";
                string unRm = "";
                int quantidade;
                foreach (itensNfe item in dadosNf.Itens)
                {
                    using (SqlConnection connection = dbt.GetConnection())
                    {
                        fLog(arq.Name, "SELECT ZXMLTRANSFERENCIA.IDPRD, SUM(ISNULL(QTDE_MAISBRASIL,0) + ISNULL(QTDE_ALIMENTAR,0) + ISNULL(QTDE_VAIVAREJO,0)) QUANTIDADE, ZUNIDADE.UNRM, ZUNIDADE.UNVEX FROM ZUNIDADE(NOLOCK) JOIN ZXMLTRANSFERENCIA(NOLOCK) ON ZUNIDADE.IDPRD = ZXMLTRANSFERENCIA.IDPRD WHERE CODCFO = '" + dadosNf.SCodCfoEmi + "' AND NOME_XML = '" + arq.Name + "' AND UNCFO = '" + item.UnidadeNota + "' AND ZXMLTRANSFERENCIA.IDPRD = " + item.Idprd + " GROUP BY ZXMLTRANSFERENCIA.IDPRD, ZUNIDADE.UNRM, ZUNIDADE.UNVEX"); 
                        SqlCommand command = new SqlCommand("SELECT ZXMLTRANSFERENCIA.IDPRD, SUM(ISNULL(QTDE_MAISBRASIL,0) + ISNULL(QTDE_ALIMENTAR,0) + ISNULL(QTDE_VAIVAREJO,0)) QUANTIDADE, ZUNIDADE.UNRM, ZUNIDADE.UNVEX FROM ZUNIDADE(NOLOCK) JOIN ZXMLTRANSFERENCIA(NOLOCK) ON ZUNIDADE.IDPRD = ZXMLTRANSFERENCIA.IDPRD WHERE CODCFO = '" + dadosNf.SCodCfoEmi + "' AND NOME_XML = '" + arq.Name + "' AND UNCFO = '" + item.UnidadeNota + "' AND ZXMLTRANSFERENCIA.IDPRD = " + item.Idprd + " GROUP BY ZXMLTRANSFERENCIA.IDPRD, ZUNIDADE.UNRM, ZUNIDADE.UNVEX", connection);
                        try
                        {
                            
                            connection.Open();
                            SqlDataReader dr;
                            dr = command.ExecuteReader();
                            if (dr.HasRows)
                            {
                                while (dr.Read())
                                {
                                    try
                                    {
                                        idprd = dr[0].ToString();
                                        unRm = dr[2].ToString();
                                        unVex = dr[3].ToString();
                                    }
                                    catch { idprd = ""; }
                                    try
                                    {
                                        quantidade = int.Parse(dr[1].ToString());
                                    }
                                    catch { quantidade = 0; }
                                    DataSet dataDados = new DataSet();
                                    dataDados = buscaDados(arq.Name, "CHAVEACESSONFE = '" + dadosNf.ChaveAcesso + "' AND IDPRD = " + idprd, "RMSPRJ4872704Server", sCodUsuarioTBC, sSenhaTBC);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        try
                                        {
                                            try
                                            {
                                                double fator = 1;
                                                double quantidadePedido = 0;
                                                DataTable dt = dataDados.Tables[0];
                                                string expression = "CHAVEACESSONFE = '" + dadosNf.ChaveAcesso + "'";
                                                string sort = "CHAVEACESSONFE ASC";
                                                // Use the Select method to find all rows matching the filter.
                                                DataRow[] foundRows = dt.Select(expression, sort);
                                                for (int j = 0; j < foundRows.Length; j++)
                                                {
                                                    try
                                                    {
                                                        quantidadePedido = double.Parse(foundRows[j]["QTDEPRD"].ToString().Replace(".", ","));

                                                    }
                                                    catch { quantidadePedido = 0; }
                                                    try
                                                    {
                                                        fator = double.Parse(foundRows[j]["FATORCONVERSAO"].ToString().Replace(".", ","));
                                                        if (fator <= 0)
                                                        {
                                                            fator = 1;
                                                        }
                                                    }
                                                    catch { fator = 1; }
                                                    if (quantidadePedido > 0)
                                                    {
                                                        string fatorRm = "";
                                                        string fatorVex = "";
                                                        dataDados = buscaDadosRecord(arq.Name, item.CodUnd, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                                        if (dataDados.Tables.Count > 0)
                                                        {
                                                            dt = dataDados.Tables["TUND"];
                                                            expression = "CODUND = '" + item.CodUnd + "'";
                                                            fLog(arq.Name, expression);
                                                            sort = "CODUND DESC";

                                                            //Use the Select method to find all rows matching the filter.F
                                                            foundRows = dt.Select(expression, sort);
                                                            for (int l = 0; l < foundRows.Length; l++)
                                                            {
                                                                item.FatorNf = foundRows[l]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                                            }
                                                        }
                                                        dataDados = buscaDadosRecord(arq.Name, unVex, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                                        if (dataDados.Tables.Count > 0)
                                                        {
                                                            dt = dataDados.Tables["TUND"];
                                                            expression = "CODUND = '" + unVex + "'";
                                                            fLog(arq.Name, expression);
                                                            sort = "CODUND DESC";

                                                            //Use the Select method to find all rows matching the filter.F
                                                            foundRows = dt.Select(expression, sort);
                                                            for (int y = 0; y < foundRows.Length; y++)
                                                            {
                                                                fatorVex = foundRows[y]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                                                double auxQuant = double.Parse(item.FatorNf);
                                                                double auxQuantVex = double.Parse(fatorVex);
                                                                try { quantidadePedido = quantidadePedido * auxQuantVex / auxQuant; } catch { quantidadePedido = 0; }
                                                            }
                                                        }
                                                        if (quantidadePedido != quantidade && quantidadePedido > 0)
                                                        {
                                                            bVal = true;
                                                            using (SqlConnection connection2 = dbt.GetConnection())
                                                            {
                                                                //verificar;
                                                                SqlCommand command2 = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'QUANTIDADE PASSADA PELA VEX: " + quantidadePedido.ToString() + " É DIFERENTE DA QUANTIDADE TOTAL DE TRANSFERÊNCIA: " + quantidade.ToString() + " DO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')", connection2);
                                                                try
                                                                {
                                                                    connection2.Open();
                                                                    command2.ExecuteReader();
                                                                }
                                                                catch (Exception exItens)
                                                                {
                                                                    fLog(arq.Name, "aqui1");
                                                                    command2 = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection2);
                                                                    command2.ExecuteReader();
                                                                    oReader.Close();
                                                                    throw new Exception(exItens.Message);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        fLog(arq.Name, "quantidadepedido = 0");
                                                        bVal = true;
                                                        aguardaVex = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                fLog(arq.Name, ex.Message);
                                                bVal = true;
                                                aguardaVex = true;
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            fLog(arq.Name, ex.Message);
                                            bVal = true;
                                        }
                                    }
                                    else
                                    {
                                        using (SqlConnection connection2 = dbt.GetConnection())
                                        {
                                            fLog(arq.Name, "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'O IDPRD: " + item.Idprd + " NÃO ENCONTRADO NA TABELA DE RETORNO VEX,'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')");
                                            //verificar;
                                            SqlCommand command2 = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, COD_PRD, QUANTIDADE, CHAVEACESSO) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + item.CodProduto + "', '" + item.NomeProduto + "', '" + item.CodUnd + "', 'O IDPRD: " + item.Idprd + " NÃO ENCONTRADO NA TABELA DE RETORNO VEX','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + item.Idprd + "', " + item.Quantidade.Replace(".", "").Replace(",", ".") + ",'" + dadosNf.ChaveAcesso + "')", connection2);
                                            try
                                            {
                                                connection2.Open();
                                                command2.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui1");
                                                command2 = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection2);
                                                command2.ExecuteReader();
                                                oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }
                                        }
                                        bVal = true;
                                    }
                                }
                            }
                            else
                            {
                                bVal = true;
                            }
                            dr.Close();

                        }
                        catch (Exception ex)
                        {
                            fLog(arq.Name, "aqui2");
                            command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                            command.ExecuteReader();
                            bVal = true;
                        }
                    }
                    i++;
                }
            }
            catch (Exception ex) { bVal = true; fLog(arq.Name, "aqui2"); }
            return bVal;
        }
        */
        private void AtualizarTmovRelac(dadosNfe dadosNF, string resultadoProcessa)
        {
            if (resultadoProcessa.Contains("Já Existe Nota")) return;

            string idmovDestino = resultadoProcessa.Substring(resultadoProcessa.IndexOf(";") + 1);
            string comando = "";
            List<int> idMovUnico = new List<int>();
            foreach (itensNfe item in dadosNF.Itens)
            {
                if (!idMovUnico.Contains(item.Idmov))
                {
                    idMovUnico.Add(item.Idmov);
                }
            }

            int nseqDest = 0;
            foreach (itensNfe item in dadosNF.Itens)
            {
                nseqDest++;
                comando += " INSERT INTO TITMMOVRELAC(IDMOVORIGEM, NSEQITMMOVORIGEM, CODCOLORIGEM, IDMOVDESTINO, NSEQITMMOVDESTINO, CODCOLDESTINO, QUANTIDADE) VALUES " +
                      "(" + dadosNF.Itens[0].Idmov.ToString() + ", " + item.Nseq.ToString() + ", " + dadosNF.SColigada + ", " + idmovDestino + ", " + nseqDest + ", " + dadosNF.SColigada + ", " + qtdeCritica + ") ";

                comando += " UPDATE TITMMOV SET QUANTIDADE = CASE WHEN QUANTIDADE - " + qtdeCritica + " < 0 THEN 0 ELSE QUANTIDADE - " + qtdeCritica + " END WHERE IDMOV = " + item.Idmov.ToString() + " AND NSEQITMMOV = " + item.Nseq.ToString() + " AND CODCOLIGADA = " + dadosNF.SColigada + " ";
            }
            foreach (int idMov in idMovUnico)
            {
                comando += " INSERT INTO TMOVRELAC(IDMOVORIGEM, CODCOLORIGEM, IDMOVDESTINO, CODCOLDESTINO, TIPORELAC) VALUES " +
                           "(" + idMov.ToString() + ", " + dadosNF.SColigada + ", " + idmovDestino + ", " + dadosNF.SColigada + ", 'P') ";
                comando += " IF((SELECT SUM(QUANTIDADE) FROM TITMMOV WHERE IDMOV = " + dadosNF.Itens[0].Idmov.ToString() + ") > 0) UPDATE TMOV SET STATUS = 'G' WHERE IDMOV = " + dadosNF.Itens[0].Idmov.ToString() +
                          " ELSE UPDATE TMOV SET STATUS = 'F' WHERE IDMOV = " + dadosNF.Itens[0].Idmov.ToString() + " ";
            }

            dbt.executarSQL(comando, "Crazms");
        }

        private DataRow buscarProduto(string nomeXML, int idMov, int idPrd)
        {
            //Busca o produto no movimento selecionado, se houver, retornará o produto preenchido.
            DataRow[] foundRows;
            DataSet ds = new DataSet();
            DataTable dtProd = new DataTable();
            DataTable dtAssoc = new DataTable();
            string filtro = "";
            //ds = buscaDadosRecord(nomeXML, " TITMMOV.IDMOV  = " + idMov.ToString() + " AND TITMMOV.IDPRD = " + idPrd.ToString() + " AND TITMMOV.CODCOLIGADA = " + sCodColigada.ToString(), "MovMovimentoTBCData", "svcjoins_xml", "svcjoins_xml");
            ds = buscaDadosRecord(nomeXML, sCodColigada + "; " + idMov.ToString(), "MovMovimentoTBCData", "svcjoins_xml", "svcjoins_xml");
            dtProd = ds.Tables["TITMMOV"];
            if (ds.Tables.Count <= 0)
            {
                return null;
            }

            List<string> associados = new List<string>();
            ds = dbt.resultadoConsulta("SELECT IDMOV_ASS FROM ZCRITICAXML WHERE IDMOV_ASS IS NOT NULL");
            dtAssoc = ds.Tables[0];
            foreach (DataRow linha in dtAssoc.Rows)
            {
                if (!associados.Contains(linha[0].ToString()))
                {
                    associados.Add(linha[0].ToString());
                }
            }
            if (associados.Count > 0)
            {
                filtro = " AND IDMOV NOT IN (" + String.Join(",", associados.ToArray()) + ")";
            }

            string expression = "IDPRD = " + idPrd.ToString() + filtro;
            string sort = "IDPRD DESC";

            foundRows = dtProd.Select(expression, sort);
            if (foundRows.Length == 1)
            {
                return foundRows[0];
            }
            else return null;
        }

        private void CriticarXML(string InsertCritica, string InsertErrosAplic)
        {
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand(InsertCritica, connection);
                try
                {
                    connection.Open();
                    command.ExecuteReader();
                    if (oReader != null)
                    {
                        oReader.Close();
                    }
                }
                catch (Exception ex2)
                {
                    command = new SqlCommand(InsertErrosAplic + ex2.Message.Replace("'", "") + "')", connection);
                    command.ExecuteReader();
                    if (oReader != null)
                    {
                        oReader.Close();
                    }
                }
            }
        }

        private DataSet validaCnpjEmpresa(string nomeArquivo)
        {
            string serverAddress = serverTBC;
            string dataServerName = "FisFilialDataBR";
            fLog(nomeArquivo, "Busca Dados Filial" + dataServerName);
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string user = sCodUsuarioTBC;
            string senha = sSenhaTBC;
            string viewData = null;
            string filter = "GFILIAL.CODCOLIGADA IS NOT NULL";
            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, user, senha))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(user, senha);

                    viewData = client.ReadView(dataServerName, filter, context);
                }
            }
            DataSet dat = null;
            if (!string.IsNullOrEmpty(viewData))
            {
                dat = loadDataSet(viewData);
            }
            return dat;
        }
        private DataSet buscaDadosConsulta(string nomeArquivo, string filter, string user, string senha, string aplicacao, string codSentenca)
        {
            fLog(nomeArquivo, "Busca dados Consulta" + codSentenca + " - " + filter);
            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            using (Consulta.IwsConsultaSQLClient client = this.CreateClientConsulta(serverAddress, user, senha))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {

                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(user, senha);
                    viewData = client.RealizarConsultaSQL(codSentenca, sCodColigada, aplicacao, filter);
                }
            }
            /*
            if (viewData == "&lt;NewDataSet /&gt;" || viewData  == "<NewDataSet />")
            {
                fLog(nomeArquivo, "entrou");
                viewData = "";
            }*/
            DataSet dat = null;
            if (!string.IsNullOrEmpty(viewData))
            {
                dat = loadDataSet(viewData);
            }
            return dat;
        }
        private DataSet buscaDados(string nomeArquivo, string filter, string dataServerName, string user, string senha)
        {
            fLog(nomeArquivo, "Busca dados " + dataServerName + " - " + filter);
            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, user, senha))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {

                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(user, senha);

                    viewData = client.ReadView(dataServerName, filter, context);
                }
            }
            /*
            if (viewData == "&lt;NewDataSet /&gt;" || viewData  == "<NewDataSet />")
            {
                fLog(nomeArquivo, "entrou");
                viewData = "";
            }*/
            DataSet dat = null;
            if (!string.IsNullOrEmpty(viewData))
            {
                dat = loadDataSet(viewData);
            }
            return dat;
        }

        private DataSet buscaDadosRecord(string nomeArquivo, string filter, string dataServerName, string user, string senha)
        {
            fLog(nomeArquivo, "Busca dados " + dataServerName + " - " + filter);
            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=" + sCodColigada.ToString() + ";Codsistema=G";
            string viewData = null;
            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, user, senha))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {

                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(user, senha);

                    viewData = client.ReadRecord(dataServerName, filter, context);
                }
            }
            DataSet dat = null;
            if (!string.IsNullOrEmpty(viewData))
            {
                dat = loadDataSet(viewData);
            }
            return dat;
        }

        public DataSet loadDataSet(string viewData)
        {
            
            if (!viewData.StartsWith("<"))
            {
                throw new Exception(viewData);
            }

            DataSet dataSet = new DataSet();
            dataSet.EnforceConstraints = false;

            loadDataSet(dataSet, viewData);

            dataSet.AcceptChanges();

            return dataSet;
        }

        private void loadDataSet(DataSet dataSet, string data)
        {
            if (!data.StartsWith("<", StringComparison.CurrentCultureIgnoreCase))
            {
                throw new Exception(data);
            }
            dataSet.Clear();
            try
            {
                using (StringReader reader = new StringReader(data))
                {
                    dataSet.ReadXml(reader, XmlReadMode.Auto);
                }
            }
            catch
            {
                dataSet.Clear();
                using (StringReader reader = new StringReader(data))
                {
                    dataSet.ReadXml(reader, XmlReadMode.IgnoreSchema);
                }
            }
        }

        private VexPedidosNovo.ServiceSoapClient CreateClientVexNovo(string serverAddress, string user, string senha)
        {
            string url = "http://129.159.63.229:8088/Service.asmx?wsdl";
            VexPedidosNovo.ServiceSoapClient client = new VexPedidosNovo.ServiceSoapClient(
                CreateBindingVex(), new System.ServiceModel.EndpointAddress(serverAddress));
            client.ClientCredentials.UserName.UserName = user;
            client.ClientCredentials.UserName.Password = senha;
            return client;
        }
        private VexPedidos.VexPedidosSoapClient CreateClientVex(string serverAddress, string user, string senha)
        {
            string url = "https://www2.vexlogistica.com.br/wss/vexpedidos.asmx?wsdl";
            VexPedidos.VexPedidosSoapClient client = new VexPedidos.VexPedidosSoapClient(
                CreateBindingVex(), new System.ServiceModel.EndpointAddress(serverAddress));
            client.ClientCredentials.UserName.UserName = user;
            client.ClientCredentials.UserName.Password = senha;
            return client;
        }

        private IwsDataServerClient CreateClient(string serverAddress, string user, string senha)
        {
            string url = asmxTBC;
            
            DataServer.IwsDataServerClient client = new DataServer.IwsDataServerClient(
                CreateBinding(), new System.ServiceModel.EndpointAddress(url));
            client.ClientCredentials.UserName.UserName = user;
            client.ClientCredentials.UserName.Password = senha;
            return client;
        }

        private IwsProcessClient CreateClientProcess(string serverAddress, string user, string senha)
        {
            
            string url = asmxProcessTBC;
            fLog("teste", asmxProcessTBC);
            Process.IwsProcessClient client = new Process.IwsProcessClient(
                CreateBinding(), new System.ServiceModel.EndpointAddress(url));
            client.ClientCredentials.UserName.UserName = user;
            client.ClientCredentials.UserName.Password = senha;
            return client;
        }
        private IwsConsultaSQLClient CreateClientConsulta(string serverAddress, string user, string senha)
        {

            string url = asmxConsultaTBC;
            fLog("teste", asmxConsultaTBC);
            Consulta.IwsConsultaSQLClient client = new Consulta.IwsConsultaSQLClient(
                CreateBinding(), new System.ServiceModel.EndpointAddress(url));
            client.ClientCredentials.UserName.UserName = user;
            client.ClientCredentials.UserName.Password = senha;
            return client;
        }
        public static System.ServiceModel.Channels.Binding CreateBindingVex()
        {
            BasicHttpBinding binding = new BasicHttpBinding();
            binding.TextEncoding = System.Text.Encoding.UTF8;

            binding.MaxBufferPoolSize = Int32.MaxValue;
            binding.MaxReceivedMessageSize = Int32.MaxValue;
            binding.ReaderQuotas = new System.Xml.XmlDictionaryReaderQuotas();
            binding.ReaderQuotas.MaxArrayLength = int.MaxValue;
            binding.ReaderQuotas.MaxBytesPerRead = int.MaxValue;
            binding.ReaderQuotas.MaxDepth = int.MaxValue;
            binding.ReaderQuotas.MaxNameTableCharCount = int.MaxValue;
            binding.ReaderQuotas.MaxStringContentLength = int.MaxValue;

            binding.CloseTimeout = TimeSpan.MaxValue;
            binding.OpenTimeout = TimeSpan.MaxValue;
            binding.ReceiveTimeout = TimeSpan.MaxValue;
            binding.SendTimeout = TimeSpan.MaxValue;

            binding.Security.Mode = BasicHttpSecurityMode.None;
            return binding;
        }
        public static System.ServiceModel.Channels.Binding CreateBinding()
        {
            BasicHttpBinding binding = new BasicHttpBinding();
            binding.TextEncoding = System.Text.Encoding.UTF8;

            binding.MaxBufferPoolSize = Int32.MaxValue;
            binding.MaxReceivedMessageSize = Int32.MaxValue;
            binding.ReaderQuotas = new System.Xml.XmlDictionaryReaderQuotas();
            binding.ReaderQuotas.MaxArrayLength = int.MaxValue;
            binding.ReaderQuotas.MaxBytesPerRead = int.MaxValue;
            binding.ReaderQuotas.MaxDepth = int.MaxValue;
            binding.ReaderQuotas.MaxNameTableCharCount = int.MaxValue;
            binding.ReaderQuotas.MaxStringContentLength = int.MaxValue;

            binding.CloseTimeout = TimeSpan.MaxValue;
            binding.OpenTimeout = TimeSpan.MaxValue;
            binding.ReceiveTimeout = TimeSpan.MaxValue;
            binding.SendTimeout = TimeSpan.MaxValue;

            binding.Security.Mode = BasicHttpSecurityMode.Transport;
            binding.Security.Transport.ClientCredentialType = HttpClientCredentialType.Basic;
            return binding;
        }
        public static HttpRequestMessageProperty CreateBasicAuthorizationMessageProperty(string username, string password)
        {
            Encoding encoding = Encoding.GetEncoding("iso-8859-1");
            string credential = String.Format("{0}:{1}", username, password);

            var httpRequestProperty = new HttpRequestMessageProperty();
            httpRequestProperty.Headers[System.Net.HttpRequestHeader.Authorization] =
                "Basic " + Convert.ToBase64String(encoding.GetBytes(credential));

            return httpRequestProperty;
        }
        private string PopulaTabelasCte(dadosCte dados, ElapsedEventArgs e)
        {
            var movimentoEstoque = new MOVMOVIMENTO
            {
                TMOV = new TMOV
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = -1,
                    CODFILIAL = short.Parse(dados.SFilial),
                    CODCFO = dados.SCodCfoEmi,
                    CODCOLCFO = dados.SColCfoEmi,
                    CODTMV = dados.MovCte,
                    SERIE = dados.Serie,
                    TIPO = "P",
                    CODCPG = dados.CodCpg,
                    IDNAT = dados.Idnat,
                    STATUS = "F",
                    CODTDO = "DACTE",
                    CODLOC = dados.SCodLoc,
                    CODCCUSTO = dados.SCodccusto,
                    CHAVEACESSONFE = dados.ChaveAcesso,
                    DATAEMISSAO = DateTime.Parse(dados.SDataEmissao),
                    NUMEROMOV = dados.SNumeromov,
                    RECCREATEDON = DateTime.Now,
                    RECCREATEDBY = sCodUsuarioTBC

                }
            };

            movimentoEstoque.TMOVCOMPL = new TMOVCOMPL
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = -1

            };

            movimentoEstoque.TMOVHISTORICO = new TMOVHISTORICO
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = -1,

            };
            if (dados.SCodCfoRem != "" && !string.IsNullOrEmpty(dados.SCodCfoRem) && !string.IsNullOrEmpty(dados.SCodCfoDest) && dados.SCodCfoDest != "")
            {
                movimentoEstoque.TMOVTRANSP = new TMOVTRANSP
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = -1,
                    CODETDENTREGA = dados.UfFim,
                    CODMUNICIPIOENTREGA = dados.MunicipioFim,
                    CODETDCOLETA = dados.UfIni,
                    CODMUNICIPIOCOLETA = dados.MunicipioIni,
                    TIPOREMETENTE = "C",
                    REMETENTECODCOLCFO = dados.SColCfoRem,
                    REMETENTECODCFO = dados.SCodCfoRem,
                    TIPODESTINATARIO = "C",
                    DESTINATARIOCODCOLCFO = dados.SColCfoDest,
                    DESTINATARIOCODCFO = dados.SCodCfoDest

                };
            }
            else if (dados.SCodCfoDest == "" || string.IsNullOrEmpty(dados.SCodCfoDest))
            {
                movimentoEstoque.TMOVTRANSP = new TMOVTRANSP
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = -1,
                    CODETDENTREGA = dados.UfFim,
                    CODMUNICIPIOENTREGA = dados.MunicipioFim,
                    CODETDCOLETA = dados.UfIni,
                    CODMUNICIPIOCOLETA = dados.MunicipioIni,
                    TIPOREMETENTE = "C",
                    REMETENTECODCOLCFO = dados.SColCfoRem,
                    REMETENTECODCFO = dados.SCodCfoRem,
                    TIPODESTINATARIO = "M",
                    DESTINATARIOCODCOLCFO = dados.SColigada,
                    DESTINATARIOFILIAL = dados.SFilial

                };
            }
            else
            {
                movimentoEstoque.TMOVTRANSP = new TMOVTRANSP
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = -1,
                    CODETDENTREGA = dados.UfFim,
                    CODMUNICIPIOENTREGA = dados.MunicipioFim,
                    CODETDCOLETA = dados.UfIni,
                    CODMUNICIPIOCOLETA = dados.MunicipioIni,
                    TIPOREMETENTE = "M",
                    REMETENTECODCOLCFO = dados.SColigada,
                    REMETENTEFILIAL = dados.SFilial,
                    TIPODESTINATARIO = "C",
                    DESTINATARIOCODCOLCFO = dados.SColCfoDest,
                    DESTINATARIOCODCFO = dados.SCodCfoDest

                };
            }

            var itensMovimento = new List<TITMMOV>();
            var NSEQITMMOV = 1;
            itensMovimento.Add(new TITMMOV
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = -1,
                NSEQITMMOV = NSEQITMMOV,
                IDPRD = dados.Idprd,
                IDNAT = dados.Idnat,
                QUANTIDADE = "1",
                CODCCUSTO = dados.SCodccusto,
                CODUND = "UNID",
                PRECOUNITARIO = dados.DvProduto  // - Alteração conforme solicitação do Claudinei em 26/07/2018 para enviar o preço ou 0 em caso de nulo ou valor negativo
            });
            var itensMovimentoFiscal = new List<TITMMOVFISCAL>();
            itensMovimentoFiscal.Add(new TITMMOVFISCAL
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = -1,
                NSEQITMMOV = NSEQITMMOV,
                INDNATFRETE = 0  // - Alteração conforme solicitação do Claudinei em 26/07/2018 para enviar o preço ou 0 em caso de nulo ou valor negativo
            });
            string retorno;
            retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>CTE</CODMEN><DESCRICAO>Historico CTE</DESCRICAO><DESCRICAOMEMO>" + dados.Obs + "</DESCRICAOMEMO></TMen>", "MovMenData");
            movimentoEstoque.TITMMOV = itensMovimento.ToArray();
            movimentoEstoque.TITMMOVFISCAL = itensMovimentoFiscal.ToArray();
            var dtServerNameMov = "MovMovimentoTBCData";
            //var contextMov = $"CODCOLIGADA = 1; CODSISTEMA = T; CODUSUARIO = {lancamento.USUARIO}";
            var strxmlMovAllowance = $"<![CDATA[{movimentoEstoque.SerializeObject()}]]>";

            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            //string dataServerName = "MovMovimentoTBCData";

            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, sCodUsuarioTBC, sSenhaTBC))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(sCodUsuarioTBC, sSenhaTBC);

                    viewData = client.SaveRecord(dtServerNameMov, movimentoEstoque.SerializeObject(), context);
                }
            }
            retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>CTE</CODMEN><DESCRICAO>Historico CTE</DESCRICAO><DESCRICAOMEMO></DESCRICAOMEMO></TMen>", "MovMenData");
            return viewData;
        }

        private string PopulaTabelasNfe(dadosNfe dados, ElapsedEventArgs e)
        {
            var movimentoEstoque = new MOVMOVIMENTO
            {
                TMOV = new TMOV
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = dados.Idmov,
                    CODFILIAL = short.Parse(dados.SFilial),
                    CODCFO = dados.SCodCfoEmi,
                    CODCOLCFO = dados.SColCfoEmi,
                    CODTMV = dados.MovNfe,
                    SERIE = dados.Serie,
                    TIPO = "P",
                    //CODCPG = dados.CodCpg,
                    IDNAT = dados.Idnat,
                    STATUS = "F",
                    CODTDO = dados.CodTdo,
                    CODTRA = dados.CodTra,
                    //CODLOC = dados.SCodLoc,
                    CODCCUSTO = dados.SCodccusto,
                    CHAVEACESSONFE = dados.ChaveAcesso,
                    DATAEMISSAO = DateTime.Parse(dados.SDataEmissao),
                    DATAEXTRA1 = DateTime.Parse(dados.SDataEmissao),
                    NUMEROMOV = dados.SNumeromov,
                    OBSERVACAO = dados.Obs,
                    VALORDESP = dados.VOutros,
                    VALORFRETE = dados.VFrete,
                    VALORSEGURO = dados.VSeguro,
                    VALORDESC = dados.VDesconto,
                    SEGUNDONUMERO = dados.SegundoNumero,
                    RECCREATEDON = DateTime.Now,
                    RECCREATEDBY = sCodUsuarioTBC
                }
            };



            movimentoEstoque.TMOVCOMPL = new TMOVCOMPL
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = dados.Idmov

        };

            movimentoEstoque.TMOVHISTORICO = new TMOVHISTORICO
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = dados.Idmov,
            };
            var itensMovimento = new List<TITMMOV>();
            var itensMovimentoCompl = new List<TITMMOVCOMPL>();
            var itensTributos = new List<TTRBITMMOV>();
            var itensLote = new List<TITMLOTEPRD>();
            //var NSEQITMMOV = 1;

            foreach (itensNfe item in dados.Itens)
            {
                itensMovimento.Add(new TITMMOV
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = dados.Idmov,
                    NSEQITMMOV = item.Nseq,
                    IDPRD = int.Parse(item.Idprd),
                    IDNAT = item.Idnat,
                    QUANTIDADE = item.Quantidade,
                    CODCCUSTO = dados.SCodccusto,
                    CODUND = item.CodUnd,
                    PRECOUNITARIO = item.ValorProduto,
                    RATEIODESC = item.VDesconto,
                    RATEIODESP = item.VOutros,
                    RATEIOFRETE = item.VFrete,
                    RATEIOSEGURO = item.VSeguro
                    // - Alteração conforme solicitação do Claudinei em 26/07/2018 para enviar o preço ou 0 em caso de nulo ou valor negativo
                });
                itensMovimentoCompl.Add(new TITMMOVCOMPL
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = dados.Idmov,
                    NSEQITMMOV = item.Nseq,
                    QTDALIMENTAR = item.EstoqueAlimentar,
                    QTDMAISBRASIL = item.EstoqueMaisBrasil,
                    QTDVAIVAREJO = item.EstoqueVaiVarejo,
                    ESTOQUE_SOBRA = item.EstoqueSobra,
                      // - Alteração conforme solicitação do Claudinei em 26/07/2018 para enviar o preço ou 0 em caso de nulo ou valor negativo
                });
                if (item.AliqIcms != "0" & !String.IsNullOrEmpty(item.AliqIcms))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = dados.Idmov,
                        NSEQITMMOV = item.Nseq,
                        CODTRB = "ICMS",
                        ALIQUOTA = item.AliqIcms,
                        BASEDECALCULO = item.BaseIcms,
                        VALOR = item.ValorIcms,
                        SITTRIBUTARIA = item.CstIcms,
                        FATORREDUCAO = item.BaseRed,
                        EDITADO = 1,
                    });
                }
                if (item.AliqIcmsSt != "0" & !String.IsNullOrEmpty(item.AliqIcmsSt))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = dados.Idmov,
                        NSEQITMMOV = item.Nseq,
                        CODTRB = "ICMSST",
                        ALIQUOTA = item.AliqIcmsSt,
                        BASEDECALCULO = item.BaseIcmsSt,
                        VALOR = item.ValorIcmsSt,
                        FATORREDUCAO = item.BaseRedIcmsST,
                        EDITADO = 1,
                    });
                }
                if (item.AliqPis != "0" & !String.IsNullOrEmpty(item.AliqPis))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = dados.Idmov,
                        NSEQITMMOV = item.Nseq,
                        CODTRB = "PIS",
                        ALIQUOTA = item.AliqPis,
                        BASEDECALCULO = item.BasePis,
                        VALOR = item.ValorPis,
                        SITTRIBUTARIA = item.CstPis,
                        EDITADO = 1
                    });
                }
                if (item.AliqCofins != "0" & !String.IsNullOrEmpty(item.AliqCofins))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = dados.Idmov,
                        NSEQITMMOV = item.Nseq,
                        CODTRB = "COFINS",
                        ALIQUOTA = item.AliqCofins,
                        BASEDECALCULO = item.BaseCofins,
                        VALOR = item.ValorCofins,
                        SITTRIBUTARIA = item.CstCofins,
                        EDITADO = 1
                    });
                }
                if (item.AliqIpi != "0" & !String.IsNullOrEmpty(item.AliqIpi))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = dados.Idmov,
                        NSEQITMMOV = item.Nseq,
                        CODTRB = "IPI",
                        ALIQUOTA = item.AliqIpi,
                        BASEDECALCULO = item.BaseIpi,
                        VALOR = item.ValorIpi,
                        SITTRIBUTARIA = item.CstIpi,
                        EDITADO = 1
                    });
                }
                if (item.IdLote != 0 & !String.IsNullOrEmpty(item.NumLote))
                {
                    itensLote.Add(new TITMLOTEPRD
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = dados.Idmov,
                        IDLOTE = item.IdLote,
                        NUMLOTE = item.NumLote,
                        QUANTIDADE2 = item.Quantidade,
                        QUANTIDADEARECEBER = item.Quantidade,
                        NSEQITMMOV = item.Nseq,
                    });
                }
                
            }

            string retorno;
            retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>NFE</CODMEN><DESCRICAO>Historico NFe</DESCRICAO><DESCRICAOMEMO>" + dados.Obs + "</DESCRICAOMEMO></TMen>", "MovMenData");

            movimentoEstoque.TITMMOV = itensMovimento.ToArray();
            movimentoEstoque.TITMMOVCOMPL = itensMovimentoCompl.ToArray();
            movimentoEstoque.TITMLOTEPRD = itensLote.ToArray();
            movimentoEstoque.TTRBITMMOV = itensTributos.ToArray();

            var dtServerNameMov = "MovMovimentoTBCData";
            //var contextMov = $"CODCOLIGADA = 1; CODSISTEMA = T; CODUSUARIO = {lancamento.USUARIO}";
            var strxmlMovAllowance = $"<![CDATA[{movimentoEstoque.SerializeObject()}]]>";
            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            //string dataServerName = "MovMovimentoTBCData";

            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, sCodUsuarioTBC, sSenhaTBC))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(sCodUsuarioTBC, sSenhaTBC);

                    viewData = client.SaveRecord(dtServerNameMov, movimentoEstoque.SerializeObject(), context);
                }
            }
            retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>NFE</CODMEN><DESCRICAO>Historico NFe</DESCRICAO><DESCRICAOMEMO></DESCRICAOMEMO></TMen>", "MovMenData");
            return viewData;
        }

        private string PopulaTabelasNfse(dadosNfe dados, ElapsedEventArgs e)
        {
            var movimentoEstoque = new MOVMOVIMENTO
            {
                TMOV = new TMOV
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = -1,
                    CODFILIAL = short.Parse(dados.SFilial),
                    CODCFO = dados.SCodCfoEmi,
                    CODCOLCFO = dados.SColCfoEmi,
                    CODTMV = dados.MovNfe,
                    SERIE = dados.Serie,
                    TIPO = "P",
                    CODCPG = dados.CodCpg,
                    //CODCPG = "001",
                    IDNAT = dados.Idnat,
                    STATUS = "F",
                    CODTDO = dados.CodTdo,
                    CODLOC = dados.SCodLoc,
                    CODCCUSTO = dados.SCodccusto,
                    CHAVEACESSONFE = dados.ChaveAcesso,
                    VALORSERVICO = dados.TotalNf,
                    DATAEMISSAO = DateTime.Parse(dados.SDataEmissao),
                    NUMEROMOV = dados.SNumeromov,
                    RECCREATEDON = DateTime.Now,
                    RECCREATEDBY = sCodUsuarioTBC
                }
            };



            movimentoEstoque.TMOVCOMPL = new TMOVCOMPL
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = -1
            };

            movimentoEstoque.TMOVHISTORICO = new TMOVHISTORICO
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                IDMOV = -1,
            };
            var itensMovimento = new List<TITMMOV>();
            var itensTributos = new List<TTRBITMMOV>();
            var NSEQITMMOV = 1;

            foreach (itensNfe item in dados.Itens)
            {
                itensMovimento.Add(new TITMMOV
                {
                    CODCOLIGADA = short.Parse(dados.SColigada),
                    IDMOV = -1,
                    NSEQITMMOV = NSEQITMMOV,
                    IDPRD = int.Parse(item.Idprd),
                    IDNAT = item.Idnat,
                    QUANTIDADE = "1",
                    CODCCUSTO = dados.SCodccusto,
                    CODUND = item.CodUnd,
                    PRECOUNITARIO = item.ValorProduto,  // - Alteração conforme solicitação do Claudinei em 26/07/2018 para enviar o preço ou 0 em caso de nulo ou valor negativo
                });



                if (item.AliqIss != "0" & !String.IsNullOrEmpty(item.AliqIss))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "ISS",
                        ALIQUOTA = item.AliqIss,
                        BASEDECALCULO = item.ValorProduto,
                        VALOR = item.ValorIss,
                        EDITADO = 1,
                    });
                }
                else
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "ISS",
                        ALIQUOTA = "0",
                        BASEDECALCULO = "0",
                        VALOR = "0",
                        EDITADO = 1,
                    });
                }
                if (item.AliqInss != "0" & !String.IsNullOrEmpty(item.AliqInss))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "INSS",
                        ALIQUOTA = item.AliqInss,
                        BASEDECALCULO = item.ValorProduto,
                        VALOR = item.ValorInss,
                        EDITADO = 1,
                    });
                }
                else
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "INSS",
                        ALIQUOTA = "0",
                        BASEDECALCULO = "0",
                        VALOR = "0",
                        EDITADO = 1,
                    });
                }

                if (item.AliqIr != "0" & !String.IsNullOrEmpty(item.AliqIr))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "IRRFPJ",
                        ALIQUOTA = item.AliqIr,
                        BASEDECALCULO = item.ValorProduto,
                        VALOR = item.ValorIr,
                        EDITADO = 1
                    });
                }
                else
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "IRRFPJ",
                        ALIQUOTA = "0",
                        BASEDECALCULO = "0",
                        VALOR = "0",
                        EDITADO = 1
                    });
                }
                if (item.AliqCsll != "0" & !String.IsNullOrEmpty(item.AliqCsll))
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "CSRF",
                        ALIQUOTA = item.AliqCsll,
                        BASEDECALCULO = item.ValorProduto,
                        VALOR = item.ValorCsll,
                        EDITADO = 1
                    });
                }
                else
                {
                    itensTributos.Add(new TTRBITMMOV
                    {
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        IDMOV = -1,
                        NSEQITMMOV = NSEQITMMOV,
                        CODTRB = "CSLL",
                        ALIQUOTA = "0",
                        BASEDECALCULO = "0",
                        VALOR = "0",
                        EDITADO = 1
                    });
                }

                NSEQITMMOV = NSEQITMMOV + 1;
            }
            movimentoEstoque.TITMMOV = itensMovimento.ToArray();
            //movimentoEstoque.TTRBITMMOV = itensTributos.ToArray();
            var dtServerNameMov = "MovMovimentoTBCData";
            //var contextMov = $"CODCOLIGADA = 1; CODSISTEMA = T; CODUSUARIO = {lancamento.USUARIO}";
            var strxmlMovAllowance = $"<![CDATA[{movimentoEstoque.SerializeObject()}]]>";

            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            //string dataServerName = "MovMovimentoTBCData";

            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, sCodUsuarioTBC, sSenhaTBC))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(sCodUsuarioTBC, sSenhaTBC);

                    viewData = client.SaveRecord(dtServerNameMov, movimentoEstoque.SerializeObject(), context);
                }
            }
            //retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>CTE</CODMEN><DESCRICAO>Historico CTE</DESCRICAO><DESCRICAOMEMO></DESCRICAOMEMO></TMen>");
            return viewData;
        }
        private void excluirMovimento(string nome, string message)
        {
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("INSERT INTO ZERROSAPLICXML(NOME_XML, DATA, ERRO) VALUES('" + nome.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + message.Replace("'", "") + "')", connection);
                try
                {
                    connection.Open();
                    command.ExecuteReader();
                    if (oReader != null)
                    {
                        oReader.Close();
                    }
                }
                catch (Exception ex2)
                {
                    if (oReader != null)
                    {
                        oReader.Close();
                    }
                }
            }
        }
        private void populaSenior(dadosSenior dadosS)
        {
            var XMLSENIOR = new xml();
            var cabecalho = new cabecalho();

            cabecalho.codigointerno = dadosS.CodigoInterno;
            cabecalho.numpedido = dadosS.NumPedido;
            cabecalho.cnpj_depositante = dadosS.Cnpj_Depositante;
            cabecalho.cnpj_emitente = dadosS.Cnpj_Emitente;
            cabecalho.sequencia = dadosS.Sequencia;
            cabecalho.tipo = dadosS.Tipo;
            cabecalho.descroper = dadosS.Descroper;
            cabecalho.cfop = dadosS.Cfop;
            cabecalho.data_emissao = dadosS.Data_Emissao;
            cabecalho.pessoa_dest = dadosS.Pessoa_Dest;
            cabecalho.codigo_dest = dadosS.Codigo_Dest;
            cabecalho.nome_dest = dadosS.Nome_Dest;
            cabecalho.fantasia_dest = dadosS.Fantasia_Dest;
            cabecalho.cnpj_dest = dadosS.Cnpj_Dest;
            cabecalho.endereco_dest = dadosS.Endereco_Dest;
            cabecalho.numend_dest = dadosS.Numend_Dest;
            cabecalho.complementoend_dest = dadosS.Complementoend_Dest;
            cabecalho.bairro_dest = dadosS.Bairro_Dest;
            cabecalho.cep_dest = dadosS.Cep_Dest;
            cabecalho.cidade_dest = dadosS.Cidade_Dest;
            cabecalho.telefone_dest = dadosS.Telefone_Dest;
            cabecalho.estado_dest = dadosS.Estado_Dest;
            cabecalho.inscrestadual_dest = dadosS.Inscrestadual_Dest;
            cabecalho.inscrmunicipal_dest = dadosS.Inscrmunicipal_Dest;
            cabecalho.endereco_entrega = dadosS.Endereco_Entrega;
            cabecalho.cidade_entrega = dadosS.Cidade_Entrega;
            cabecalho.bairro_entrega = dadosS.Bairro_Entrega;
            cabecalho.estado_entrega = dadosS.Estado_Entrega;
            cabecalho.cep_entrega = dadosS.Cep_Entrega;
            cabecalho.cnpj_entrega = dadosS.Cnpj_Entrega;
            cabecalho.increstadual_entrega = dadosS.Increstadual_Entrega;
            cabecalho.baseicms = dadosS.Baseicms.ToString();
            cabecalho.valoricms = dadosS.Valoricms.ToString();
            cabecalho.basesubstituicao = dadosS.Basesubstituicao.ToString();
            cabecalho.valorsubstituicao = dadosS.Valorsubstituicao.ToString();
            cabecalho.frete = dadosS.Frete.ToString();
            cabecalho.seguro = dadosS.Seguro.ToString();
            cabecalho.despesasacessorias = dadosS.Despesasacessorias.ToString();
            cabecalho.ipi = dadosS.Ipi.ToString();
            cabecalho.vlrprodutos = dadosS.Vlrprodutos;
            cabecalho.vlrtotal = dadosS.Vlrtotal;
            cabecalho.nome_transp = dadosS.Nome_Transp;
            cabecalho.cnpj_transp = dadosS.Cnpj_Transp;
            cabecalho.endereco_transp = dadosS.Endereco_Transp;
            cabecalho.numend_transp = dadosS.Numend_Transp;
            cabecalho.bairro_transp = dadosS.Bairro_Transp;
            cabecalho.cidade_transp = dadosS.Cidade_Transp;
            cabecalho.estado_transp = dadosS.Estado_Transp;
            cabecalho.cep_transp = dadosS.Cep_Transp;
            cabecalho.increstadual_transp = dadosS.Increstadual_Transp;
            cabecalho.ciffob = dadosS.Ciffob;
            cabecalho.veiculo = dadosS.Veiculo;
            cabecalho.estado_veiculo = dadosS.Estado_Veiculo;
            cabecalho.qtde = dadosS.Qtde.ToString() ;
            cabecalho.especie = dadosS.Especie;
            cabecalho.marca = dadosS.Marca;
            cabecalho.numero = dadosS.Numero;
            cabecalho.pesoliquido = dadosS.Pesoliquido;
            cabecalho.pis = dadosS.Pis.ToString();
            cabecalho.cofins = dadosS.Cofins.ToString();
            cabecalho.cs = dadosS.Cs.ToString();
            cabecalho.ir = dadosS.Ir.ToString();
            cabecalho.valoriss = dadosS.Valoriss.ToString();
            cabecalho.valorservicos = dadosS.Valorservicos.ToString();
            cabecalho.idmovimento = dadosS.Idmovimento.ToString();
            cabecalho.idnotafiscal = dadosS.Idnotafiscal.ToString();
            cabecalho.gerafinceiro = dadosS.Gerafinceiro;
            cabecalho.tipodocumento = dadosS.Tipodocumento;
            cabecalho.tipocarga = dadosS.Tipocarga;
            cabecalho.limitecorte = dadosS.Limitecorte.ToString();
            cabecalho.paginageomapa = dadosS.Paginageomapa.ToString();
            cabecalho.num_itens = dadosS.Num_Itens.ToString();
            cabecalho.tiponf = dadosS.Tiponf;
            cabecalho.estado = dadosS.Estado;
            cabecalho.data_coleta = dadosS.Data_Coleta;
            cabecalho.hora_coleta = dadosS.Hora_Coleta;
            cabecalho.pessoa_entrega = dadosS.Pessoa_Entrega;
            cabecalho.codigo_entrega = dadosS.Codigo_Entrega;
            cabecalho.nome_entrega = dadosS.Nome_Entrega;
            cabecalho.fantasia_entrega = dadosS.Fantasia_Entrega;
            cabecalho.numend_entrega = dadosS.Numend_Entrega;
            cabecalho.complementoend_entrega = dadosS.Complementoend_Entrega;
            cabecalho.nomerepresentante = dadosS.Nomerepresentante;
            cabecalho.telefone_representante = dadosS.Telefone_Representante;
            cabecalho.cnpj_unidade = dadosS.Cnpj_Unidade;
            cabecalho.fatura = dadosS.Fatura;
            cabecalho.observacao = dadosS.Observacao;
            cabecalho.estoqueverificado = dadosS.Estoqueverificado;
            cabecalho.chaveidentificacaoext = dadosS.Chaveidentificacaoext;
            cabecalho.classificacaocliente = dadosS.Classificacaocliente.ToString();
            cabecalho.prioridade = dadosS.Prioridade;
            cabecalho.porcentagemcxfechada = dadosS.Porcentagemcxfechada.ToString();
            cabecalho.chaveacessonfe = dadosS.Chaveacessonfe;
            cabecalho.sequenciaped = dadosS.Sequenciaped;
            cabecalho.cnpj_transpredespacho = dadosS.Cnpj_Transpredespacho;
            cabecalho.inscrestadual_emitente = dadosS.Inscrestadual_Emitente;
            cabecalho.codigo_servicotransp = dadosS.Codigo_Servicotransp;
            cabecalho.codigotipopedido = dadosS.Codigotipopedido;
            cabecalho.embarqueprioritario = dadosS.Embarqueprioritario.ToString();
            cabecalho.complemento_dest = dadosS.Complemento_Dest;
            cabecalho.roteiro = dadosS.Roteiro;
            cabecalho.seq_entrega = dadosS.Seq_Entrega.ToString();
            cabecalho.nome_emit = dadosS.Nome_Emit;
            cabecalho.fantasia_emit = dadosS.Fantasia_Emit;
            cabecalho.codretencao = dadosS.Codretencao;
            cabecalho.valoricmsdesonerado = dadosS.Valoricmsdesonerado.ToString();
            cabecalho.presencacomprador = dadosS.Presencacomprador.ToString();
            cabecalho.formapagamento = dadosS.Formapagamento.ToString();
            cabecalho.tipoentrada = dadosS.Tipoentrada.ToString();
            cabecalho.utilizazpl = dadosS.Utilizazpl.ToString();
            cabecalho.codigorastreio = dadosS.Codigorastreio;
            cabecalho.codpais_dest = dadosS.Codpais_Dest;
            cabecalho.codservetiqext = dadosS.Codservetiqext;
            cabecalho.intcubometro = dadosS.Intcubometro;
            cabecalho.faturamentodoismomentos = dadosS.Faturamentodoismomentos.ToString();
            cabecalho.dataplanejamento = dadosS.Dataplanejamento;
            cabecalho.telefone2 = dadosS.Telefone2;

            cabecalho.itemS = dadosS.Itens;
            XMLSENIOR.cabecalho = cabecalho;

            string retorno = "";
            fLog(dadosS.Chaveacessonfe, XMLSENIOR.SerializeObject());
            try
            {
                retorno = retorno = postXMLData("https://clyj4jlj4m.execute-api.us-east-2.amazonaws.com/stable/xml-to-workflow-with-headers/yandeh-cloud/interface-yandeh-notaEntrada", XMLSENIOR.SerializeObject());
            }
            catch (Exception ex)
            {
                retorno = ex.Message;
            }
            fLog(dadosS.Chaveacessonfe, retorno);
            try
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    string viewMax = "";
                    if (retorno.Length > 200)
                    {
                        viewMax = retorno.Substring(0, 200);
                    }
                    else { viewMax = retorno; }
                    fLog(dadosS.Chaveacessonfe.ToString(), "INSERT INTO LOGENVIASENIOR(CHAVEACESSO, XML, RETORNO, RECCREATEDON) VALUES('" + dadosS.Chaveacessonfe.ToString() + "','" + XMLSENIOR.SerializeObject() + "','" + viewMax + "', GETDATE())");
                    SqlCommand command = new SqlCommand("INSERT INTO LOGENVIASENIOR(CHAVEACESSO, XML, RETORNO, RECCREATEDON) VALUES('" + dadosS.Chaveacessonfe.ToString() + "','" + XMLSENIOR.SerializeObject() + "','" + viewMax + "', GETDATE())", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                        fLog(dadosS.Chaveacessonfe.ToString(), "erroinserirzlog");
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + dadosS.Chaveacessonfe.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                    }
                }
            }
            catch (Exception ex) { fLog(dadosS.Chaveacessonfe.ToString(), ex.Message); }

        }
        public string postXMLData(string destinationUrl, string requestXml)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(destinationUrl);
            byte[] bytes;
            request.Headers.Add("Authorization", "Bearer " + "eyJhbGciOiJIUzUxMiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY3ODk5MzI2NiwiaWF0IjoxNjc4OTkzMjY2fQ.bJINCK4SeOEwST1M5mvIo3N6-aJSNN-45_JPsyTwFAgiwY0mpMDMSuK-792re9AeCPVtBoVT3SPo0ksQGeAmug");
            bytes = System.Text.Encoding.ASCII.GetBytes(requestXml);
            request.ContentType = "text/xml; encoding='utf-8'";
            request.ContentLength = bytes.Length;
            request.Method = "POST";
            Stream requestStream = request.GetRequestStream();
            requestStream.Write(bytes, 0, bytes.Length);
            requestStream.Close();
            HttpWebResponse response;
            response = (HttpWebResponse)request.GetResponse();
            if (response.StatusCode == HttpStatusCode.OK)
            {
                Stream responseStream = response.GetResponseStream();
                string responseStr = new StreamReader(responseStream).ReadToEnd();
                return responseStr;
            }
            return null;
        }
        private string PopulaVex(dadosVex dados, ElapsedEventArgs e)
        {
            var Recebimento = new VexPedidos.Recebimento();
            var Destinatario = new VexPedidos.Cadastro();
            var Remetente = new VexPedidos.Cadastro();
            var ItensEnvio = new List<VexPedidos.RecebimentoItens>();
            string validaVex = "";
            validaVex = "<PRJ4872704>";
            Destinatario.CEP = dados.DestCEP;
            Destinatario.CNPJ = dados.DestCNPJ;
            Destinatario.CodigoIBGE = dados.DestCodigoIBGE;
            Destinatario.Complemento = dados.DestComplemento;
            Destinatario.Endereco = dados.DestEndereco;
            Destinatario.Fantasia = dados.DestFantasia;
            Destinatario.IE = dados.DestIE;
            Destinatario.Numero = dados.DestNumero;
            Destinatario.RazaoSocial = dados.DestRazaoSocial;
            Destinatario.TipoDeEndereco = dados.DestTipoDeEndereco;
            Destinatario.UF = dados.DestUF;
            Remetente.CEP = dados.RemCEP;
            Remetente.CNPJ = dados.RemCNPJ;
            Remetente.CodigoIBGE = dados.RemCodigoIBGE;
            Remetente.Complemento = dados.RemComplemento;
            Remetente.Endereco = dados.RemEndereco;
            Remetente.Fantasia = dados.RemFantasia;
            Remetente.IE = dados.RemIE;
            Remetente.Numero = dados.RemNumero;
            Remetente.RazaoSocial = dados.RemRazaoSocial;
            Remetente.TipoDeEndereco = dados.RemTipoDeEndereco;
            Remetente.UF = dados.RemUF;
            Recebimento.Destinatario = Destinatario;
            Recebimento.Remetente = Remetente;
            Recebimento.Chave = dados.Chave;
            Recebimento.DataEmissaoNf = DateTime.Parse(dados.DataEmissaoNf);
            Recebimento.NumeroNfe = int.Parse(dados.NumeroNfe);
            Recebimento.Serie = dados.Serie;
            int i = 1;
            foreach (itensVex item in dados.Itens)
            {
                var teste = new VexPedidos.RecebimentoItens();
                try { teste.Codigo = item.Codigo; } catch { teste.Codigo = ""; }
                try { teste.Descricao = item.Descricao; } catch { teste.Descricao = ""; }
                try { teste.Quantidade = item.Quantidade; } catch { teste.Quantidade = 0; }
                try { teste.ValorUnitario = decimal.Parse(item.ValorUnitario); } catch { teste.ValorUnitario = 0; }
                try { teste.CodigoDeBarras = item.CodigoDeBarras; } catch { teste.CodigoDeBarras = ""; }
                try { teste.FatorDeConversao = item.FatorDeConversao; } catch { teste.FatorDeConversao = 1; }
                try { teste.QuantidadeNF = item.QuantidadeNF; } catch { teste.QuantidadeNF = 0; }
                validaVex = validaVex + "<ZMDVALIDAVEX><CHAVEACESSONFE>" + Recebimento.Chave + "</CHAVEACESSONFE><CODIGOPRD>" + item.Codigo + "</CODIGOPRD><IDPRD>" + item.Idprd + "</IDPRD><QTDENOTA>" + item.Quantidade + "</QTDENOTA></ZMDVALIDAVEX>";
                ItensEnvio.Add(teste);
                i = i + 1;
            }
            validaVex = validaVex + "</PRJ4872704>";
            Recebimento.Itens = ItensEnvio.ToArray();

            VexPedidos.VexPedidosSoapClient testando = null;
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            try
            {
                testando = new VexPedidos.VexPedidosSoapClient();
                testando.ClientCredentials.UserName.UserName = "Girotrade";
                testando.ClientCredentials.UserName.Password = "@giro2020";

            }
            catch (Exception ex) { }
            string viewData = null;

            try
            {
                //viewData = "Recebimento Cadastrado Com Sucesso";
                viewData = testando.RemessaDeRecebimentoTeste("Girotrade", "@giro2020", Recebimento, "");
            }
            catch (Exception ex) 
            {
                viewData = ex.Message;
            }

            try
            {
                if (viewData.Contains("Recebimento Cadastrado Com Sucesso") || viewData.Contains("Pedido de entrada já existente"))
                {
                    //string retorno = insereMensagem(validaVex, "RMSPRJ4872704Server");
                }
            }
            catch { }
            try
            {
                fLog(Recebimento.Chave.ToString(), Recebimento.SerializeObject());
            }
            catch { }
            try
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    string viewMax = "";
                    if (viewData.Length > 200)
                    {
                        viewMax = viewData.Substring(0, 200);
                    }
                    else { viewMax = viewData; }
                    fLog(Recebimento.Chave.ToString(), "inserindo");
                    SqlCommand command = new SqlCommand("INSERT INTO LOGENVIAVEX(CHAVEACESSO, XML, RETORNO, RECCREATEDON) VALUES('" + Recebimento.Chave.ToString() + "','" + Recebimento.SerializeObject() + "','" + viewMax + "', GETDATE())", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                        fLog(Recebimento.Chave.ToString(), "erroinserirzlog");
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + Recebimento.Chave.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                    }
                }
            }
            catch (Exception ex) { fLog(Recebimento.Chave.ToString(), ex.Message); }
            return viewData;
        }

        private string PopulaVexNovo(dadosVex dados, ElapsedEventArgs e)
        {
            var Recebimento = new VexPedidosNovo.Recebimento();
            var Destinatario = new VexPedidosNovo.Destinatario();
            //var Destinatario = new VexPedidos.Cadastro();
            var Remetente = new VexPedidosNovo.Remetente();
            //var Remetente = new VexPedidos.Cadastro();
            var ItensEnvio = new VexPedidosNovo.ItensRec();
            var ItensEnvioRec = new List<VexPedidosNovo.RecebimentoItens>();
            string validaVex = "";
            validaVex = "<PRJ4872704>";
            Destinatario.CEP = dados.DestCEP;
            Destinatario.CNPJ = dados.DestCNPJ;
            Destinatario.CodigoIBGE = dados.DestCodigoIBGE.ToString();
            Destinatario.Complemento = dados.DestComplemento;
            Destinatario.Endereco = dados.DestEndereco;
            Destinatario.Fantasia = dados.DestFantasia;
            Destinatario.IE = dados.DestIE;
            Destinatario.Numero = dados.DestNumero;
            Destinatario.RazaoSocial = dados.DestRazaoSocial;
            Destinatario.TipoDeEndereco = dados.DestTipoDeEndereco;
            Destinatario.UF = dados.DestUF;
            Remetente.CEP = dados.RemCEP;
            Remetente.CNPJ = dados.RemCNPJ;
            Remetente.CodigoIBGE = dados.RemCodigoIBGE.ToString();
            Remetente.Complemento = dados.RemComplemento;
            Remetente.Endereco = dados.RemEndereco;
            Remetente.Fantasia = dados.RemFantasia;
            Remetente.IE = dados.RemIE;
            Remetente.Numero = dados.RemNumero;
            Remetente.RazaoSocial = dados.RemRazaoSocial;
            Remetente.TipoDeEndereco = dados.RemTipoDeEndereco;
            Remetente.UF = dados.RemUF;
            Recebimento.Destinatario = Destinatario;
            Recebimento.Remetente = Remetente;
            Recebimento.Chave = dados.Chave;
            Recebimento.DataEmissaoNf = dados.DataEmissaoNf;
            Recebimento.NumeroNfe = dados.NumeroNfe;
            Recebimento.Serie = dados.Serie;
            int i = 1;
            foreach (itensVex item in dados.Itens)
            {
                var teste = new VexPedidosNovo.RecebimentoItens();
                try { teste.Codigo = item.Codigo; } catch { teste.Codigo = ""; }
                try { teste.Descricao = item.Descricao; } catch { teste.Descricao = ""; }
                try { teste.Quantidade = item.Quantidade.ToString(); } catch { teste.Quantidade = "0"; }
                try { teste.ValorUnitario = decimal.Parse(item.ValorUnitario); } catch { teste.ValorUnitario = 0; }
                try { teste.CodigoDeBarras = item.CodigoDeBarras; } catch { teste.CodigoDeBarras = ""; }
                try { teste.FatorDeConversao = item.FatorDeConversao; } catch { teste.FatorDeConversao = 1; }
                try { teste.QuantidadeNF = item.QuantidadeNF.ToString(); } catch { teste.QuantidadeNF = "0"; }
                validaVex = validaVex + "<ZMDVALIDAVEX><CHAVEACESSONFE>" + Recebimento.Chave + "</CHAVEACESSONFE><CODIGOPRD>" + item.Codigo + "</CODIGOPRD><IDPRD>" + item.Idprd + "</IDPRD><QTDENOTA>" + item.Quantidade + "</QTDENOTA></ZMDVALIDAVEX>";
                ItensEnvio.Add(teste);
                i = i + 1;
            }
            validaVex = validaVex + "</PRJ4872704>";
            Recebimento.Itens = ItensEnvio;
            VexPedidosNovo.ServiceSoapClient testando = null;
            System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            try
            {
                testando = new VexPedidosNovo.ServiceSoapClient();
                testando.ClientCredentials.UserName.UserName = "wms";
                testando.ClientCredentials.UserName.Password = "wms";
            }
            catch (Exception ex) { }
            string viewData = null;
            
            try
            {
                //viewData = "Recebimento Cadastrado Com Sucesso";
                viewData = testando.RemessaDeRecebimentoTeste("wms", "wms", Recebimento);
            }
            catch (Exception ex)
            {
                viewData = ex.Message;  
            }

            try
            {
                if (viewData.Contains("Recebimento Cadastrado Com Sucesso") || viewData.Contains("Pedido de entrada já existente"))
                {
                    //string retorno = insereMensagem(validaVex, "RMSPRJ4872704Server");
                }
            }
            catch { }
            try
            {
                fLog(Recebimento.Chave.ToString(), Recebimento.SerializeObject());
            }
            catch { }
            try
            {
                using (SqlConnection connection = dbt.GetConnection())
                {
                    string viewMax = "";
                    if (viewData.Length > 200)
                    {
                        viewMax = viewData.Substring(0, 200);
                    }
                    else { viewMax = viewData; }
                    fLog(Recebimento.Chave.ToString(), "inserindo");
                    SqlCommand command = new SqlCommand("INSERT INTO LOGENVIAVEXNOVO(CHAVEACESSO, XML, RETORNO, RECCREATEDON) VALUES('" + Recebimento.Chave.ToString() + "','" + Recebimento.SerializeObject() + "','" + viewMax + "', GETDATE())", connection);
                    try
                    {
                        connection.Open();
                        command.ExecuteReader();
                    }
                    catch (Exception ex2)
                    {
                        fLog(Recebimento.Chave.ToString(), "erroinserirzlog");
                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + Recebimento.Chave.ToString() + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex2.Message.Replace("'", "") + "')", connection);
                        command.ExecuteReader();
                    }
                }
            }
            catch (Exception ex) { fLog(Recebimento.Chave.ToString(), ex.Message); }
            return viewData;
        }
        private string insereMensagem(string mensagem, string dataServerName)
        {

            var dtServerNameMov = dataServerName;
            //var contextMov = $"CODCOLIGADA = 1; CODSISTEMA = T; CODUSUARIO = {lancamento.USUARIO}";
            var strxmlMovAllowance = $"<![CDATA[{mensagem}]]>";

            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            //string dataServerName = "MovMovimentoTBCData";

            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, sCodUsuarioTBC, sSenhaTBC))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(sCodUsuarioTBC, sSenhaTBC);

                    viewData = client.SaveRecord(dtServerNameMov, mensagem, context);
                    //viewData = client.DeleteRecord()
                }
            }
            return viewData;
        }



        private string processarXMLNFE(dadosNfe dados) // dadosNfe dados, ElapsedEventArgs e
        {

            string viewData = "";
            try
            {

                var MOVFATURAMENTOPROCPARAMS = new MOVFATURAMENTOPROCPARAMS();
                var MovItemFatAuto = new List<MOVITEMFATAUTOMATICO>();
                var listaMovItemFatAuto = new LISTAMOVITEMFATAUTOMATICO();
                var listaIdMov = new List<int>();
                Dictionary<int, int> idMovUnico = new Dictionary<int, int>();

                foreach (itensNfe item in dados.Itens)
                {
                    MovItemFatAuto.Add(new MOVITEMFATAUTOMATICO
                    {
                        NSEQITMMOV = item.Nseq,
                        QUANTIDADE = item.QuantidadeBaixar,
                        IDMOV = item.Idmov,
                        CODCOLIGADA = short.Parse(dados.SColigada),
                        CHECKED = 1,
                        IDLOTE = item.IdLote,
                        NUMLOTE = item.NumLote,
                        QUANTIDADE2 = item.QuantidadeBaixar,
                        QUANTIDADEARECEBER = item.QuantidadeBaixar,
                    });
                    if (!idMovUnico.ContainsKey(item.Idmov))
                    {
                        idMovUnico.Add(item.Idmov, item.Idmov);
                        listaIdMov.Add(item.Idmov);
                    }
                }
                MOVFATURAMENTOPROCPARAMS = new MOVFATURAMENTOPROCPARAMS
                {
                    MOVCOPIAFATPAR = new MOVCOPIAFATPAR
                    {
                        CODCOLIGADA = dados.SColigada,
                        CODSISTEMA = "T",
                        CODTMVDESTINO = dados.MovNfe,
                        CODTMVORIGEM = dados.MovNfeOrigem,
                        CODUSUARIO = sCodUsuarioTBC,
                        // GRUPOFATURAMENTO = "",
                        IDEXERCICIOFISCAL = 10,
                        NUMEROMOV = dados.SNumeromov,
                        TIPOFATURAMENTO = 2,
                        DATABASE = DateTime.Now,
                        DATAEMISSAO = DateTime.Parse(dados.SDataEmissao),
                        //DATASAIDA = 
                        EFEITOPEDIDOFATAUTOMATICO = 2,
                        SERIE = dados.Serie,
                        CHAVEACESSONFE = dados.ChaveAcesso,
                        REALIZABAIXAPEDIDO = "true",
                        IDMOV = listaIdMov.ToArray(),
                        LISTAMOVITEMFATAUTOMATICO = listaMovItemFatAuto,
                    }
                };

                listaMovItemFatAuto.MOVITEMFATAUTOMATICO = MovItemFatAuto.ToArray();
                var dtServerNameMov = "MovFaturamentoProc";
                //var contextMov = $"CODCOLIGADA = 1; CODSISTEMA = T; CODUSUARIO = {lancamento.USUARIO}";
                //var strxmlMovAllowance = $"<![CDATA[{movimentoEstoque.SerializeObject()}]]>";
                var strxmlMovAllowance = $"<![CDATA[{MOVFATURAMENTOPROCPARAMS.SerializeObject()}]]>";
                string serverAddress = serverTBC;
                fLog("Teste", strxmlMovAllowance);
                fLog("Teste", serverAddress);
                fLog("Teste", sCodUsuarioTBC);
                fLog("Teste", sSenhaTBC);
                using (Process.IwsProcessClient client = this.CreateClientProcess(serverAddress, sCodUsuarioTBC, sSenhaTBC))
                {
                    using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                    {
                        OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                            CreateBasicAuthorizationMessageProperty(sCodUsuarioTBC, sSenhaTBC);
                        fLog("Teste", "antesdoview");
                        viewData = client.ExecuteWithParams(dtServerNameMov, MOVFATURAMENTOPROCPARAMS.SerializeObject());
                        fLog("Teste", "antesdoview");
                        fLog("Teste", viewData);
                    }
                }


            }
            catch (Exception ex) { fLog("teste", ex.Message); }
            //retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>CTE</CODMEN><DESCRICAO>Historico CTE</DESCRICAO><DESCRICAOMEMO></DESCRICAOMEMO></TMen>");
            return viewData;
        }

        private string processarXMLNFS(dadosNfe dados) // dadosNfe dados, ElapsedEventArgs e
        {
            var MOVFATURAMENTOPROCPARAMS = new MOVFATURAMENTOPROCPARAMS();
            var MovItemFatAuto = new List<MOVITEMFATAUTOMATICO>();
            var listaMovItemFatAuto = new LISTAMOVITEMFATAUTOMATICO();
            var listaIdMov = new List<int>();
            Dictionary<int, int> idMovUnico = new Dictionary<int, int>();

            MovItemFatAuto.Add(new MOVITEMFATAUTOMATICO
            {
                CODCOLIGADA = short.Parse(dados.SColigada),
                CHECKED = 1,
                IDMOV = itensNfs.Idmov,
                NSEQITMMOV = itensNfs.Nseq,
                QUANTIDADE = "1",
            });

            listaIdMov.Clear();
            try
            {
                listaIdMov.Add(itensNfs.Idmov);
            }
            catch { listaIdMov.Add(itensNfs.Idmov); }

            MOVFATURAMENTOPROCPARAMS = new MOVFATURAMENTOPROCPARAMS
            {
                MOVCOPIAFATPAR = new MOVCOPIAFATPAR
                {
                    CODCOLIGADA = dados.SColigada,
                    CODSISTEMA = "T",
                    CODTMVORIGEM = tmvorignfs,
                    CODTMVDESTINO = tmvdestnfs,
                    CODUSUARIO = sCodUsuarioTBC,
                    // GRUPOFATURAMENTO = "",
                    IDEXERCICIOFISCAL = 16,
                    NUMEROMOV = dados.SNumeromov,
                    TIPOFATURAMENTO = 1,
                    DATABASE = DateTime.Now,
                    DATAEMISSAO = DateTime.Parse(dados.SDataEmissao),
                    //DATASAIDA = 
                    EFEITOPEDIDOFATAUTOMATICO = 1,
                    SERIE = dados.Serie,
                    CHAVEACESSONFE = dados.ChaveAcesso,
                    REALIZABAIXAPEDIDO = "true",
                    IDMOV = listaIdMov.ToArray(),
                    LISTAMOVITEMFATAUTOMATICO = listaMovItemFatAuto,
                }
            };

            listaMovItemFatAuto.MOVITEMFATAUTOMATICO = MovItemFatAuto.ToArray();
            var dtServerNameMov = "MovFaturamentoProc";
            //var contextMov = $"CODCOLIGADA = 1; CODSISTEMA = T; CODUSUARIO = {lancamento.USUARIO}";
            //var strxmlMovAllowance = $"<![CDATA[{movimentoEstoque.SerializeObject()}]]>";
            var strxmlMovAllowance = $"<![CDATA[{MOVFATURAMENTOPROCPARAMS.SerializeObject()}]]>";
            string serverAddress = "http://crater/TOTVSBusinessConnect/";
            string viewData = null;

            using (Process.IwsProcessClient client = this.CreateClientProcess(serverAddress, sCodUsuarioTBC, sSenhaTBC))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(sCodUsuarioTBC, sSenhaTBC);

                    viewData = client.ExecuteWithParams(dtServerNameMov, MOVFATURAMENTOPROCPARAMS.SerializeObject());
                }
            }
            //retorno = insereMensagem("<TMen><CODCOLIGADA>" + dados.SColigada + "</CODCOLIGADA><CODMEN>CTE</CODMEN><DESCRICAO>Historico CTE</DESCRICAO><DESCRICAOMEMO></DESCRICAOMEMO></TMen>");
            return viewData;
        }
        private bool fvalXmlProcessadoTransfNfe(string arquivoName, string sColigada, string sChaveNfe, string dataServerName, string user, string senha)
        {
            fLog(arquivoName, "Busca XML Transferido" + dataServerName);
            bool bVal = false;
            string filter;
            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            filter = "CHAVEACESSONFE = '" + sChaveNfe + "' AND TMOV.CODCOLIGADA = " + sColigada;
            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, user, senha))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(user, senha);

                    viewData = client.ReadView(dataServerName, filter, context);
                }
            }
            if (viewData.Contains("<TMOV>"))
            {
                bVal = true;
            }
            else
            {
                bVal = false;
            }
            return bVal;

        }

        private bool fvalXmlProcessadoTransf(string arquivoName, string sColigada, string sChaveNfe, string dataServerName, string user, string senha)
        {
            return false;
            fLog(arquivoName, "Busca XML Transferido" + dataServerName);
            bool bVal = false;
            string filter;
            string serverAddress = serverTBC;
            string context = "Codusuario=svcjoins_xml;CodColigada=17;Codsistema=G";
            string viewData = null;
            filter = "CHAVEACESSONFE = '" + sChaveNfe + "' AND TMOV.CODCOLIGADA = " + sColigada;
            using (DataServer.IwsDataServerClient client = this.CreateClient(serverAddress, user, senha))
            {
                using (OperationContextScope scope = new OperationContextScope(client.InnerChannel))
                {
                    OperationContext.Current.OutgoingMessageProperties[HttpRequestMessageProperty.Name] =
                        CreateBasicAuthorizationMessageProperty(user, senha);

                    viewData = client.ReadView(dataServerName, filter, context);
                }
            }
            if (viewData.Contains("<TMOV>"))
            {
                bVal = true;
            }
            else
            {
                bVal = false;
            }
            return bVal;

        }

        private string RemoverAcentos(string valor, string arq)
        {
            if (bAtivaLog)
            {
                fLog(arq, "Função RemoverAcentos");
            }

            string TextoNormalizado = valor.Normalize(NormalizationForm.FormD);
            StringBuilder sbTexto = new StringBuilder();
            for (int i = 0; i < TextoNormalizado.Length; i++)
            {
                char C = TextoNormalizado[i];
                //If C = "'" Then
                //C = "`"
                //End If
                if (!C.Equals("'"))
                {
                    if (!C.Equals("'"))
                    {
                        if (CharUnicodeInfo.GetUnicodeCategory(C) != UnicodeCategory.NonSpacingMark)
                        {
                            sbTexto.Append(C);
                        }
                    }
                }
            }
            return sbTexto.ToString();
        }

        private void fLog(string sXMLName, string sXMLDesc)
        {
            if (this.bAtivaLog)
            {
                AppSettingsReader configurationAppSettings = new AppSettingsReader();
                //Variáveis para Log
                string dDataAtual = DateTime.Now.ToString("yyyyMMdd");
                string sPath = ConfigurationManager.AppSettings["CaminhoLog"];
                //Dim sPathLog As FileInfo = New FileInfo("C:\InspetorXML\Log\" & dDataAtual & "InspetorXML.log")
                const string V = @"\InspetorXML";
                FileInfo sPathLog = new FileInfo(sPath + V + dDataAtual + ".log");
                StreamWriter sw;

                //Verifica se existe caminho do log
                if (!System.IO.Directory.Exists(sPath))
                {
                    System.IO.Directory.CreateDirectory(sPath);
                }

                //Se o arquivo de log foi criado, salva variaveis da função no arquivo
                if (sPathLog.Exists == true)
                {
                    sw = sPathLog.AppendText();
                }
                else
                {
                    sw = sPathLog.CreateText();
                }


                sw.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " --- " + sXMLDesc + " --- " + sXMLName);
                sw.Flush();
                sw.Close();
            }
        }

        private void CarregaApp(ElapsedEventArgs e)
        {
            sEnvioHoraDiario1 = ConfigurationManager.AppSettings["HoraEnvioDiario1"];
            sEnvioHoraDiario2 = ConfigurationManager.AppSettings["HoraEnvioDiario2"];
            sEnvioHoraDiario3 = ConfigurationManager.AppSettings["HoraEnvioDiario3"];
            sEnvioHoraDiario4 = ConfigurationManager.AppSettings["HoraEnvioDiario4"];
            caminho = ConfigurationManager.AppSettings["RepositorioXML"];
            sProcessado = ConfigurationManager.AppSettings["ProcessadosXML"];
            sManual = ConfigurationManager.AppSettings["ManualXML"];
            sCriticados = ConfigurationManager.AppSettings["CriticadosXML"];
            sIgnorada = ConfigurationManager.AppSettings["IgnoraXML"];
            sLogo = ConfigurationManager.AppSettings["Logo"];
            sCodUsuario = ConfigurationManager.AppSettings["User"];
            sCodUsuarioTBC = ConfigurationManager.AppSettings["UserTBC"];
            sSenhaTBC = ConfigurationManager.AppSettings["SenhaTBC"];
            serverTBC = ConfigurationManager.AppSettings["ServidorTBC"];
            asmxTBC = ConfigurationManager.AppSettings["asmxTBC"];
            asmxProcessTBC = ConfigurationManager.AppSettings["asmxProcessTBC"];
            asmxConsultaTBC = ConfigurationManager.AppSettings["asmxConsultaTBC"];

        }

        private bool novoLeItensNfTransf(string caminho, FileInfo arq, XmlDocument xmlDoc, ElapsedEventArgs e, dadosNfe dadosNf, dadosVex dadosV, dadosSenior dadosS)
        {
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXMLTRANSF(NOLOCK) WHERE FLAG_STATUS = 'I' AND NOME_XML = '" + arq.Name + "'", connection);
                try
                {
                    connection.Open();
                    SqlDataReader drItens = command.ExecuteReader();
                    if (drItens.HasRows) { }
                    drItens.Close();
                }
                catch (Exception exItens)
                {
                    fLog(arq.Name, "aqui3");
                    command.CommandText = "INSERT INTO ZERROSAPLICXMLTRASNF (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')";
                    command.ExecuteReader();
                    this.oReader.Close();
                    throw new Exception(exItens.Message);
                }
            }

            nfeProc nfe = DeserializeFromXml<nfeProc>("C:\\Users\\lucas\\Downloads\\31230366288002000136550010001979731966463964-procNFe-44.xml");

            return false;
        }

        public static T DeserializeFromXml<T>(string xmlFilePath)
        {
            XmlSerializer serializer = new XmlSerializer(typeof(T));
            using (StreamReader reader = new StreamReader(xmlFilePath))
            {
                return (T)serializer.Deserialize(reader);
            }
        }

        private bool LeItensNfe(string caminho, FileInfo arq, XmlDocument xmlDoc, ElapsedEventArgs e, dadosNfe dadosNf, dadosVex dadosV, dadosSenior dadosS)
        {
            if (this.bAtivaLog)
            {
                fLog(arq.Name, "Função LeItensNfe");
            }
            XmlNamespaceManager ns = new XmlNamespaceManager(xmlDoc.NameTable);
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe");
            XPathNavigator xpathNav = xmlDoc.CreateNavigator();
            XPathNavigator node;
            DataSet dataDados;
            DataTable dt;
            string expression;
            string fatorRm = "";
            string fatorVex = "";
            string unVex = "";
            string ean = "";
            string sort;
            DataRow[] foundRows;
            DataRow linha;
            int stNDestacado;
            int prioridade;
            string coligadaIdprd = "";
            string movimentoAux = "";
            prioridade = -1;
            stNDestacado = 0;
            int i = 0;
            bool bSai = false;
            string sCodProd;
            string sNomeProd;
            string sUnd;
            bool bVal = false;
            string sIcms, tipoPrd;
            string pedidoXml = "";
            int associado = 0;
            tipoPrd = "";
            Dictionary <string, string> idPrdUnico = new Dictionary <string, string>();
            //verifica se não esta criticado
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand("SELECT DISTINCT (FLAG_STATUS) FROM ZCRITICAXML(NOLOCK) WHERE FLAG_STATUS = 'I' AND NOME_XML = '" + arq.Name + "'", connection);
                try
                {
                    connection.Open();
                    SqlDataReader drItens = command.ExecuteReader();
                    if (drItens.HasRows) { }
                    drItens.Close();
                }
                catch (Exception exItens)
                {
                    fLog(arq.Name, "aqui3");
                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')";
                    command.ExecuteReader();
                    this.oReader.Close();
                    throw new Exception(exItens.Message);
                }
            }
            var listaItem = new List<itensNfe>();
            var listaItemVex = new List<itensVex>();
            var listaItemSenior = new List<itensSenior>();
            string sCompCfop;
            while (!bSai)
            {
                itensNfe item = new itensNfe();
                itensVex itemV = new itensVex();
                itensSenior itemS = new itensSenior();
                i = i + 1;
                itemS.Tipo_item = "E";
                itemS.Tipoproduto_item = "P";
                itemS.Idseq_item = i;
                itemS.Codigointerno_item = dadosS.CodigoInterno;
                itemS.Numpedido_item = dadosS.NumPedido;
                itemS.Cnpj_depositante_item = dadosS.Cnpj_Depositante;
                itemS.Cnpj_emitente_item = dadosS.Cnpj_Emitente;
                dadosS.Tipo = "E";
                dadosS.Pessoa_Dest = "J";
                dadosS.Gerafinceiro = "N";
                dadosS.Tipodocumento = "T";
                dadosS.Tipocarga = "Secos";
                dadosS.Tiponf = "N";
                dadosS.Estado = "N";
                dadosS.Codigo_Dest = "000165";


                node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:xProd", ns);
                if (node == null)
                {
                    bSai = true;
                    i = i - 1;
                }
                else
                {
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:CFOP", ns);
                    sCompCfop = node.InnerXml.ToString();
                    item.NseqNota = i;
                    if(i == 1)
                    {
                        dadosS.Cfop = sCompCfop;
                    }

                    if (dadosNf.Crt == 1)
                    {
                        sIcms = FPegaIcmsSn(xmlDoc, i, arq);
                    }
                    else if (dadosNf.Crt == 2 || dadosNf.Crt == 3)
                    {
                        sIcms = FPegaIcms(xmlDoc, i, arq);
                    }
                    else
                    { sIcms = ""; }
                    if (dadosNf.Crt == 1)
                    {
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:CSOSN", ns);
                    }
                    else
                    {
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:CST", ns);
                    }
                    if (node != null)
                    {
                        item.CstIcms = node.InnerXml.ToString();
                    }
                    else
                    {
                        item.CstIcms = "";
                    }
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:cProd", ns);
                    sCodProd = node.InnerXml.ToString();
                    item.CodProduto = sCodProd;
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:xProd", ns);
                    sNomeProd = node.InnerXml.ToString();
                    sNomeProd = sNomeProd.Replace("'", "");
                    item.NomeProduto = sNomeProd;
                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:uCom", ns);
                    sUnd = node.InnerXml.ToString();
                    item.UnidadeNota = sUnd;
                    try
                    {
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:xPed", ns);
                        if (node != null)
                        {
                            pedidoXml = node.InnerXml.ToString();
                        }
                        else
                        {
                            pedidoXml = "";
                        }
                    }
                    catch { pedidoXml = ""; }
                    //busca pedido vinculado caso não encontre, pega as tags de xped e nitemped

                    if (lote)
                    {

                        try // Início do Lote
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:rastro/nfe:nLote", ns);
                            item.NumLote = node.InnerXml.ToString();
                        }
                        catch { }
                        try
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:rastro/nfe:dFab", ns);
                            item.FabLote = node.InnerXml.ToString();
                        }
                        catch { }
                        try
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:rastro/nfe:dVal", ns);
                            item.ValLote = node.InnerXml.ToString();
                        }
                        catch { }  // Fim do Lote
                    }
                    //TAG PRODUTO E VALOR
                    try
                    {
                        try
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:cEAN", ns);
                            ean = node.InnerXml.ToString();
                        }
                        catch { }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:vUnCom", ns);
                        if (node != null)
                        {
                            item.ValorProduto = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.ValorProduto = "0";
                        }
                        itemV.ValorUnitario = item.ValorProduto;
                        itemS.Vlrunit_item = item.ValorProduto;

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:vProd", ns);
                        if (node != null)
                        {
                            itemS.Vlrtotal_item = node.InnerXml.ToString().Replace(".", ",");
                            itemS.Totalliquido_item = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            itemS.Vlrtotal_item = "0";
                            itemS.Totalliquido_item  = "0";
                        }
                       
                    }

                    catch { }
                    //IMPOSTO
                    try
                    {
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:pCredSN", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:pICMS", ns);
                        }
                        if (node != null)
                        {
                            item.AliqIcms = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.AliqIcms = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:vCredICMSSN", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:vICMS", ns);
                        }
                        if (node != null)
                        {
                            item.ValorIcms = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.ValorIcms = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:vBC", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:vBC", ns);
                        }
                        if (node != null)
                        {
                            item.BaseIcms = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.BaseIcms = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:pRedBC", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:pRedBC", ns);
                        }
                        if (node != null)
                        {
                            string aux = "";
                            aux = node.InnerXml.ToString().Replace(".", ",");
                            int z = 0;
                            foreach (char a in aux)
                            {
                                if (a == ',')
                                {
                                    z = 97;
                                }
                                if (z < 100)
                                {
                                    item.BaseRed = item.BaseRed + a;
                                }
                                z = z + 1;
                            }
                        }
                        else
                        {
                            item.BaseRed = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:pRedBCST", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:pRedBCST", ns);
                        }
                        if (node != null)
                        {
                            string aux = "";
                            aux = node.InnerXml.ToString().Replace(".", ",");
                            int z = 0;
                            foreach (char a in aux)
                            {
                                if (a == ',')
                                {
                                    z = 97;
                                }
                                if (z < 100)
                                {
                                    item.BaseRedIcmsST = item.BaseRedIcmsST + a;
                                }
                                z = z + 1;
                            }
                        }
                        else
                        {
                            item.BaseRedIcmsST = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:pMVAST", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:pMVAST", ns);
                        }
                        if (node != null)
                        {
                            item.Mva = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.Mva = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:vBCST", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:vBCST", ns);
                        }
                        if (node != null)
                        {
                            item.BaseIcmsSt = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.BaseIcmsSt = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:vICMSST", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:vICMSST", ns);
                        }
                        if (node != null)
                        {
                            try
                            {
                                item.ValorIcmsSt = node.InnerXml.ToString().Replace(".", ",");
                                double valor;
                                valor = double.Parse(item.ValorIcmsSt.ToString()) + double.Parse(item.ValorIcms.ToString());
                                item.ValorIcmsSt = valor.ToString().Replace(".", ",");
                            }
                            catch { }
                        }
                        else
                        {
                            item.ValorIcmsSt = "0";
                        }
                        if (dadosNf.Crt == 1)
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN" + sIcms + "/nfe:pICMSST", ns);
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:ICMS/nfe:ICMS" + sIcms + "/nfe:pICMSST", ns);
                        }
                        if (node != null)
                        {
                            item.AliqIcmsSt = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.AliqIcmsSt = "0";
                        }
                        switch (item.CstIcms)
                        {
                            case "101":
                                item.CstIcms = "00";
                                break;
                            case "102":
                                item.CstIcms = "41";
                                break;
                            case "103":
                                item.CstIcms = "40";
                                break;
                            case "201":
                                item.CstIcms = "10";
                                break;
                            case "202":
                                item.CstIcms = "30";
                                break;
                            case "203":
                                item.CstIcms = "30";
                                break;
                            case "300":
                                item.CstIcms = "40";
                                break;
                            case "400":
                                item.CstIcms = "41";
                                break;
                            case "500":
                                item.CstIcms = "60";
                                break;
                            case "900":
                                item.CstIcms = "90";
                                break;
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:vBC", ns);
                        if (node != null)
                        {
                            item.BaseIpi = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.BaseIpi = "0";
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:pIPI", ns);
                        if (node != null)
                        {
                            item.AliqIpi = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.AliqIpi = "0";
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:IPI/nfe:IPITrib/nfe:vIPI", ns);
                        if (node != null)
                        {
                            item.ValorIpi = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.ValorIpi = "0";
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:CST", ns);
                        if (node != null)
                        {
                            item.CstCofins = node.InnerXml.ToString();
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:COFINS/nfe:COFINSNT/nfe:CST", ns);
                        }
                        if (node != null)
                        {
                            item.CstCofins = node.InnerXml.ToString();
                        }

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:pCOFINS", ns);
                        if (node != null)
                        {
                            item.AliqCofins = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.AliqCofins = "0";
                        }

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vBC", ns);
                        if (node != null)
                        {
                            item.BaseCofins = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.BaseCofins = "0";
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:COFINS/nfe:COFINSAliq/nfe:vCOFINS", ns);
                        if (node != null)
                        {
                            item.ValorCofins = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.ValorCofins = "0";
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:CST", ns);
                        if (node != null)
                        {
                            item.CstPis = node.InnerXml.ToString();
                        }
                        else
                        {
                            node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:PIS/nfe:PISNT/nfe:CST", ns);
                        }
                        if (node != null)
                        {
                            item.CstPis = node.InnerXml.ToString();
                        }

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:pPIS", ns);
                        if (node != null)
                        {
                            item.AliqPis = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.AliqPis = "0";
                        }

                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vBC", ns);
                        if (node != null)
                        {
                            item.BasePis = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.BasePis = "0";
                        }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:imposto/nfe:PIS/nfe:PISAliq/nfe:vPIS", ns);
                        if (node != null)
                        {
                            item.ValorPis = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else
                        {
                            item.ValorPis = "0";
                        }
                    }
                    catch { }
                    //FIM IMPOSTO
                    //valores adicionais
                    try
                    {
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:vFrete", ns);
                        if (node != null)
                        {
                            item.VFrete = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else { item.VFrete = "0"; }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:vSeg", ns);
                        if (node != null)
                        {
                            item.VSeguro = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else { item.VSeguro = "0"; }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:vDesc", ns);
                        if (node != null)
                        {
                            item.VDesconto = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else { item.VDesconto = "0"; }
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:vOutro", ns);
                        if (node != null)
                        {
                            item.VOutros = node.InnerXml.ToString().Replace(".", ",");
                        }
                        else { item.VOutros = "0"; }
                    }
                    catch { }
                    //fim valores adicionais
                    //quantidade
                    try
                    {
                        node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:qCom", ns);
                        item.Quantidade = node.InnerXml.ToString().Replace(".", ",");     //Quantidade
                    }
                    catch { }
                    try
                    {
                        double auxQuant = double.Parse(item.Quantidade);
                        itemV.Quantidade = Convert.ToInt32(Math.Floor(auxQuant));
                        itemS.Qtde_item = Convert.ToInt32(Math.Floor(auxQuant));
                    }
                    catch
                    {
                        itemV.Quantidade = 0;
                        itemS.Qtde_item = 0;
                    }
                    try
                    {
                        itemV.QuantidadeNF = itemV.Quantidade;
                    }
                    catch { itemV.QuantidadeNF = 0; }
                    try
                    {
                        qtdeCritica = item.Quantidade.Replace(",", ".");
                    }
                    catch { qtdeCritica = "0"; }
                    //BUSCA O PRODUTO
                    try
                    {
                        //busca pelo ean
                        //busca idprd  dataDados = new DataSet();
                        dataDados = new DataSet();
                        dataDados = buscaDadosConsulta(arq.Name, "COD=" + ean, sCodUsuarioTBC, sSenhaTBC, "T", "JOINSEAN");
                        if (dataDados.Tables.Count > 0)
                        {
                            dt = dataDados.Tables[0];
                            foundRows = dt.Select("");
                            for (int j = 0; j < foundRows.Length; j++)
                            {
                                item.Idprd = foundRows[j]["IDPRD"].ToString();
                                coligadaIdprd = foundRows[j]["CODCOLIGADA"].ToString();
                                item.CodigoPrd = foundRows[j]["CODIGOPRD"].ToString();
                            }
                            if (item.Idprd != "" && !string.IsNullOrEmpty(item.Idprd) && enviarVex)
                            {
                                dataDados = buscaDadosRecord(arq.Name, coligadaIdprd + ";" + item.Idprd, "EstPrdDataBR", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables["TPRODUTO"];
                                    expression = "IDPRD = '" + item.Idprd + "'";
                                    fLog(arq.Name, expression);
                                    sort = "IDPRD DESC";

                                    //Use the Select method to find all rows matching the filter.F
                                    foundRows = dt.Select(expression, sort);
                                    for (int j = 0; j < foundRows.Length; j++)
                                    {
                                        try { itemV.Codigo = foundRows[j]["CODIGOPRD"].ToString(); } catch { }
                                        try { itemV.Descricao = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }
                                        try { itemV.Idprd = item.Idprd; } catch { }
                                        try { itemS.Codigoindustria_item = foundRows[j]["CODIGOPRD"].ToString(); } catch { }
                                        try { itemS.Codindustria_item = foundRows[j]["CODIGOPRD"].ToString(); } catch { }
                                        try { itemS.Descrprod_item = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }
                                        try { itemS.Descr_prod_item = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }



                                    }
                                    if (!idPrdUnico.ContainsKey(itemS.Codigoindustria_item))
                                    {
                                        idPrdUnico.Add(itemS.Codigoindustria_item, itemS.Codigoindustria_item);
                                       
                                    }
                                   
                                }

                            }
                        }
                    }
                    catch { }
                    //CASO NÃO ENCONTRE PELO EAN IRA BUSCAR PELO DE PARA
                    if (item.Idprd == "" || string.IsNullOrEmpty(item.Idprd))
                    {
                        dataDados = new DataSet();
                        dataDados = buscaDados(arq.Name, "CODNOFORN = '" + sCodProd + "' AND FCFO.CODCFO = '" + dadosNf.SCodCfoEmi + "' AND TPRDCFO.CODCOLCFO =  " + dadosNf.SColCfoEmi + " AND TPRDCFO.ATIVO = 1", "EstPrdCfoDataBR", sCodUsuarioTBC, sSenhaTBC);
                        if (dataDados.Tables.Count > 0)
                        {
                            dt = dataDados.Tables[0];
                            expression = "CODNOFORN = '" + sCodProd + "'";
                            fLog(arq.Name, expression);
                            sort = "CODCOLCFO ASC";
                            // Use the Select method to find all rows matching the filter.
                            foundRows = dt.Select(expression, sort);
                            for (int j = 0; j < foundRows.Length; j++)
                            {
                                item.Idprd = foundRows[j]["IDPRD"].ToString();
                                coligadaIdprd = foundRows[j]["CODCOLIGADA"].ToString();
                            }
                            if (item.Idprd != "" && !string.IsNullOrEmpty(item.Idprd) && enviarVex)
                            {
                                dataDados = buscaDadosRecord(arq.Name, coligadaIdprd + ";" + item.Idprd, "EstPrdDataBR", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables["TPRODUTO"];
                                    expression = "IDPRD = '" + item.Idprd + "'";
                                    fLog(arq.Name, expression);
                                    sort = "IDPRD DESC";

                                    //Use the Select method to find all rows matching the filter.F
                                    foundRows = dt.Select(expression, sort);
                                    for (int j = 0; j < foundRows.Length; j++)
                                    {
                                        try { itemV.Codigo = foundRows[j]["CODIGOPRD"].ToString(); } catch { }
                                        try { itemV.Descricao = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }
                                        try { item.CodigoPrd = foundRows[j]["CODIGOPRD"].ToString(); } catch { }
                                        try { itemV.Idprd = item.Idprd; } catch { }
                                        try { itemS.Codigoindustria_item = foundRows[j]["CODIGOPRD"].ToString(); } catch { }
                                        try { itemS.Descrprod_item = foundRows[j]["NOMEFANTASIA"].ToString().Replace("'", ""); } catch { }

                                    }
                                }

                            }
                        }
                    }
                    //CASO NÃO ENCONTRE, CRITICA A FALTA DE CADASTRO
                    if (item.Idprd == "" || string.IsNullOrEmpty(item.Idprd))
                    {
                        enviavex = false;
                        enviavexnovo = false;
                        bVal = true;
                        using (SqlConnection connection = dbt.GetConnection())
                        {
                            SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO,TIPO_DOC, EAN, CHAVEACESSO, CODPRD) VALUES " +
                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'CMP', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'NÃO ENCONTRADO VÍNCULO DO CÓDIGO DO PRODUTO DO FORNECEDOR COM O PRODUTO DO RM', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "','" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);

                            try
                            {
                                connection.Open();
                                command.ExecuteReader();
                            }
                            catch (Exception exItens)
                            {
                                fLog(arq.Name, "aquiNOVO4");
                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                command.ExecuteReader();
                                oReader.Close();
                                throw new Exception(exItens.Message);
                            }

                        }
                    }
                    //ENCONTROU O PRODUTO
                    if (item.Idprd != "" && !string.IsNullOrEmpty(item.Idprd))
                    {
                        //BUSCA O TIPOPRD
                        dataDados = new DataSet();
                        dataDados = buscaDados(arq.Name, "TPRODUTO.IDPRD = " + item.Idprd, "EstPrdDataBr", sCodUsuarioTBC, sSenhaTBC);
                        if (dataDados.Tables.Count > 0)
                        {
                            dt = dataDados.Tables[0];


                            expression = "IDPRD = " + item.Idprd;
                            fLog(arq.Name, expression);
                            sort = "IDPRD ASC";

                            foundRows = dt.Select(expression, sort);
                            for (int j = 0; j < foundRows.Length; j++)
                            {
                                try { tipoPrd = foundRows[j]["CODTB1FAT"].ToString(); } catch { tipoPrd = ""; }

                                try { item.TipoPrdFiscal = foundRows[j]["IDPRDFISCALE"].ToString(); } catch { item.TipoPrdFiscal = ""; }
                            }
                        }
                        //CASO NÃO ENCONTRE, CRITICA
                        if (tipoPrd == "" || string.IsNullOrEmpty(tipoPrd))
                        {
                            bVal = true;
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, CODPRD, CHAVEACESSO) VALUES " +
                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'CMP', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'PRODUTO SEM TIPO DEFINIDO. IDPRD: " + item.Idprd + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "','" + item.CodigoPrd + "','" + dadosNf.ChaveAcesso + "')", connection);

                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception exItens)
                                {
                                    fLog(arq.Name, "aqui5");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                    throw new Exception(exItens.Message);
                                }

                            }
                        }
                        //busca unidade padrão 1 BUSCA TABELA RM, 0 BUSCA TABELA TELA DE CRITICA (ESTA FIXO 0)
                        if (buscaUnidade == 1)
                        {
                            dataDados = new DataSet();
                            dataDados = buscaDados(arq.Name, "CODUNDCFO = '" + sUnd + "' AND FCFO.CODCFO = '" + dadosNf.SCodCfoEmi + "' AND TUNDCFOCOLAB.CODCOLCFO =  " + dadosNf.SColCfoEmi, "MovUndCfocolabData", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables[0];


                                expression = "CODUNDCFO = '" + sUnd + "'";
                                fLog(arq.Name, expression);
                                sort = "CODCOLCFO ASC";


                                // Use the Select method to find all rows matching the filter.
                                foundRows = dt.Select(expression, sort);
                                for (int j = 0; j < foundRows.Length; j++)
                                {
                                    item.CodUnd = foundRows[j]["CODUND"].ToString();
                                }
                            }
                        }
                        else //busca unidade depara
                        {
                            //BUSCA UNIDADE
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("SELECT UNRM,UNVEX FROM ZUNIDADE(NOLOCK) WHERE COLCFO = " + dadosNf.SColCfoEmi + " AND CODCFO = '" + dadosNf.SCodCfoEmi + "' AND IDPRD = " + item.Idprd + " AND UNCFO = '" + sUnd + "' AND CODCOLIGADA = " + dadosNf.SColigada, connection);
                                try
                                {
                                    connection.Open();
                                    SqlDataReader dr;
                                    dr = command.ExecuteReader();
                                    if (dr.HasRows)
                                    {
                                        while (dr.Read())
                                        {
                                            item.CodUnd = dr[0].ToString();
                                            unVex = dr[1].ToString();
                                        }
                                    }
                                    dr.Close();

                                }
                                catch (Exception ex)
                                {
                                    fLog(arq.Name, "aqui8");
                                    command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                    command.ExecuteReader();
                                    this.oReader.Close();
                                }
                            }
                        }
                        //não encontrando a unidade no de para, critica.
                        if (item.CodUnd == "" || string.IsNullOrEmpty(item.CodUnd))
                        {
                            bVal = true;
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                string mensagem;
                                if (buscaUnidade == 1)
                                {
                                    mensagem = "UNIDADE DE MEDIDA NAO CADASTRADA NA TABELA DE DE->PARA NO ANEXO DO CADASTRO DAS UNIDADE (ANEXOS-> UNIDADES PARA IMPORTAÇÃO DE XML)";
                                }
                                else
                                {
                                    mensagem = "UNIDADE DE MEDIDA NAO CADASTRADA NA TABELA DE DE->PARA NA TELA DE CRITICA PARA: IDPRD: " + item.Idprd + " CODIGO FORNECEDOR: " + dadosNf.SCodCfoEmi + " Unidade Fornecedor: " + sUnd;
                                }
                                //verificar;
                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, CHAVEACESSO, COD_PRD, CODPRD) VALUES " +
                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'CMP', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "','" + mensagem + "' , 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "','" + dadosNf.ChaveAcesso + "'," + item.Idprd + ",'" + item.CodigoPrd + "')", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception exItens)
                                {
                                    fLog(arq.Name, "aqui9");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                    throw new Exception(exItens.Message);
                                }
                            }
                        }
                        else if (unVex == "" || string.IsNullOrEmpty(unVex))
                        {
                            enviavex = false;
                            enviavexnovo = false;
                            bVal = true;
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                string mensagem;
                                if (buscaUnidade == 1)
                                {
                                    mensagem = "UNIDADE DE MEDIDA ENVIO VEX NAO CADASTRADA NA TABELA DE DE->PARA NO ANEXO DO CADASTRO DAS UNIDADE (ANEXOS-> UNIDADES PARA IMPORTAÇÃO DE XML)";
                                }
                                else
                                {
                                    mensagem = "UNIDADE DE MEDIDA ENVIO VEX NAO CADASTRADA NA TABELA DE DE->PARA NA TELA DE CRITICA PARA: IDPRD: " + item.Idprd + " CODIGO FORNECEDOR: " + dadosNf.SCodCfoEmi + " Unidade Fornecedor: " + sUnd;
                                }
                                //verificar;
                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, CHAVEACESSO, CODPRD) VALUES " +
                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'CMP', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "','" + mensagem + "' , 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception exItens)
                                {
                                    fLog(arq.Name, "aqui10");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                    throw new Exception(exItens.Message);
                                }
                            }
                        }
                        //CASO TENHA ENCONTRADO AS UNIDADE 
                        if (item.CodUnd != "" && !string.IsNullOrEmpty(item.CodUnd) && enviarVex && unVex != "" && !string.IsNullOrEmpty(unVex))
                        {
                            //Busca codigo de barras para vex
                            dataDados = new DataSet();
                            dataDados = buscaDadosRecord(arq.Name, coligadaIdprd + ";" + item.Idprd, "EstPrdDataBR", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables["TPRDCODIGO"];
                                expression = "CODUND = '" + unVex + "'";
                                fLog(arq.Name, expression);
                                sort = "CODIGO DESC";

                                //Use the Select method to find all rows matching the filter.F
                                foundRows = dt.Select(expression, sort);
                                for (int j = 0; j < foundRows.Length; j++)
                                {
                                    try { itemV.CodigoDeBarras = foundRows[j]["CODIGO"].ToString(); } catch { }
                                    try { itemS.Barra_item = foundRows[j]["CODIGO"].ToString(); } catch { }
                                }
                            }
                            //busca fator conversao
                            dataDados = buscaDadosRecord(arq.Name, item.CodUnd, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables["TUND"];
                                expression = "CODUND = '" + item.CodUnd + "'";
                                fLog(arq.Name, expression);
                                sort = "CODUND DESC";

                                //Use the Select method to find all rows matching the filter.F
                                foundRows = dt.Select(expression, sort);
                                for (int j = 0; j < foundRows.Length; j++)
                                {
                                    item.FatorNf = foundRows[j]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                }
                                try
                                {
                                    double auxQuantidadeEstoque = 0;
                                    double auxFator = double.Parse(item.FatorNf);
                                    double auxQuant = double.Parse(item.Quantidade);
                                    auxQuantidadeEstoque = auxFator * auxQuant;
                                    item.QuantidadeEstoqueNota = auxQuantidadeEstoque;
                                    double auxFatorNf = double.Parse(item.FatorNf);
                                    item.PrecoUnitario = Math.Round(double.Parse(item.ValorProduto) / auxFatorNf, 2);
                                }
                                catch { }
                            }
                            //busca fatorvex
                            dataDados = buscaDadosRecord(arq.Name, unVex, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables["TUND"];
                                expression = "CODUND = '" + unVex + "'";
                                fLog(arq.Name, expression);
                                sort = "CODUND DESC";

                                //Use the Select method to find all rows matching the filter.F
                                foundRows = dt.Select(expression, sort);
                                for (int j = 0; j < foundRows.Length; j++)
                                {
                                    fatorVex = foundRows[j]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                    double auxQuant = double.Parse(item.FatorNf);
                                    double auxQuantVex = double.Parse(fatorVex);
                                    double fatorVexAux = 0;
                                    try { fatorVexAux = auxQuant / auxQuantVex; } catch { fatorVexAux = 1; }
                                    try { fatorVexAux = Convert.ToInt32(Math.Floor(fatorVexAux)); } catch { fatorVexAux = 1; }
                                    if (fatorVexAux < 1)
                                    {
                                        fatorVexAux = 1;
                                    }
                                    try { itemV.FatorDeConversao = Convert.ToInt32(Math.Floor(fatorVexAux)); } catch { itemV.FatorDeConversao = 1; }
                                    try { itemV.Quantidade = itemV.Quantidade * Convert.ToInt32(Math.Floor(auxQuant)) / Convert.ToInt32(Math.Floor(auxQuantVex)); } catch { }
                                    try { itemV.QuantidadeNF = itemV.Quantidade / itemV.FatorDeConversao; } catch { itemV.QuantidadeNF = itemV.Quantidade; }
                                    try { itemS.Qtde_item = itemS.Qtde_item * Convert.ToInt32(Math.Floor(auxQuant)); } catch { itemS.Qtde_item = itemS.Qtde_item; }

                                }
                            }
                            //não encontrou codigo de barras, critica
                            if (itemV.CodigoDeBarras == "" || string.IsNullOrEmpty(itemV.CodigoDeBarras))
                            {
                                enviavex = false;
                                enviavexnovo = false;
                                bVal = true;
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO,TIPO_DOC, EAN, COD_PRD, CHAVEACESSO, CODPRD) VALUES " +
                                    "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'CMP', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'NÃO ENCONTRADO CÓDIGO DE BARRAS COM A UNIDADE " + unVex + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                    }
                                    catch (Exception exItens)
                                    {
                                        fLog(arq.Name, "aqui11");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        oReader.Close();
                                        throw new Exception(exItens.Message);
                                    }

                                }
                            }
                        }
                        //busca Lote NÃO ESTA UTILIZANDO 
                        if (!String.IsNullOrEmpty(item.NumLote) && !String.IsNullOrEmpty(item.Idprd))
                        {
                            dataDados = new DataSet();
                            dataDados = buscaDados(arq.Name, "NUMLOTE = '" + item.NumLote + "' AND IDPRD = '" + item.Idprd + "' AND CODCOLIGADA = '" + dadosNf.SColigada + "'", "estPrdLoteData", sCodUsuarioTBC, sSenhaTBC);
                            if (dataDados.Tables.Count > 0)
                            {
                                dt = dataDados.Tables[0];


                                expression = "NUMLOTE = '" + item.NumLote + "'";
                                fLog(arq.Name, expression);
                                sort = "NUMLOTE ASC";


                                // Use the Select method to find all rows matching the filter.
                                foundRows = dt.Select(expression, sort);
                                for (int j = 0; j < foundRows.Length; j++)
                                {
                                    item.IdLote = int.Parse(foundRows[j]["IDLOTE"].ToString());
                                    temLote = true;
                                }
                            }
                            //insere lote
                            if (item.IdLote == 0 && !String.IsNullOrEmpty(item.NumLote))
                            {
                                string retorno;
                                retorno = insereMensagem("<EstPrdLote><TLotePrd><CODCOLIGADA>" + dadosNf.SColigada + "</CODCOLIGADA><IDLOTE>-1</IDLOTE><IDSTATUS>1</IDSTATUS><IDPRD>" + item.Idprd + "</IDPRD><NUMLOTE>" + item.NumLote + "</NUMLOTE><DATAVALIDADE>" + item.ValLote + "</DATAVALIDADE><DATAENTRADA>" + dadosNf.SDataEmissao + "</DATAENTRADA></TLotePrd></EstPrdLote>", "estPrdLoteData");
                                if (retorno.Substring(0, dadosNf.SColigada.Length) == dadosNf.SColigada & retorno.Substring(dadosNf.SColigada.Length, 1) == ";")
                                {
                                    string[] split = retorno.Split(';');
                                    try
                                    {
                                        item.IdLote = int.Parse(split[1].ToString());
                                    }
                                    catch { }
                                }
                                else
                                {
                                    if (retorno.Length > 75)
                                    {
                                        retorno = retorno.Substring(0, 75);
                                    }
                                    bVal = true;
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO,TIPO_DOC, EAN, CHAVEACESSO, CODPRD) VALUES " +
                                                    "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'CMP', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'Erro Ao tentar incluir o lote: " + retorno + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "','" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);

                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                        }
                                        catch (Exception exItens)
                                        {
                                            fLog(arq.Name, "aqui6");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            throw new Exception(exItens.Message);
                                        }

                                    }
                                }
                            }
                        }
                    }
                    //BUSCA PEDIDO
                    //VERIFICA SE ESTA ASSOCIADO OU NÃO
                    if (item.CodUnd != "" && !string.IsNullOrEmpty(item.CodUnd) && item.Idprd != "" && !string.IsNullOrEmpty(item.Idprd))
                    {
                        try
                        {
                            dbTools dbt = new dbTools();
                            string select = "SELECT ID FROM ZASSOCIAPO(NOLOCK) WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND NUMEROMOV_ASS IS NOT NULL AND ITEM_XML = " + i;
                            if (!dbt.ConsultaRetornaValor(select))
                            {
                                associado = 0;
                                try
                                {
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:xPed", ns);
                                    if (node != null)
                                    {
                                        item.PedidoDeCompra = node.InnerXml.ToString();
                                        if (item.PedidoDeCompra.Length < 6)
                                        {
                                            string nAux = new string('0', 6 - item.PedidoDeCompra.Length);
                                            item.PedidoDeCompra = nAux + item.PedidoDeCompra;
                                        }
                                    }
                                    else
                                    {
                                        item.PedidoDeCompra = "0";
                                        tagEncontrada = false;
                                    }
                                }
                                catch { item.PedidoDeCompra = "0"; tagEncontrada = false; }
                                try
                                {
                                    node = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + i + "]/nfe:prod/nfe:nItemPed", ns);
                                    if (node != null)
                                    {
                                        item.Nseq = int.Parse(node.InnerXml.ToString());
                                        item.CodcfoAssossiado = dadosNf.SCodCfoEmi;
                                    }
                                }
                                catch { }
                            }
                            else
                            {
                                associado = 1;
                                select = "SELECT NUMEROMOV_ASS, NSEQ_ASS, IDMOV_ASS, CODCFO FROM ZASSOCIAPO(NOLOCK) WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i;
                                DataSet zCritica = dbt.resultadoConsulta(select);
                                if (zCritica != null)
                                {
                                    //Pega o numero do movimento associado e adiciona ao pedido de compra
                                    item.PedidoDeCompra = zCritica.Tables[0].Rows[0][0].ToString();
                                    item.Nseq = int.Parse(zCritica.Tables[0].Rows[0][1].ToString());
                                    item.Idmov = int.Parse(zCritica.Tables[0].Rows[0][2].ToString());
                                    item.CodcfoAssossiado = zCritica.Tables[0].Rows[0][3].ToString();
                                }
                            }
                        }
                        catch { }
                        //caso o pedido tenha sido preenchido
                        if (!tagEncontrada && associado == 0)
                        {
                            vinculado = false;
                            fLog(arq.Name, "erro1");
                            bVal = true;
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, ASSOCIADO, CODPRD) VALUES " +
                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " NÃO ENCONTRADO PARA O FORNECEDOR: " + item.CodcfoAssossiado + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "'," + associado + ",'" + item.CodigoPrd + "')", connection);

                                fLog(arq.Name, "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, ASSOCIADO, CODPRD) VALUES " +
                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " NÃO ENCONTRADO PARA O FORNECEDOR: " + item.CodcfoAssossiado + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "'," + associado + ",'" + item.CodigoPrd + "')");
                                try
                                {
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception exItens)
                                {
                                    fLog(arq.Name, "aqui7");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                    throw new Exception(exItens.Message);

                                }

                            }
                            using (SqlConnection connection = dbt.GetConnection())
                            {
                                SqlCommand command = new SqlCommand("DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO (CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES " +
                                    "('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')", connection);

                                try
                                {
                                    fLog(arq.Name, "DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO(CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')");
                                    connection.Open();
                                    command.ExecuteReader();
                                }
                                catch (Exception exItens)
                                {
                                    fLog(arq.Name, "aqui4");
                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                    command.ExecuteReader();
                                    oReader.Close();
                                    throw new Exception(exItens.Message);
                                }
                            }

                        }
                        else if (item.PedidoDeCompra != "" && !string.IsNullOrEmpty(item.PedidoDeCompra) && item.Idprd != "" && !string.IsNullOrEmpty(item.Idprd))
                        {
                            string userComprador;
                            //Busca pedidos
                            dataDados = new DataSet();
                            if (associado == 0)
                            {
                                dataDados = buscaDados(arq.Name, "TMOV.NUMEROMOV = '" + item.PedidoDeCompra + "' and TMOV.CODCFO = '" + item.CodcfoAssossiado + "' and TMOV.CODCOLIGADA =  " + dadosNf.SColigada + " AND TMOV.CODTMV IN ('1.1.46','1.1.47','1.1.48') AND TMOV.CODCOLCFO = " + dadosNf.SColCfoEmi, "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC);
                            }
                            else
                            {
                                dataDados = buscaDados(arq.Name, "TMOV.IDMOV = '" + item.Idmov + "' and TMOV.CODCOLIGADA =  " + dadosNf.SColigada + " AND TMOV.CODTMV IN ('1.1.46','1.1.47','1.1.48')", "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC);
                            }
                            if (dataDados.Tables.Count > 0 && dataDados.Tables[0].TableName == "TMOV")
                            {
                                dt = dataDados.Tables[0];
                                expression = "NUMEROMOV = '" + item.PedidoDeCompra + "'";
                                fLog(arq.Name, expression);
                                sort = "NUMEROMOV DESC";

                                //Use the Select method to find all rows matching the filter.F
                                foundRows = dt.Select(expression, sort);
                                for (int j = 0; j < foundRows.Length; j++)
                                {
                                    item.Idmov = int.Parse(foundRows[j]["IDMOV"].ToString());
                                    item.CodcfoAssossiado = foundRows[j]["CODCFO"].ToString();
                                    try { item.IdnatCapa = foundRows[j]["IDNAT"].ToString(); } catch { item.IdnatCapa = ""; }
                                    try { item.IdnatCapa2 = foundRows[j]["IDNAT2"].ToString(); } catch { item.IdnatCapa2 = ""; }
                                    try { item.EstadoPedido = buscaEstadoPedido(arq.Name, item.CodcfoAssossiado, dadosNf.SColigada); } catch { item.EstadoPedido = ""; }
                                    if (dadosNf.SCodLoc == "" || string.IsNullOrEmpty(dadosNf.SCodLoc))
                                    {
                                        dadosNf.SCodLoc = foundRows[j]["CODLOC"].ToString();
                                    }
                                    try
                                    {
                                        dadosNf.MovNfeOrigem = foundRows[j]["CODTMV"].ToString();
                                        userComprador = foundRows[j]["CODVEN1"].ToString();
                                        if (userComprador != "" && associado == 0)
                                        {
                                            string pessoa = "";
                                            DataSet DVen = buscaDadosRecord(arq.Name, dadosNf.SColigada + ";" + userComprador, "MovVenData", sCodUsuarioTBC, sSenhaTBC);
                                            DataTable DTVen = DVen.Tables["TVEN"];
                                            pessoa = DTVen.Rows[0]["CODPESSOA"].ToString();
                                            if (pessoa != "")
                                            {
                                                DataSet DPes = buscaDadosRecord(arq.Name, pessoa, "RhuPessoaData", sCodUsuarioTBC, sSenhaTBC);
                                                DataTable DTPes = DPes.Tables["PPESSOA"];
                                                item.Comprador = DTPes.Rows[0]["CODUSUARIO"].ToString();
                                            }
                                        }
                                    }
                                    catch { }

                                }
                            }
                            //caso não encontre o pedido, critica
                            if (item.Idmov == 0)
                            {
                                fLog(arq.Name, "erro2222");
                                vinculado = false;
                                bVal = true;
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, ASSOCIADO, CODPRD) VALUES " +
                                                "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " NÃO ENCONTRADO PARA O FORNECEDOR: " + item.CodcfoAssossiado + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "'," + associado + ",'" + item.CodigoPrd + "')", connection);
                                    try
                                    {
                                        connection.Open();
                                        command.ExecuteReader();
                                    }
                                    catch (Exception exItens)
                                    {
                                        fLog(arq.Name, "aqui7");
                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                        command.ExecuteReader();
                                        oReader.Close();
                                        throw new Exception(exItens.Message);
                                    }

                                }
                                if (associado == 0 && item.FatorNf != "" && !string.IsNullOrEmpty(item.FatorNf))
                                {
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand("DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO (CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES " +
                                            "('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')", connection);

                                        try
                                        {
                                            fLog(arq.Name, "DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO(CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')");
                                            connection.Open();
                                            command.ExecuteReader();
                                        }
                                        catch (Exception exItens)
                                        {
                                            fLog(arq.Name, "aqui4");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            throw new Exception(exItens.Message);
                                        }

                                    }
                                }
                            }




                            //criarverificapedido
                            dataDados = new DataSet();
                            string idprdAux = "";
                            double quantidadePedido = 0;
                            double quantidadeMaisBrasil = 0;
                            double quantidadeAlimentar = 0;
                            double quantidadeVaiVarejo = 0;
                            double quantidadeCompl = 0;
                            double quantidadeSobra = 0;
                            string codUnidadePedido = "";
                            //caso tenha encontrado o idmov
                            if (item.Idmov != 0)
                            {
                                //busca o item
                                dataDados = buscaDadosRecord(arq.Name, dadosNf.SColigada + ";" + item.Idmov, "MovMovimentoTBCData", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    fLog("teste", "1");
                                    string nseqAux = "";
                                    string expressionItem = "";
                                    DataRow[] foundRowsItem;
                                    DataTable dt2 = new DataTable();
                                    DataTable dtItem = new DataTable();
                                    dtItem = dataDados.Tables["TITMMOV"];
                                    if (associado == 0)
                                    {
                                        expressionItem = "NUMEROSEQUENCIAL = '" + item.Nseq + "'";
                                    }
                                    else
                                    {
                                        expressionItem = "NSEQITMMOV = '" + item.Nseq + "'";
                                    }
                                    foundRowsItem = dtItem.Select(expressionItem);
                                    for (int jj = 0; jj < foundRowsItem.Length; jj++)
                                    {
                                        fLog("teste", "11");
                                        codUnidadePedido = foundRowsItem[jj]["CODUND"].ToString();
                                        idprdAux = foundRowsItem[jj]["IDPRD"].ToString();
                                        item.IdnatPedido = foundRowsItem[jj]["IDNAT"].ToString();
                                        item.QuantidadePedido = foundRowsItem[jj]["QUANTIDADE"].ToString();
                                        if (associado == 0)
                                        {
                                            item.Nseq = int.Parse(foundRowsItem[jj]["NSEQITMMOV"].ToString());
                                        }
                                        try
                                        {
                                            item.PrecoPedido = double.Parse(foundRowsItem[jj]["PRECOUNITARIO"].ToString().Replace(".", ","));
                                        }
                                        catch { item.PrecoPedido = 0; }
                                    }
                                    //NSEQ NÃO ENCONTRADO
                                    if (idprdAux == "" || string.IsNullOrEmpty(idprdAux))
                                    {
                                        vinculado = false;
                                        fLog("teste", "12");
                                        bVal = true;
                                        using (SqlConnection connection = dbt.GetConnection())
                                        {
                                            SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO, CODPRD) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'ITEM Nº " + item.Nseq + " NÃO ENCONTRADO NO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ",'" + item.CodigoPrd + "')", connection);
                                            try
                                            {
                                                connection.Open();
                                                command.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui12");
                                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                command.ExecuteReader();
                                                oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }

                                        } //nao encontrou o item e nao esta 
                                        if (associado == 0 && item.FatorNf != "" && !string.IsNullOrEmpty(item.FatorNf))
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO (CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES " +
                                                    "('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')", connection);

                                                try
                                                {
                                                    fLog(arq.Name, "DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO(CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')");
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui4");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }

                                            }
                                        }
                                        //CASO O IDPRD NÃO SEJA IGUAL
                                    }
                                    else if (idprdAux != item.Idprd)//PRODUTO DIFERENTE
                                    {
                                        vinculado = false;
                                        fLog("teste", "13");
                                        bVal = true;
                                        using (SqlConnection connection = dbt.GetConnection())
                                        {
                                            SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO, CODPRD) VALUES " +
                                                        "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'O IDPRD DA NOTA FISCAL: " + item.Idprd + " É DIFERENTE DO IDPRD " + idprdAux + " DO ITEM Nº " + item.Nseq + "  DO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ",'" + item.CodigoPrd + "')", connection);
                                            try
                                            {
                                                connection.Open();
                                                command.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui13");
                                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                command.ExecuteReader();
                                                oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }

                                        }
                                        using (SqlConnection connection = dbt.GetConnection())
                                        {
                                            SqlCommand command = new SqlCommand("DELETE FROM ZXMLTRANSFERENCIA WHERE NOME_XML = '" + arq.Name + "' AND NITEM = '" + i.ToString() + "'", connection);
                                            try
                                            {
                                                connection.Open();
                                                command.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui16");
                                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                command.ExecuteReader();
                                                oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }
                                        }
                                        if (associado == 0 && item.FatorNf != "" && !string.IsNullOrEmpty(item.FatorNf))
                                        {
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO (CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES " +
                                                    "('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')", connection);

                                                try
                                                {
                                                    fLog(arq.Name, "DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO(CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')");
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui4");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }

                                            }
                                        }
                                    }
                                    else //preenche os valores de quantidade
                                    {
                                        string fatorPedido = "";
                                        DataSet dataUnd = new DataSet();
                                        dataUnd = buscaDadosRecord(arq.Name, codUnidadePedido, "EstUndData", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataUnd.Tables.Count > 0)
                                        {
                                            dt = dataUnd.Tables["TUND"];
                                            expression = "CODUND = '" + codUnidadePedido + "'";
                                            fLog(arq.Name, expression);
                                            sort = "CODUND DESC";

                                            //Use the Select method to find all rows matching the filter.F
                                            foundRows = dt.Select(expression, sort);
                                            for (int l = 0; l < foundRows.Length; l++)
                                            {
                                                item.FatorPedido = foundRows[l]["FATORCONVERSAO"].ToString().Replace(".", ",");
                                            }
                                        }
                                        try
                                        {
                                            double auxQuantidadeEstoque = 0;
                                            double auxFator = double.Parse(item.FatorPedido);
                                            double auxQuant = double.Parse(item.QuantidadePedido.Replace(".", ","));
                                            double quantidadeBaixar = 0;
                                            auxQuantidadeEstoque = auxFator * auxQuant;
                                            quantidadeBaixar = item.QuantidadeEstoqueNota / auxFator;
                                            item.QuantidadeEstoquePedido = auxQuantidadeEstoque;
                                            item.QuantidadeBaixar = quantidadeBaixar.ToString();
                                        }
                                        catch { }
                                        //CASO A UNIDADE SEJA DIFERENTE (nao esta verificando)
                                        try
                                        {
                                            /*
                                            if (codUnidadePedido != item.CodUnd)
                                            {
                                                bVal = true;
                                                using (SqlConnection connection = dbt.GetConnection())
                                                {
                                                    SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO,FLAG_STATUS, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO) VALUES " +
                                                                "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'A UNIDADE DA NOTA FISCAL: " + item.CodUnd + " É DIFERENTE DA UNIDADE " + codUnidadePedido + " DO ITEM Nº " + item.Nseq + "  DO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C','E', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ")", connection);
                                                    try
                                                    {
                                                        connection.Open();
                                                        command.ExecuteReader();
                                                    }
                                                    catch (Exception exItens)
                                                    {
                                                        fLog(arq.Name, "aqui16");
                                                        command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                        command.ExecuteReader();
                                                        oReader.Close();
                                                        throw new Exception(exItens.Message);
                                                    }

                                                }
                                            }*/
                                        }
                                        catch { }
                                        //criticar preco tabela especial. (reaproveitar para aprovacao)
                                        //verifica aprovação se for pedido associado
                                        if (associado == 1)
                                        {

                                            string statusAprov = "";
                                            try
                                            {
                                                using (SqlConnection connection = dbt.GetConnection())
                                                {
                                                    SqlCommand command = new SqlCommand("SELECT APPROVAL_STATUS FROM PRODUCT_CONTROL(NOLOCK) WHERE NFE_KEY = '" + dadosNf.ChaveAcesso + "' AND NITEM = '" + i + "'", connection);
                                                    try
                                                    {
                                                        connection.Open();
                                                        SqlDataReader dr;
                                                        dr = command.ExecuteReader();
                                                        if (dr.HasRows)
                                                        {
                                                            while (dr.Read())
                                                            {
                                                                statusAprov = dr[0].ToString();
                                                            }
                                                        }
                                                        dr.Close();

                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        fLog(arq.Name, "aqui8");
                                                        command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + ex.Message.Replace("'", "") + "')";
                                                        command.ExecuteReader();
                                                        this.oReader.Close();
                                                    }
                                                }
                                                if (string.IsNullOrEmpty(statusAprov) || statusAprov == "" || statusAprov.ToUpper() == "PENDENTE")
                                                {
                                                    vinculado = false;
                                                    bVal = true;
                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        string insertMensagem = "";
                                                        if (statusAprov.ToUpper() == "PENDENTE")
                                                        {
                                                            insertMensagem = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO) VALUES " +
                                                                    "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'ITEM PENDENTE DE APROVAÇÃO','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ")";
                                                        }
                                                        else
                                                        {
                                                            insertMensagem = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO) VALUES " +
                                                                    "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'ITEM NÃO EXISTE NA TABELA DE APROVAÇÃO','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ")";
                                                        }
                                                        SqlCommand command = new SqlCommand(insertMensagem, connection);
                                                        try
                                                        {
                                                            connection.Open();
                                                            command.ExecuteReader();
                                                        }
                                                        catch (Exception exItens)
                                                        {
                                                            fLog(arq.Name, "erro preco");
                                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                            command.ExecuteReader();
                                                            oReader.Close();
                                                            throw new Exception(exItens.Message);
                                                        }

                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            { }

                                        }
                                        //caso não esteja associado, vai criar a zassociapo já associado automaticamente.
                                        if (associado == 0 && item.FatorNf != "" && !string.IsNullOrEmpty(item.FatorNf))
                                        {

                                            try
                                            {
                                                decimal difPreco = 0;
                                                decimal precoNotaAux = 0;
                                                decimal precoPedidoAux = 0;
                                                decimal auxFatorNf = decimal.Parse(item.FatorNf);
                                                decimal auxFatorPedido = decimal.Parse(item.FatorPedido);
                                                precoNotaAux = Math.Round(decimal.Parse(item.ValorProduto) / auxFatorNf, 2);
                                                precoPedidoAux = Math.Round(decimal.Parse(item.PrecoPedido.ToString()) / auxFatorPedido, 2);
                                                difPreco = precoNotaAux - precoPedidoAux;
                                                if (difPreco >= minAp && difPreco <= maxAp)
                                                {
                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        SqlCommand command = new SqlCommand("DELETE FROM PRODUCT_CONTROL WHERE NFE_KEY = '" + dadosNf.ChaveAcesso + "' AND NITEM = " + i + " INSERT INTO PRODUCT_CONTROL(NFE_KEY, NITEM, APPROVAL_STATUS,APPROVAL_DATE,APPROVAL_MESSAGE,PRICE_RM,PRICE_XML,APPROVAL_USER_ID, COMPRADOR) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", 'AUTO', GETDATE(), 'Autorizado Automaticamente'," + precoNotaAux.ToString().Replace(",", ".") + "," + precoPedidoAux.ToString().Replace(",", ".") + ",'1','" + item.Comprador + "')", connection);

                                                        try
                                                        {
                                                            fLog(arq.Name, "DELETE FROM PRODUCT_CONTROL WHERE NFE_KEY = '" + dadosNf.ChaveAcesso + "' AND NITEM = " + i + " INSERT INTO PRODUCT_CONTROL(NFE_KEY, NITEM, APPROVAL_STATUS,APPROVAL_DATE,APPROVAL_MESSAGE,PRICE_RM,PRICE_XML,APPROVAL_USER_ID, COMPRADOR) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", 'AUTO', GETDATE(), 'Autorizado Automaticamente'," + precoNotaAux.ToString().Replace(",", ".") + "," + precoPedidoAux.ToString().Replace(",", ".") + ",'1','" + item.Comprador + "')");
                                                            connection.Open();
                                                            command.ExecuteReader();
                                                        }
                                                        catch (Exception exItens)
                                                        {
                                                            fLog(arq.Name, "aqui4");
                                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                            command.ExecuteReader();
                                                            oReader.Close();
                                                            throw new Exception(exItens.Message);
                                                        }

                                                    }
                                                }
                                                else
                                                {
                                                    vinculado = false;
                                                    bVal = true;
                                                    using (SqlConnection connection = dbt.GetConnection())
                                                    {
                                                        string insertMensagem = "";

                                                        insertMensagem = "INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO) VALUES " +
                                                                "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'ITEM PENDENTE DE APROVAÇÃO','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ")";

                                                        SqlCommand command = new SqlCommand(insertMensagem, connection);
                                                        try
                                                        {
                                                            connection.Open();
                                                            command.ExecuteReader();
                                                        }
                                                        catch (Exception exItens)
                                                        {
                                                            fLog(arq.Name, "erro preco");
                                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                            command.ExecuteReader();
                                                            oReader.Close();
                                                            throw new Exception(exItens.Message);
                                                        }

                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            { }


                                            //insere associado automaticamente
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO (CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT,CODCFO, NUMEROMOV_ASS, NSEQ_ASS, IDMOV_ASS, RECMODIFIEDBY, RECMODIFIEDON, CODPRD) VALUES " +
                                                    "('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodcfoAssossiado + "','" + item.PedidoDeCompra + "','" + item.Nseq + "','" + item.Idmov + "','auto',GETDATE(),'" + item.CodigoPrd + "')", connection);

                                                try
                                                {
                                                    fLog(arq.Name, "DELETE FROM ZASSOCIAPO WHERE CHAVEACESSO = '" + dadosNf.ChaveAcesso + "' AND ITEM_XML = " + i + " INSERT INTO ZASSOCIAPO(CHAVEACESSO, ITEM_XML, CNPJ, RAZAO, COD_PRD, COD_PRD_AUX, DESC_PRD, UND_MED, PEDIDO, QUANTIDADE, EAN, FATOR, QUANTIDADEESTOQUE, PRECOITEM, PRECOUNIT, CODPRD) VALUES ('" + dadosNf.ChaveAcesso + "', " + i + ", '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "','" + item.Idprd + "','" + sCodProd + "','" + sNomeProd + "','" + sUnd + "','" + pedidoXml + "','" + qtdeCritica + "','" + ean + "','" + item.FatorNf.Replace(",", ".") + "'," + item.QuantidadeEstoqueNota.ToString().Replace(",", ".") + ", " + item.ValorProduto.Replace(",", ".") + ", " + item.PrecoUnitario.ToString().Replace(",", ".") + ",'" + item.CodigoPrd + "')");
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui4");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }

                                            }
                                        }
                                        //quantidade não bate para inclusao, critica.
                                        if (item.QuantidadeEstoqueNota > item.QuantidadeEstoquePedido)
                                        {
                                            vinculado = false;
                                            bVal = true;
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, QUANTIDADE, CHAVEACESSO, PEDIDO, ASSOCIADO, CODPRD) VALUES " +
                                                            "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'PO', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'A QUANTIDADE DO PEDIDO CONVERTIDO EM UN: " + item.QuantidadeEstoquePedido.ToString() + " É MENOR QUE A QUANTIDADE DA NOTA CONVERTIDO EM UN: " + item.QuantidadeEstoqueNota.ToString() + " DO ITEM Nº " + item.Nseq + "  DO PEDIDO DE COMPRA Nº " + item.PedidoDeCompra + " DO FORNECEDOR: " + dadosNf.SCodCfoEmi + " - " + dadosNf.NomeEmi + "','C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "', '" + item.Idprd + "', " + qtdeCritica + ",'" + dadosNf.ChaveAcesso + "','" + pedidoXml + "'," + associado + ",'" + item.CodigoPrd + "')", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui1226");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }

                                            }
                                        }
                                    }
                                }
                            }
                            //buscar cst pis e cofins do produto
                            if (item.Idprd != "" && !string.IsNullOrEmpty(item.Idprd))
                            {
                                //BUSCAR CST PIS E COFINS do item
                                dataDados = new DataSet();
                                dataDados = buscaDados(arq.Name, "CODCOLIGADA = " + coligadaIdprd + " AND IDPRD = " + item.Idprd + " AND CODTRB IN('PIS', 'COFINS')", "EstTrbPrdData", sCodUsuarioTBC, sSenhaTBC);
                                if (dataDados.Tables.Count > 0)
                                {
                                    dt = dataDados.Tables[0];
                                    expression = "CODTRB = 'PIS'";
                                    sort = "CODCOLIGADA ASC";
                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression, sort);
                                    for (int j = 0; j < foundRows.Length; j++)
                                    {
                                        try
                                        {
                                            item.CstPis = foundRows[j]["SITTRIBUTARIAENT"].ToString();
                                        }
                                        catch { item.CstPis = ""; }
                                    }
                                    expression = "CODTRB = 'COFINS'";
                                    sort = "CODCOLIGADA ASC";
                                    // Use the Select method to find all rows matching the filter.
                                    foundRows = dt.Select(expression, sort);
                                    for (int j = 0; j < foundRows.Length; j++)
                                    {
                                        try
                                        {
                                            item.CstCofins = foundRows[j]["SITTRIBUTARIAENT"].ToString();
                                        }
                                        catch { item.CstCofins = ""; }
                                    }
                                }
                                if (item.CstPis == "" || string.IsNullOrEmpty(item.CstPis))
                                {
                                    bVal = true;
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        //verificar;
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, CHAVEACESSO, CODPRD) VALUES " +
                                                    "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'FIS', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'CST DE ENTRADA DO PIS NÃO ENCONTRADO NO CADASTRO DO PRODUTO', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                        }
                                        catch (Exception exItens)
                                        {
                                            fLog(arq.Name, "aqui17");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            throw new Exception(exItens.Message);
                                        }
                                    }
                                }
                                if (item.CstCofins == "" || string.IsNullOrEmpty(item.CstCofins))
                                {
                                    bVal = true;
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        //verificar;
                                        SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO,CODCFO, CNPJ, RAZAO, SETOR, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, COD_PRD, CHAVEACESSO, CODPRD) VALUES " +
                                                    "('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'FIS', '" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "', 'CST DE ENTRADA DO COFINS NÃO ENCONTRADO NO CADASTRO DO PRODUTO', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                        }
                                        catch (Exception exItens)
                                        {
                                            fLog(arq.Name, "aqui18");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            throw new Exception(exItens.Message);
                                        }
                                    }
                                }
                            }
                            //busca natureza e movimento
                            if (tipoPrd != "")
                            {
                                try
                                {
                                    //busca natureza que está no xml
                                    string idnat = "";
                                    ArrayList idnatInversa = new ArrayList();
                                    string idnatColigada = "";
                                    sCompCfop = Convert.ToInt16(sCompCfop).ToString(@"0\.000");
                                    DataSet dataNat = new DataSet();
                                    fLog(arq.Name, "Busca dados Natureza");
                                    dataDados = buscaDados(arq.Name, "(DCFOP.CODCOLIGADA = " + dadosNf.SColigada + " OR DCFOP.codcoligada = 0) AND CODNAT = '" + sCompCfop + "'", "FisNaturezaData", sCodUsuarioTBC, sSenhaTBC);
                                    if (dataDados.Tables.Count > 0)
                                    {
                                        dt = dataDados.Tables[0];
                                        expression = "CODNAT = '" + sCompCfop + "'";
                                        sort = "CODCOLIGADA ASC";

                                        // Use the Select method to find all rows matching the filter.
                                        foundRows = dt.Select(expression, sort);
                                        for (int j = 0; j < foundRows.Length; j++)
                                        {
                                            idnatColigada = foundRows[j]["CODCOLIGADA"].ToString();
                                            idnat = foundRows[j]["IDNAT"].ToString();
                                        }
                                    }
                                    //nao encontrou, critica
                                    if (idnatColigada == "" || string.IsNullOrEmpty(idnatColigada))
                                    {
                                        bVal = true;
                                        using (SqlConnection connection = dbt.GetConnection())
                                        {

                                            SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR, CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO,TIPO_DOC, EAN, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, COD_PRD, CHAVEACESSO, CODPRD) VALUES ('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121), 'FIS','" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "','" + dadosNf.NomeEmi + "', 'CFOP " + sCompCfop + " NÃO ENCONTRADA NO SISTEMA', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "','" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                            try
                                            {
                                                connection.Open();
                                                command.ExecuteReader();
                                            }
                                            catch (Exception exItens)
                                            {
                                                fLog(arq.Name, "aqui19");
                                                command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                command.ExecuteReader();
                                                this.oReader.Close();
                                                throw new Exception(exItens.Message);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        //busca inversa
                                        fLog(arq.Name, "Busca dados Natureza Inversa");
                                        dataDados = buscaDados(arq.Name, "CODCOLIGADA = " + idnatColigada + " AND IDNATORIGEM = '" + idnat + "'", "MovNaturezaColabData", sCodUsuarioTBC, sSenhaTBC);
                                        if (dataDados.Tables.Count > 0)
                                        {
                                            dt = dataDados.Tables[0];
                                            expression = "IDNATORIGEM = '" + idnat + "'";
                                            sort = "CODCOLIGADA ASC";

                                            // Use the Select method to find all rows matching the filter.
                                            foundRows = dt.Select(expression, sort);
                                            for (int j = 0; j < foundRows.Length; j++)
                                            {
                                                idnatInversa.Add(foundRows[j]["IDNATINVERSA"].ToString());
                                            }
                                        }
                                        //nao encontrou inversa, critica
                                        if (idnatInversa.Count == 0)
                                        {
                                            bVal = true;
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand($"INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, COD_PRD, CHAVEACESSO, CODPRD) VALUES ('{dadosNf.SColigada}','{dadosNf.SFilial}','{arq.Name}', convert(datetime, '{dadosNf.SDataEmissao}', 121), 'FIS','{dadosNf.SCodCfoEmi}','{dadosNf.CnpjEmi}','{dadosNf.NomeEmi}','NENHUMA NATUREZA INVERSA ENCONTRADA PARA A CFOP: {sCompCfop}','C',0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','{ean}','{i}','{sCodProd}','{sNomeProd}','{sUnd}',{item.Idprd},'{dadosNf.ChaveAcesso}','{item.CodigoPrd}')", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui20");
                                                    command = new SqlCommand($"INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ({arq.Name}', convert(datetime, '{string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now)}', 121), '{exItens.Message.Replace("'", "")}')", connection);
                                                    command.ExecuteReader();
                                                    this.oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }
                                            }
                                        }
                                    }
                                    if (idnatInversa.Count >= 0)
                                    {
                                        string regraNatureza = "";
                                        //tipo produto fiscal = 1 e tipo fiscal forn = 1, natureza fixa 16
                                        if (item.TipoPrdFiscal == "1" && dadosNf.Idcfofiscal == "1")
                                        {
                                            item.Idnat = "16";
                                        }
                                        else
                                        {
                                            //busca a natureza inversa correta na lista de inversas
                                            fLog(arq.Name, "Busca dados Natureza para inserção");
                                            foreach (string itemNat in idnatInversa)
                                            {
                                                regraNatureza = "DCFOP.CODCOLIGADA = " + idnatColigada + " AND TIPOPRD = '" + tipoPrd + "' AND ALIQICMS = " + item.AliqIcms.ToString().Replace(",", ".") + " AND DCFOP.IDNAT = " + itemNat.ToString();
                                                /*
                                                if (item.AliqIcmsSt == "" || string.IsNullOrEmpty(item.AliqIcmsSt) || item.AliqIcmsSt == "0")
                                                {
                                                    regraNatureza = "DCFOP.CODCOLIGADA = " + idnatColigada + " AND FATORICMS = '" + item.BaseRed.Replace(",", ".") + "' AND TIPOPRD = '" + tipoPrd + "' AND ALIQICMS = " + item.AliqIcms.ToString().Replace(",", ".") + "  AND DCFOP.IDNAT = " + itemNat.ToString();
                                                }
                                                else
                                                {
                                                    regraNatureza = "DCFOP.CODCOLIGADA = " + idnatColigada + " AND FATORICMS = '" + item.BaseRed.Replace(",", ".") + "' AND TIPOPRD = '" + tipoPrd + "' AND ALIQSUBST = " + item.AliqIcmsSt.ToString().Replace(",", ".") + " AND ALIQICMS = " + item.AliqIcms.ToString().Replace(",", ".") + " AND DCFOP.IDNAT = " + itemNat.ToString();
                                                }
                                                */
                                                try
                                                {
                                                    dataDados = buscaDados(arq.Name, regraNatureza, "FisNaturezaData", sCodUsuarioTBC, sSenhaTBC);
                                                    if (dataDados.Tables.Count > 0)
                                                    {
                                                        dt = dataDados.Tables[0];
                                                        linha = dt.Rows[0];
                                                        item.Idnat = linha["IDNAT"].ToString();
                                                        break;
                                                    }
                                                }
                                                catch (Exception ex)
                                                { }
                                            }
                                        }
                                        //nao encontrou nenhum, critica
                                        if (item.Idnat == "" || string.IsNullOrEmpty(item.Idnat))
                                        {

                                            bVal = true;
                                            using (SqlConnection connection = dbt.GetConnection())
                                            {
                                                SqlCommand command = new SqlCommand("INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO, SETOR,CODCFO, CNPJ, RAZAO, CRITICA, TIPO, FLAG_ERRO, CST, ALIQ_ICMS, ALIQ_ICMSST, RED_BC, TIPO_DOC, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, EAN, COD_PRD, CHAVEACESSO, CODPRD) VALUES ('" + dadosNf.SColigada + "', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121), 'FIS','" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "','" + dadosNf.NomeEmi + "', 'NENHUMA NATUREZA INVERSA ATENDE AS REGRAS DE ICMS DA NFE. TIPO PRODUTO: " + tipoPrd + ", CFOP NOTA: " + sCompCfop + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082, '" + item.CstIcms + "', '" + item.AliqIcms.Replace(',', '.') + "', '" + item.AliqIcmsSt.Replace(',', '.') + "', '" + item.BaseRed.Replace(',', '.') + "','2','" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "','" + ean + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                                try
                                                {
                                                    connection.Open();
                                                    command.ExecuteReader();
                                                }
                                                catch (Exception exItens)
                                                {
                                                    fLog(arq.Name, "aqui21");
                                                    command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                                    command.ExecuteReader();
                                                    this.oReader.Close();
                                                    throw new Exception(exItens.Message);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            string regra, aliqSt;
                                            aliqSt = "0";
                                            regra = "";
                                            dataDados = buscaDadosRecord(arq.Name, idnatColigada + ";" + item.Idnat, "FisNaturezaData", sCodUsuarioTBC, sSenhaTBC);
                                            if (dataDados.Tables.Count > 0)
                                            {
                                                dt = dataDados.Tables[0];
                                                linha = dt.Rows[0];
                                                regra = linha["IDREGRAICMS"].ToString();
                                            }
                                            dataDados = buscaDadosRecord(arq.Name, idnatColigada + ";" + regra, "FisRegraICMSDATA", sCodUsuarioTBC, sSenhaTBC);
                                            if (dataDados.Tables.Count > 0)
                                            {
                                                try
                                                {
                                                    dt = dataDados.Tables[0];
                                                    linha = dt.Rows[0];
                                                    aliqSt = linha["ALIQSUBST"].ToString();
                                                }
                                                catch (Exception ex) { fLog("AliquotaErro", ex.Message); }
                                            }
                                            fLog("Aliquota", aliqSt);
                                            fLog("Aliquota", item.AliqIcmsSt);
                                            if (item.AliqIcmsSt == "0" || String.IsNullOrEmpty(item.AliqIcmsSt))
                                            {
                                                if (aliqSt != "0" & aliqSt != "0.0000" & !String.IsNullOrEmpty(aliqSt))
                                                {
                                                    stNDestacado = 1;
                                                }
                                                else { stNDestacado = 2; }
                                            }
                                            else { stNDestacado = 0; }
                                            fLog("Aliquota", aliqSt);
                                        }

                                    }
                                }
                                catch { }
                                movimentoAux2 = "";
                                //busca o movimento que será inserido
                                using (SqlConnection connection = dbt.GetConnection())
                                {
                                    fLog("Busca", "SELECT MOVIMENTO, PRIORIDADE FROM ZCFOPXMOVIMENTO(NOLOCK) WHERE CODCOLIGADA = '" + dadosNf.SColigada + "' AND CFOP = '" + sCompCfop + "' AND ICMSST = " + stNDestacado);
                                    SqlCommand command = new SqlCommand("SELECT MOVIMENTO,PRIORIDADE FROM ZCFOPXMOVIMENTO(NOLOCK) WHERE CODCOLIGADA = '" + dadosNf.SColigada + "' AND CFOP = '" + sCompCfop + "' AND ICMSST = " + stNDestacado, connection);
                                    try
                                    {
                                        int prioridadeaux = 0;
                                        connection.Open();
                                        SqlDataReader drItens;
                                        drItens = command.ExecuteReader();
                                        if (drItens.HasRows)
                                        {
                                            while (drItens.Read())
                                            {
                                                fLog("prioridadeaux", drItens[1].ToString());
                                                fLog("Movimento", drItens[0].ToString());
                                                prioridadeaux = int.Parse(drItens[1].ToString());
                                                movimentoAux2 = drItens[0].ToString();
                                            }
                                        }
                                        else
                                        {
                                            movimentoAux2 = "";
                                        }
                                        drItens.Close();
                                        if (movimentoAux2 != "")
                                        {
                                            fLog("prioridadeatual", prioridade.ToString());
                                            fLog("Movimentoatual", movimentoAux.ToString());
                                            if (prioridadeaux > prioridade)
                                            {
                                                movimentoAux = movimentoAux2;
                                                prioridade = prioridadeaux;

                                            }
                                            fLog("prioridadepos", prioridade.ToString());
                                            fLog("Movimentopos", movimentoAux.ToString());
                                        }
                                    }
                                    catch (Exception exItens)
                                    {
                                        fLog(arq.Name, "aqui22");
                                        command.CommandText = "INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')";
                                        command.ExecuteReader();
                                        this.oReader.Close();
                                        throw new Exception(exItens.Message);
                                    }
                                }
                                //movimento não encontrado critica
                                if (movimentoAux2 == "")
                                {

                                    string destacado = "";
                                    if (stNDestacado == 0)
                                    {
                                        destacado = "Destacado";
                                    }
                                    else if (stNDestacado == 1)
                                    {
                                        destacado = "Não Destacado";
                                    }
                                    else { destacado = "Sem ICMSST"; }
                                    bVal = true;
                                    using (SqlConnection connection = dbt.GetConnection())
                                    {
                                        SqlCommand command = new SqlCommand($"INSERT INTO ZCRITICAXML (CODCOLIGADA, CODFILIAL, NOME_XML, DATAEMISSAO,CODCFO, CNPJ, RAZAO, SETOR, CRITICA, TIPO, FLAG_ERRO, TIPO_DOC, EAN, ITEM_XML, COD_PRD_AUX, DESC_PRD, UND_MED, COD_PRD, CHAVEACESSO, CODPRD) VALUES ('{dadosNf.SColigada}', '" + dadosNf.SFilial + "', '" + arq.Name + "', convert(datetime, '" + dadosNf.SDataEmissao + "', 121),'" + dadosNf.SCodCfoEmi + "', '" + dadosNf.CnpjEmi + "', '" + dadosNf.NomeEmi + "', 'FIS', " + "'A CFOP " + sCompCfop + " NAO POSSUI AMARRACAO COM O IDENT. DO MOVIMENTO. Tipo ICMSST: " + destacado + "', 'C', 0x89504E470D0A1A0A0000000D49484452000000100000001008060000001FF3FF610000001974455874536F6674776172650041646F626520496D616765526561647971C9653C000002834944415478DA94534D48545114FEEE7D777E34FCA9E887C24D84AEAA31A3280285F611D52668D322863645AD0C142257B58820A26D446E6A4A6A9398529925848E498CFD50298EA8E138FA7CD38CCE7BEFDECEBD6F466C51D161CEDC77DE9CF39DEF7CF70C4B5C3DF44472753492F13B370CE6FA25FCB894FE5E2525A000C61898102B3C1CBAC9B9D58AB5A67F7B78E5A06A38D008BBB5179BF7C750BB6B376A624D88EC6C004261A090C7CFD124863ADA115EBF9179CE522394AC570A6779287444286A23A582E7B9A8BF761BCBBD5D50DFC7E08DBE85A8AE42716606EBCE5C82BF52845FC8AB4A02B62A2AB0E3DC45BC8F9F86D04C3488FE765323C875DE4178DB76F02D5BA1BC02FCD934FCC971486AB0EF6E02A2B212CC2DC2E2A0775E0040C3D087C3B7178876886626EA9628B905B9B4084939D1E13EA8CFA3C87DFD8268BCCD68C0CBF53A900B59C2B1827855280EA5818530C52A1C01C8158182F31280360AA43D6F4E46455C9FC4931B0604A0818D73734A0DBACAC034D29DB2C4DA820859F08A2E9CB96C909473A8CE32CFC1B8C4D6CE1A762298809984E9BE6EE47814CE788A3455256885AAE8077002D63B01CE0C2B102BCD30002878241CC798E33F1D7132DD1D9F26DF9599BD6EDE3394F724696B191D1889BC4CF16CCF33645CF958986D0B0B4C1DABC2A9CB8317A8667EA039E694857C31B778BE6553ED0D4A13AFBA9E133185AC2B7B5E66EDFBB7BE4DF707D7488BC45D8507D70F4FD43D5A82D9621E8CDB9E9AB8474F6FF0FBDD9000F831D012CB072368163E7951217DA20675091B4A30E39464534A127F30CE4A37A005E2D4991725A68E57237DB29A4095F943FDCD842EFE984C060A6B300281A7F40DFDB3B8BC834D6BE261FCA7FD1260008F040498366F1A820000000049454E44AE426082,'2','" + ean + "','" + i + "', '" + sCodProd + "', '" + sNomeProd + "', '" + sUnd + "'," + item.Idprd + ",'" + dadosNf.ChaveAcesso + "','" + item.CodigoPrd + "')", connection);
                                        try
                                        {
                                            connection.Open();
                                            command.ExecuteReader();
                                        }
                                        catch (Exception exItens)
                                        {
                                            fLog(arq.Name, "aqui24");
                                            command = new SqlCommand("INSERT INTO ZERROSAPLICXML (NOME_XML, DATA, ERRO) VALUES ('" + arq.Name + "', convert(datetime, '" + string.Format("{0:yyyy-MM-dd HH:mm:ss.fff}", DateTime.Now) + "', 121), '" + exItens.Message.Replace("'", "") + "')", connection);
                                            command.ExecuteReader();
                                            oReader.Close();
                                            throw new Exception(exItens.Message);
                                        }
                                    }
                                }
                            } //fim busca natureza
                        }//encontra pedido
                    }
                }// se não encontrar o codigo prd


                if (!bSai)
                {
                    //item.Idnat = "96";
                    listaItem.Add(item);
                    if (enviarVex)
                    {
                        if (listaItemVex.Count > 0)
                        {
                            bool duplicado = false;
                            foreach (itensVex itemLista in listaItemVex)
                            {
                                if (itemLista.Codigo == itemV.Codigo)
                                {
                                    duplicado = true;
                                    itemLista.Quantidade = itemLista.Quantidade + itemV.Quantidade;
                                    itemLista.QuantidadeNF = itemLista.QuantidadeNF + itemV.QuantidadeNF;
                                }
                            }
                            if (!duplicado)
                            {
                                listaItemVex.Add(itemV);
                            }
                        }
                        else
                        {
                            listaItemVex.Add(itemV);
                        }
                        try { listaItemSenior.Add(itemS); }
                        catch { }
                    }
                }
            }
            dadosS.Num_Itens = idPrdUnico.Count;
            dadosNf.MovNfe = movimentoAux;
            dadosNf.Itens = listaItem.ToArray();
            if (enviarVex)
            {
                dadosV.Itens = listaItemVex.ToArray();
                try
                {
                    dadosS.Itens = listaItemSenior.ToArray();
                }
                catch
                {

                }
            }
           
            return bVal;
        }

        private string buscaEstadoPedido(string name, string codcfo, string coligada)
        {
            DataSet dataDados = new DataSet();
            string retorno = "";
            try
            {
                dataDados = buscaDadosRecord(name, coligada + ";" + codcfo, "FinCFODataBR", sCodUsuarioTBC, sSenhaTBC);
                if (dataDados.Tables.Count > 0)
                {
                    DataTable dt = dataDados.Tables["FCFO"];
                    string expression = "CODCFO = '" + codcfo + "'";
                    if (this.bAtivaLog)
                    {
                        fLog(name, expression);
                    }
                    string sort = "CODCFO DESC";

                    //Use the Select method to find all rows matching the filter.F
                    DataRow[] foundRows = dt.Select(expression, sort);
                    for (int j = 0; j < foundRows.Length; j++)
                    {
                        try { retorno = foundRows[j]["CODETD"].ToString().Replace("'", ""); } catch { }
                    }
                }
                return retorno;
            }
            catch (Exception ex) { return retorno; }
        }

        private string ResultadoSQL(string select, int tableIndex)
        {
            using (SqlConnection connection = dbt.GetConnection())
            {
                SqlCommand command = new SqlCommand(select, connection);
                try
                {
                    connection.Open();
                    SqlDataReader dr;
                    dr = command.ExecuteReader();

                    string resultado = "";
                    if (dr.HasRows)
                    {
                        try
                        {
                            if (dr.Read())
                                resultado = dr[0].ToString();
                            else
                                resultado = "";
                        }
                        catch
                        {
                            resultado = "";
                        }

                        return resultado;
                    }
                    else return null;
                }
                catch (Exception ex) { return null; }
            }
        }

        private string FPegaIcmsSn(XmlDocument xmlDoc, int item, FileInfo arq)
        {

            if (this.bAtivaLog)
            {
                fLog(arq.Name, "Função FPegaIcmsSn item " + item);
            }
            XmlNamespaceManager ns = new XmlNamespaceManager(xmlDoc.NameTable);
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe");
            XPathNavigator xpathNav = xmlDoc.CreateNavigator();
            XPathNavigator node101;
            XPathNavigator node102;
            XPathNavigator node103;
            XPathNavigator node201;
            XPathNavigator node202;
            XPathNavigator node203;
            XPathNavigator node300;
            XPathNavigator node400;
            XPathNavigator node500;
            XPathNavigator node900;
            string sResp = "";

            try
            {
                node101 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN101/nfe:orig", ns);
                if (node101 != null)
                {
                    sResp = "101";
                }
            }
            catch { }
            try
            {
                node102 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN102/nfe:orig", ns);
                if (node102 != null)
                {
                    sResp = sResp.Trim() == "" ? "102" : sResp + ";102";
                }
            }
            catch { }
            try
            {
                node103 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN103/nfe:orig", ns);
                if (node103 != null)
                {
                    sResp = sResp.Trim() == "" ? "103" : sResp + ";103";
                }
            }
            catch { }
            try
            {
                node201 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN201/nfe:orig", ns);
                if (node201 != null)
                {
                    sResp = sResp.Trim() == "" ? "201" : sResp + ";201";
                }
            }
            catch { }
            try
            {
                node202 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN202/nfe:orig", ns);
                if (node202 != null)
                {
                    sResp = sResp.Trim() == "" ? "202" : sResp + ";202";
                }
            }
            catch { }
            try
            {
                node203 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN203/nfe:orig", ns);
                if (node203 != null)
                {
                    sResp = sResp.Trim() == "" ? "203" : sResp + ";203";
                }
            }
            catch { }
            try
            {
                node300 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN300/nfe:orig", ns);
                if (node300 != null)
                {
                    sResp = sResp.Trim() == "" ? "300" : sResp + ";300";
                }
            }
            catch { }
            try
            {
                node400 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN400/nfe:orig", ns);
                if (node400 != null)
                {
                    sResp = sResp.Trim() == "" ? "400" : sResp + ";400";
                }
            }
            catch { }
            try
            {
                node500 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN500/nfe:orig", ns);
                if (node500 != null)
                {
                    sResp = sResp.Trim() == "" ? "500" : sResp + ";500";
                }
            }
            catch { }
            try
            {
                node900 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMSSN900/nfe:orig", ns);
                if (node900 != null)
                {
                    sResp = sResp.Trim() == "" ? "900" : sResp + ";900";
                }
            }
            catch { }
            return sResp;
        }
        private string FPegaIcms(XmlDocument xmlDoc, int item, FileInfo arq)
        {

            if (this.bAtivaLog)
            {
                fLog(arq.Name, "Função FPegaIcms item " + item);
            }

            XmlNamespaceManager ns = new XmlNamespaceManager(xmlDoc.NameTable);
            ns.AddNamespace("nfe", "http://www.portalfiscal.inf.br/nfe");
            XPathNavigator xpathNav = xmlDoc.CreateNavigator();
            XPathNavigator node00;
            XPathNavigator node10;
            XPathNavigator node20;
            XPathNavigator node30;
            XPathNavigator node40;
            XPathNavigator node41;
            XPathNavigator node50;
            XPathNavigator node51;
            XPathNavigator node60;
            XPathNavigator node70;
            XPathNavigator node90;
            string sResp = "";

            try
            {
                node00 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS00/nfe:orig", ns);
                if (node00 != null) { sResp = "00"; }
            }
            catch { }
            try
            {
                node10 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS10/nfe:orig", ns);
                if (node10 != null)
                {
                    sResp = sResp.Trim() == "" ? "10" : sResp + ";10";
                }
            }
            catch { }
            try
            {
                node20 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS20/nfe:orig", ns);
                if (node20 != null)
                {
                    sResp = sResp.Trim() == "" ? "20" : sResp + ";20";
                }
            }
            catch { }
            try
            {
                node30 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS30/nfe:orig", ns);
                if (node30 != null)
                {
                    sResp = sResp.Trim() == "" ? "30" : sResp + ";30";
                }
            }
            catch { }
            try
            {
                node40 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS40/nfe:orig", ns);
                if (node40 != null)
                {
                    sResp = sResp.Trim() == "" ? "40" : sResp + ";40";
                }
            }
            catch { }
            try
            {
                node41 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS41/nfe:orig", ns);
                if (node41 != null)
                {
                    sResp = sResp.Trim() == "" ? "41" : sResp + ";41";
                }
            }
            catch { }
            try
            {
                node50 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS50/nfe:orig", ns);
                if (node50 != null)
                {
                    sResp = sResp.Trim() == "" ? "50" : sResp + ";50";
                }
            }
            catch { }
            try
            {
                node51 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS51/nfe:orig", ns);
                if (node51 != null)
                {
                    sResp = sResp.Trim() == "" ? "51" : sResp + ";51";
                }
            }
            catch { }
            try
            {
                node60 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS60/nfe:orig", ns);
                if (node60 != null)
                {
                    sResp = sResp.Trim() == "" ? "60" : sResp + ";60";
                }
            }
            catch { }
            try
            {
                node70 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS70/nfe:orig", ns);
                if (node70 != null)
                {
                    sResp = sResp.Trim() == "" ? "70" : sResp + ";70";
                }
            }
            catch { }
            try
            {
                node90 = xpathNav.SelectSingleNode("//nfe:infNFe/nfe:det[" + item + "]/nfe:imposto/nfe:ICMS/nfe:ICMS90/nfe:orig", ns);
                if (node90 != null)
                {
                    sResp = sResp.Trim() == "" ? "90" : sResp + ";90";
                }
            }
            catch { }
            return sResp;
        }

    }
}
