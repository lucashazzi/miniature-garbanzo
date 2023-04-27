namespace ProcessaXML
{
    class itensNfe
    {
        private string idprd;
        private string idnat;
        private string idnatPedido;
        private string quantidade;
        private string codUnd;
        private string codProduto;
        private string nomeProduto;
        private string valorProduto;
        private string totalProduto;
        private string valorIcms;
        private string unidadeNota;
        private string baseIcms;
        private string aliqIcms;
        private string cstIcms;
        private string valorIcmsSt;
        private string baseIcmsSt;
        private string aliqIcmsSt;
        private string valorIpi;
        private string baseIpi;
        private string aliqIpi;
        private string cstIpi;
        private string valorPis;
        private string basePis;
        private string aliqPis;
        private string cstPis;
        private string valorCofins;
        private string baseCofins;
        private string aliqCofins;
        private string cstCofins;
        private string baseRed;
        private string baseRedIcmsST;
        private string mva;
        private string aliqIss;
        private string valorIss;
        private string aliqInss;
        private string valorInss;
        private string aliqCsll;
        private string valorCsll;
        private string aliqIr;
        private string valorIr;
        private int idmov;
        private int nseq;
        private int nseqNota;
        private string pedidoDeCompra;
        private string numLote;
        private int idLote;
        private string fabLote;
        private string valLote;
        private string estoqueAlimentar;
        private string estoqueMaisBrasil;
        private string estoqueVaiVarejo;
        private string estoqueSobra;
        private string codcfoAssossiado;
        private string vSeguro;
        private string vFrete;
        private string vOutros;
        private string vDesconto;
        private string fatorPedido;
        private string fatorNf;
        private string quantidadePedido;
        private double quantidadeEstoqueNota;
        private double quantidadeEstoquePedido;
        private double porcAlimentar;
        private double porcMaisBrasil;
        private double porcVaiVarejo;
        private string quantidadeBaixar;
        private string codigoPrd;
        private double precoPedido;
        private string tipoPrdFiscal;
        private double precoUnitario;
        private string comprador;
        private string idnatCapa;
        private string idnatCapa2;
        private string estadoPedido;

        public string Idprd { get => idprd; set => idprd = value; }
        public string Idnat { get => idnat; set => idnat = value; }
        public string Quantidade { get => quantidade; set => quantidade = value; }
        public string CodUnd { get => codUnd; set => codUnd = value; }
        public string ValorProduto { get => valorProduto; set => valorProduto = value; }
        public string TotalProduto { get => totalProduto; set => totalProduto = value; }
        public string ValorIcms { get => valorIcms; set => valorIcms = value; }
        public string BaseIcms { get => baseIcms; set => baseIcms = value; }
        public string AliqIcms { get => aliqIcms; set => aliqIcms = value; }
        public string CstIcms { get => cstIcms; set => cstIcms = value; }
        public string ValorIcmsSt { get => valorIcmsSt; set => valorIcmsSt = value; }
        public string BaseIcmsSt { get => baseIcmsSt; set => baseIcmsSt = value; }
        public string AliqIcmsSt { get => aliqIcmsSt; set => aliqIcmsSt = value; }
        public string ValorIpi { get => valorIpi; set => valorIpi = value; }
        public string BaseIpi { get => baseIpi; set => baseIpi = value; }
        public string AliqIpi { get => aliqIpi; set => aliqIpi = value; }
        public string CstIpi { get => cstIpi; set => cstIpi = value; }
        public string ValorPis { get => valorPis; set => valorPis = value; }
        public string BasePis { get => basePis; set => basePis = value; }
        public string AliqPis { get => aliqPis; set => aliqPis = value; }
        public string CstPis { get => cstPis; set => cstPis = value; }
        public string ValorCofins { get => valorCofins; set => valorCofins = value; }
        public string BaseCofins { get => baseCofins; set => baseCofins = value; }
        public string AliqCofins { get => aliqCofins; set => aliqCofins = value; }
        public string CstCofins { get => cstCofins; set => cstCofins = value; }
        public string BaseRed { get => baseRed; set => baseRed = value; }
        public string BaseRedIcmsST { get => baseRedIcmsST; set => baseRedIcmsST = value; }
        public string Mva { get => mva; set => mva = value; }
        public string AliqIss { get => aliqIss; set => aliqIss = value; }
        public string ValorIss { get => valorIss; set => valorIss = value; }
        public string AliqInss { get => aliqInss; set => aliqInss = value; }
        public string ValorInss { get => valorInss; set => valorInss = value; }
        public string AliqCsll { get => aliqCsll; set => aliqCsll = value; }
        public string ValorCsll { get => valorCsll; set => valorCsll = value; }
        public string AliqIr { get => aliqIr; set => aliqIr = value; }
        public string ValorIr { get => valorIr; set => valorIr = value; }
        public int Idmov { get => idmov; set => idmov = value; }
        public int Nseq { get => nseq; set => nseq = value; }
        public int NseqNota { get => nseqNota; set => nseqNota = value; }
        public string PedidoDeCompra { get => pedidoDeCompra; set => pedidoDeCompra = value; }
        public string NumLote { get => numLote; set => numLote = value; }
        public int IdLote { get => idLote; set => idLote = value; }
        public string ValLote { get => valLote; set => valLote = value; }
        public string FabLote { get => fabLote; set => fabLote = value; }
        public string CodProduto { get => codProduto; set => codProduto = value; }
        public string NomeProduto { get => nomeProduto; set => nomeProduto = value; }
        public string EstoqueAlimentar { get => estoqueAlimentar; set => estoqueAlimentar = value; }
        public string EstoqueMaisBrasil { get => estoqueMaisBrasil; set => estoqueMaisBrasil = value; }
        public string EstoqueVaiVarejo { get => estoqueVaiVarejo; set => estoqueVaiVarejo = value; }
        public string EstoqueSobra { get => estoqueSobra; set => estoqueSobra = value; }
        public string IdnatPedido { get => idnatPedido; set => idnatPedido = value; }
        public string UnidadeNota { get => unidadeNota; set => unidadeNota = value; }
        public string CodcfoAssossiado { get => codcfoAssossiado; set => codcfoAssossiado = value; }
        public string VSeguro { get => vSeguro; set => vSeguro = value; }
        public string VFrete { get => vFrete; set => vFrete = value; }
        public string VOutros { get => vOutros; set => vOutros = value; }
        public string VDesconto { get => vDesconto; set => vDesconto = value; }
        public string FatorPedido { get => fatorPedido; set => fatorPedido = value; }
        public string FatorNf { get => fatorNf; set => fatorNf = value; }
        public string QuantidadePedido { get => quantidadePedido; set => quantidadePedido = value; }
        public double QuantidadeEstoqueNota { get => quantidadeEstoqueNota; set => quantidadeEstoqueNota = value; }
        public double QuantidadeEstoquePedido { get => quantidadeEstoquePedido; set => quantidadeEstoquePedido = value; }
        public double PorcAlimentar { get => porcAlimentar; set => porcAlimentar = value; }
        public double PorcMaisBrasil { get => porcMaisBrasil; set => porcMaisBrasil = value; }
        public double PorcVaiVarejo { get => porcVaiVarejo; set => porcVaiVarejo = value; }
        public string QuantidadeBaixar { get => quantidadeBaixar; set => quantidadeBaixar = value; }
        public string CodigoPrd { get => codigoPrd; set => codigoPrd = value; }
        public double PrecoPedido { get => precoPedido; set => precoPedido = value; }
        public string TipoPrdFiscal { get => tipoPrdFiscal; set => tipoPrdFiscal = value; }
        public double PrecoUnitario { get => precoUnitario; set => precoUnitario = value; }
        public string Comprador { get => comprador; set => comprador = value; }
        public string IdnatCapa2 { get => idnatCapa2; set => idnatCapa2 = value; }
        public string IdnatCapa { get => idnatCapa; set => idnatCapa = value; }
        public string EstadoPedido { get => estadoPedido; set => estadoPedido = value; }
    }
}
