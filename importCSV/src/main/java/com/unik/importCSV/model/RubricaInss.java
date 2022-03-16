package com.unik.importCSV.model;

import java.math.BigDecimal;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 *
 * @author Carlos
 */
@Entity
@Table(name="RUBRICA_INSS")
public class RubricaInss {

    @Id
    @Basic(optional=false)
    @Column(name="nu_nb", nullable=false, length=15)
    private String nuNb;
    private String idOlConcessao;
    private String idOlManutencao;
    private String idOlManutAnt;
    private String csPa;
    private BigDecimal vlMrAtu;
    private BigDecimal vlRmi;
    private String csTratamento;
    private String csEspecie;
    private String csRamoAtividade;
    private String csFormaFiliacao;
    private String csDocEmpregador;
    private String nuDocEmpregador;
    private String nuNbAnt;
    private String d2Der;
    private String d2Dib;
    private String d2Ddb;
    private String d2Dcb;
    private String d2Dip;
    private String d2IniIncapac;
    private String d2InicioDoenca;
    private String d2Drd;
    private String d2ObitoReclusao;
    private String csClientela;
    private String nuMatrConcessor;
    private String nuMatrHabilitador;
    private String csSituacaoBenef;
    private String idBanco;
    private String idOrgaoPagador;
    private String csMeioPagto;
    private String nuAgenciaPag;
    private String nuContaCorrente;
    private String csDiagnosticoN;
    private String csDiagnostico1;
    private String nuMatrMp1;
    private String nuMatrMp2;
    private String d2FormatConc;
    private String csDespacho;
    private String dtDiaUtilPagto;
    private String nmRecebedor;
    private String dnRecebedor;
    private String nuCpfR;
    private String csSexoR;
    private String nmTitularBenefT;
    private String nmMaeT;
    private String nuCpfT;
    private String idNitT;
    private String dtNascimentoT;
    private String ctpsT;
    private String ctpsSerieT;
    private String ctpsUfT;
    private String nuIdentidadeT;
    private String identidadeUfT;
    private String csEmissorT;
    private String nuTitEleitor;
    private String csValCnis;
    private String csSexoT;
    private String teEnderecoT;
    private String nmBairroT;
    private String nuCepT;
    private String nuDddT;
    private String nuTelefoneT;
    private String idMunSinpasT;
    private String idMunIbgeT;
    private String nmMunicipioT;
    private String nmUfMunicipioT;
    private String d2ObitoT;
    private String nmInstituidorI;
    private String nmMaeI;
    private String nuCpfI;
    private String idNitI;
    private String dtNascimentoI;
    private String ctpsI;
    private String ctpsSerieI;
    private String ctpsUfI;
    private String nuIdentidadeI;
    private String identidadeUfI;
    private String csEmissorI;
    private String nuTitEleitorI;
    private String csValCnisI;
    private String csSexoI;
    private String d2ObitoI;
    private String nmProcuradorP;
    private String nmMaeP;
    private String nuCpfP;
    private String idNitP;
    private String dtNascimentoP;
    private String ctpsP;
    private String ctpsSerieP;
    private String ctpsUfP;
    private String nuIdentidadeP;
    private String identidadeUfP;
    private String csEmissorP;
    private String csSexoP;
    private String nmBairroP;
    private String nuCepP;
    private String teEnderecoP;
    private String nmMunicipioP;
    private String nmUfMunicipioP;
    private String municipNascP;
    private String nmRepresentanteR;
    private String nmMaeR;
    private String idNitR;
    private String dtNascimentoR;
    private String ctpsR;
    private String ctpsSerieR;
    private String ctpsUfR;
    private String nuIdentidadeR;
    private String identidadeUfR;
    private String csEmissorR;
    private String csTipoR;
    private Integer qtDepIr;
    private Integer qtDepValNb;
    private Integer qtDepCadastro;
    private Integer qtRubricaReg;
    private Integer csRubrica1;
    private Integer csRubrica2;
    private Integer csRubrica3;
    private Integer csRubrica4;
    private Integer csRubrica5;
    private Integer csRubrica6;
    private Integer csRubrica7;
    private Integer csRubrica8;
    private Integer csRubrica9;
    private Integer csRubrica10;
    private BigDecimal vlRubrica1;
    private BigDecimal vlRubrica2;
    private BigDecimal vlRubrica3;
    private BigDecimal vlRubrica4;
    private BigDecimal vlRubrica5;
    private BigDecimal vlRubrica6;
    private BigDecimal vlRubrica7;
    private BigDecimal vlRubrica8;
    private BigDecimal vlRubrica9;
    private BigDecimal vlRubrica10;
    private BigDecimal vlBruto;
    private BigDecimal totDescontos;
    private BigDecimal vlLiquido;
    private String nuCpf;
    private String csSexo;
    private String dtUltimaAlter;
    private String d2Limite;
    private String dsErro;
    private String dtAtualizacaoEtl;
    private String nmArquivo;
    private String anoMesRef;
    private String dtUltimaPericia;
    private String faseUltimaPericia;

    public RubricaInss() {
    	
    }

    
    
    public RubricaInss(String nuNb, String idOlConcessao, String idOlManutencao, String idOlManutAnt, String csPa,
			BigDecimal vlMrAtu, BigDecimal vlRmi, String csTratamento, String csEspecie, String csRamoAtividade,
			String csFormaFiliacao, String csDocEmpregador, String nuDocEmpregador, String nuNbAnt, String d2Der,
			String d2Dib, String d2Ddb, String d2Dcb, String d2Dip, String d2IniIncapac, String d2InicioDoenca,
			String d2Drd, String d2ObitoReclusao, String csClientela, String nuMatrConcessor, String nuMatrHabilitador,
			String csSituacaoBenef, String idBanco, String idOrgaoPagador, String csMeioPagto, String nuAgenciaPag,
			String nuContaCorrente, String csDiagnosticoN, String csDiagnostico1, String nuMatrMp1, String nuMatrMp2,
			String d2FormatConc, String csDespacho, String dtDiaUtilPagto, String nmRecebedor, String dnRecebedor,
			String nuCpfR, String csSexoR, String nmTitularBenefT, String nmMaeT, String nuCpfT, String idNitT,
			String dtNascimentoT, String ctpsT, String ctpsSerieT, String ctpsUfT, String nuIdentidadeT,
			String identidadeUfT, String csEmissorT, String nuTitEleitor, String csValCnis, String csSexoT,
			String teEnderecoT, String nmBairroT, String nuCepT, String nuDddT, String nuTelefoneT, String idMunSinpasT,
			String idMunIbgeT, String nmMunicipioT, String nmUfMunicipioT, String d2ObitoT, String nmInstituidorI,
			String nmMaeI, String nuCpfI, String idNitI, String dtNascimentoI, String ctpsI, String ctpsSerieI,
			String ctpsUfI, String nuIdentidadeI, String identidadeUfI, String csEmissorI, String nuTitEleitorI,
			String csValCnisI, String csSexoI, String d2ObitoI, String nmProcuradorP, String nmMaeP, String nuCpfP,
			String idNitP, String dtNascimentoP, String ctpsP, String ctpsSerieP, String ctpsUfP, String nuIdentidadeP,
			String identidadeUfP, String csEmissorP, String csSexoP, String nmBairroP, String nuCepP,
			String teEnderecoP, String nmMunicipioP, String nmUfMunicipioP, String municipNascP,
			String nmRepresentanteR, String nmMaeR, String idNitR, String dtNascimentoR, String ctpsR,
			String ctpsSerieR, String ctpsUfR, String nuIdentidadeR, String identidadeUfR, String csEmissorR,
			String csTipoR, Integer qtDepIr, Integer qtDepValNb, Integer qtDepCadastro, Integer qtRubricaReg,
			Integer csRubrica1, Integer csRubrica2, Integer csRubrica3, Integer csRubrica4, Integer csRubrica5,
			Integer csRubrica6, Integer csRubrica7, Integer csRubrica8, Integer csRubrica9, Integer csRubrica10,
			BigDecimal vlRubrica1, BigDecimal vlRubrica2, BigDecimal vlRubrica3, BigDecimal vlRubrica4,
			BigDecimal vlRubrica5, BigDecimal vlRubrica6, BigDecimal vlRubrica7, BigDecimal vlRubrica8,
			BigDecimal vlRubrica9, BigDecimal vlRubrica10, BigDecimal vlBruto, BigDecimal totDescontos,
			BigDecimal vlLiquido, String nuCpf, String csSexo, String dtUltimaAlter, String d2Limite, String dsErro,
			String dtAtualizacaoEtl, String nmArquivo, String anoMesRef, String dtUltimaPericia,
			String faseUltimaPericia) {
		super();
		this.nuNb = nuNb;
		this.idOlConcessao = idOlConcessao;
		this.idOlManutencao = idOlManutencao;
		this.idOlManutAnt = idOlManutAnt;
		this.csPa = csPa;
		this.vlMrAtu = vlMrAtu;
		this.vlRmi = vlRmi;
		this.csTratamento = csTratamento;
		this.csEspecie = csEspecie;
		this.csRamoAtividade = csRamoAtividade;
		this.csFormaFiliacao = csFormaFiliacao;
		this.csDocEmpregador = csDocEmpregador;
		this.nuDocEmpregador = nuDocEmpregador;
		this.nuNbAnt = nuNbAnt;
		this.d2Der = d2Der;
		this.d2Dib = d2Dib;
		this.d2Ddb = d2Ddb;
		this.d2Dcb = d2Dcb;
		this.d2Dip = d2Dip;
		this.d2IniIncapac = d2IniIncapac;
		this.d2InicioDoenca = d2InicioDoenca;
		this.d2Drd = d2Drd;
		this.d2ObitoReclusao = d2ObitoReclusao;
		this.csClientela = csClientela;
		this.nuMatrConcessor = nuMatrConcessor;
		this.nuMatrHabilitador = nuMatrHabilitador;
		this.csSituacaoBenef = csSituacaoBenef;
		this.idBanco = idBanco;
		this.idOrgaoPagador = idOrgaoPagador;
		this.csMeioPagto = csMeioPagto;
		this.nuAgenciaPag = nuAgenciaPag;
		this.nuContaCorrente = nuContaCorrente;
		this.csDiagnosticoN = csDiagnosticoN;
		this.csDiagnostico1 = csDiagnostico1;
		this.nuMatrMp1 = nuMatrMp1;
		this.nuMatrMp2 = nuMatrMp2;
		this.d2FormatConc = d2FormatConc;
		this.csDespacho = csDespacho;
		this.dtDiaUtilPagto = dtDiaUtilPagto;
		this.nmRecebedor = nmRecebedor;
		this.dnRecebedor = dnRecebedor;
		this.nuCpfR = nuCpfR;
		this.csSexoR = csSexoR;
		this.nmTitularBenefT = nmTitularBenefT;
		this.nmMaeT = nmMaeT;
		this.nuCpfT = nuCpfT;
		this.idNitT = idNitT;
		this.dtNascimentoT = dtNascimentoT;
		this.ctpsT = ctpsT;
		this.ctpsSerieT = ctpsSerieT;
		this.ctpsUfT = ctpsUfT;
		this.nuIdentidadeT = nuIdentidadeT;
		this.identidadeUfT = identidadeUfT;
		this.csEmissorT = csEmissorT;
		this.nuTitEleitor = nuTitEleitor;
		this.csValCnis = csValCnis;
		this.csSexoT = csSexoT;
		this.teEnderecoT = teEnderecoT;
		this.nmBairroT = nmBairroT;
		this.nuCepT = nuCepT;
		this.nuDddT = nuDddT;
		this.nuTelefoneT = nuTelefoneT;
		this.idMunSinpasT = idMunSinpasT;
		this.idMunIbgeT = idMunIbgeT;
		this.nmMunicipioT = nmMunicipioT;
		this.nmUfMunicipioT = nmUfMunicipioT;
		this.d2ObitoT = d2ObitoT;
		this.nmInstituidorI = nmInstituidorI;
		this.nmMaeI = nmMaeI;
		this.nuCpfI = nuCpfI;
		this.idNitI = idNitI;
		this.dtNascimentoI = dtNascimentoI;
		this.ctpsI = ctpsI;
		this.ctpsSerieI = ctpsSerieI;
		this.ctpsUfI = ctpsUfI;
		this.nuIdentidadeI = nuIdentidadeI;
		this.identidadeUfI = identidadeUfI;
		this.csEmissorI = csEmissorI;
		this.nuTitEleitorI = nuTitEleitorI;
		this.csValCnisI = csValCnisI;
		this.csSexoI = csSexoI;
		this.d2ObitoI = d2ObitoI;
		this.nmProcuradorP = nmProcuradorP;
		this.nmMaeP = nmMaeP;
		this.nuCpfP = nuCpfP;
		this.idNitP = idNitP;
		this.dtNascimentoP = dtNascimentoP;
		this.ctpsP = ctpsP;
		this.ctpsSerieP = ctpsSerieP;
		this.ctpsUfP = ctpsUfP;
		this.nuIdentidadeP = nuIdentidadeP;
		this.identidadeUfP = identidadeUfP;
		this.csEmissorP = csEmissorP;
		this.csSexoP = csSexoP;
		this.nmBairroP = nmBairroP;
		this.nuCepP = nuCepP;
		this.teEnderecoP = teEnderecoP;
		this.nmMunicipioP = nmMunicipioP;
		this.nmUfMunicipioP = nmUfMunicipioP;
		this.municipNascP = municipNascP;
		this.nmRepresentanteR = nmRepresentanteR;
		this.nmMaeR = nmMaeR;
		this.idNitR = idNitR;
		this.dtNascimentoR = dtNascimentoR;
		this.ctpsR = ctpsR;
		this.ctpsSerieR = ctpsSerieR;
		this.ctpsUfR = ctpsUfR;
		this.nuIdentidadeR = nuIdentidadeR;
		this.identidadeUfR = identidadeUfR;
		this.csEmissorR = csEmissorR;
		this.csTipoR = csTipoR;
		this.qtDepIr = qtDepIr;
		this.qtDepValNb = qtDepValNb;
		this.qtDepCadastro = qtDepCadastro;
		this.qtRubricaReg = qtRubricaReg;
		this.csRubrica1 = csRubrica1;
		this.csRubrica2 = csRubrica2;
		this.csRubrica3 = csRubrica3;
		this.csRubrica4 = csRubrica4;
		this.csRubrica5 = csRubrica5;
		this.csRubrica6 = csRubrica6;
		this.csRubrica7 = csRubrica7;
		this.csRubrica8 = csRubrica8;
		this.csRubrica9 = csRubrica9;
		this.csRubrica10 = csRubrica10;
		this.vlRubrica1 = vlRubrica1;
		this.vlRubrica2 = vlRubrica2;
		this.vlRubrica3 = vlRubrica3;
		this.vlRubrica4 = vlRubrica4;
		this.vlRubrica5 = vlRubrica5;
		this.vlRubrica6 = vlRubrica6;
		this.vlRubrica7 = vlRubrica7;
		this.vlRubrica8 = vlRubrica8;
		this.vlRubrica9 = vlRubrica9;
		this.vlRubrica10 = vlRubrica10;
		this.vlBruto = vlBruto;
		this.totDescontos = totDescontos;
		this.vlLiquido = vlLiquido;
		this.nuCpf = nuCpf;
		this.csSexo = csSexo;
		this.dtUltimaAlter = dtUltimaAlter;
		this.d2Limite = d2Limite;
		this.dsErro = dsErro;
		this.dtAtualizacaoEtl = dtAtualizacaoEtl;
		this.nmArquivo = nmArquivo;
		this.anoMesRef = anoMesRef;
		this.dtUltimaPericia = dtUltimaPericia;
		this.faseUltimaPericia = faseUltimaPericia;
	}



	public RubricaInss(String nuNb) {
        this.nuNb = nuNb;
    }

    public String getNuNb() {
        return nuNb;
    }

    public void setNuNb(String nuNb) {
        this.nuNb = nuNb;
    }

    public String getIdOlConcessao() {
        return idOlConcessao;
    }

    public void setIdOlConcessao(String idOlConcessao) {
        this.idOlConcessao = idOlConcessao;
    }

    public String getIdOlManutencao() {
        return idOlManutencao;
    }

    public void setIdOlManutencao(String idOlManutencao) {
        this.idOlManutencao = idOlManutencao;
    }

    public String getIdOlManutAnt() {
        return idOlManutAnt;
    }

    public void setIdOlManutAnt(String idOlManutAnt) {
        this.idOlManutAnt = idOlManutAnt;
    }

    public String getCsPa() {
        return csPa;
    }

    public void setCsPa(String csPa) {
        this.csPa = csPa;
    }

    public BigDecimal getVlMrAtu() {
        return vlMrAtu;
    }

    public void setVlMrAtu(BigDecimal vlMrAtu) {
        this.vlMrAtu = vlMrAtu;
    }

    public BigDecimal getVlRmi() {
        return vlRmi;
    }

    public void setVlRmi(BigDecimal vlRmi) {
        this.vlRmi = vlRmi;
    }

    public String getCsTratamento() {
        return csTratamento;
    }

    public void setCsTratamento(String csTratamento) {
        this.csTratamento = csTratamento;
    }

    public String getCsEspecie() {
        return csEspecie;
    }

    public void setCsEspecie(String csEspecie) {
        this.csEspecie = csEspecie;
    }

    public String getCsRamoAtividade() {
        return csRamoAtividade;
    }

    public void setCsRamoAtividade(String csRamoAtividade) {
        this.csRamoAtividade = csRamoAtividade;
    }

    public String getCsFormaFiliacao() {
        return csFormaFiliacao;
    }

    public void setCsFormaFiliacao(String csFormaFiliacao) {
        this.csFormaFiliacao = csFormaFiliacao;
    }

    public String getCsDocEmpregador() {
        return csDocEmpregador;
    }

    public void setCsDocEmpregador(String csDocEmpregador) {
        this.csDocEmpregador = csDocEmpregador;
    }

    public String getNuDocEmpregador() {
        return nuDocEmpregador;
    }

    public void setNuDocEmpregador(String nuDocEmpregador) {
        this.nuDocEmpregador = nuDocEmpregador;
    }

    public String getNuNbAnt() {
        return nuNbAnt;
    }

    public void setNuNbAnt(String nuNbAnt) {
        this.nuNbAnt = nuNbAnt;
    }

    public String getD2Der() {
        return d2Der;
    }

    public void setD2Der(String d2Der) {
        this.d2Der = d2Der;
    }

    public String getD2Dib() {
        return d2Dib;
    }

    public void setD2Dib(String d2Dib) {
        this.d2Dib = d2Dib;
    }

    public String getD2Ddb() {
        return d2Ddb;
    }

    public void setD2Ddb(String d2Ddb) {
        this.d2Ddb = d2Ddb;
    }

    public String getD2Dcb() {
        return d2Dcb;
    }

    public void setD2Dcb(String d2Dcb) {
        this.d2Dcb = d2Dcb;
    }

    public String getD2Dip() {
        return d2Dip;
    }

    public void setD2Dip(String d2Dip) {
        this.d2Dip = d2Dip;
    }

    public String getD2IniIncapac() {
        return d2IniIncapac;
    }

    public void setD2IniIncapac(String d2IniIncapac) {
        this.d2IniIncapac = d2IniIncapac;
    }

    public String getD2InicioDoenca() {
        return d2InicioDoenca;
    }

    public void setD2InicioDoenca(String d2InicioDoenca) {
        this.d2InicioDoenca = d2InicioDoenca;
    }

    public String getD2Drd() {
        return d2Drd;
    }

    public void setD2Drd(String d2Drd) {
        this.d2Drd = d2Drd;
    }

    public String getD2ObitoReclusao() {
        return d2ObitoReclusao;
    }

    public void setD2ObitoReclusao(String d2ObitoReclusao) {
        this.d2ObitoReclusao = d2ObitoReclusao;
    }

    public String getCsClientela() {
        return csClientela;
    }

    public void setCsClientela(String csClientela) {
        this.csClientela = csClientela;
    }

    public String getNuMatrConcessor() {
        return nuMatrConcessor;
    }

    public void setNuMatrConcessor(String nuMatrConcessor) {
        this.nuMatrConcessor = nuMatrConcessor;
    }

    public String getNuMatrHabilitador() {
        return nuMatrHabilitador;
    }

    public void setNuMatrHabilitador(String nuMatrHabilitador) {
        this.nuMatrHabilitador = nuMatrHabilitador;
    }

    public String getCsSituacaoBenef() {
        return csSituacaoBenef;
    }

    public void setCsSituacaoBenef(String csSituacaoBenef) {
        this.csSituacaoBenef = csSituacaoBenef;
    }

    public String getIdBanco() {
        return idBanco;
    }

    public void setIdBanco(String idBanco) {
        this.idBanco = idBanco;
    }

    public String getIdOrgaoPagador() {
        return idOrgaoPagador;
    }

    public void setIdOrgaoPagador(String idOrgaoPagador) {
        this.idOrgaoPagador = idOrgaoPagador;
    }

    public String getCsMeioPagto() {
        return csMeioPagto;
    }

    public void setCsMeioPagto(String csMeioPagto) {
        this.csMeioPagto = csMeioPagto;
    }

    public String getNuAgenciaPag() {
        return nuAgenciaPag;
    }

    public void setNuAgenciaPag(String nuAgenciaPag) {
        this.nuAgenciaPag = nuAgenciaPag;
    }

    public String getNuContaCorrente() {
        return nuContaCorrente;
    }

    public void setNuContaCorrente(String nuContaCorrente) {
        this.nuContaCorrente = nuContaCorrente;
    }

    public String getCsDiagnosticoN() {
        return csDiagnosticoN;
    }

    public void setCsDiagnosticoN(String csDiagnosticoN) {
        this.csDiagnosticoN = csDiagnosticoN;
    }

    public String getCsDiagnostico1() {
        return csDiagnostico1;
    }

    public void setCsDiagnostico1(String csDiagnostico1) {
        this.csDiagnostico1 = csDiagnostico1;
    }

    public String getNuMatrMp1() {
        return nuMatrMp1;
    }

    public void setNuMatrMp1(String nuMatrMp1) {
        this.nuMatrMp1 = nuMatrMp1;
    }

    public String getNuMatrMp2() {
        return nuMatrMp2;
    }

    public void setNuMatrMp2(String nuMatrMp2) {
        this.nuMatrMp2 = nuMatrMp2;
    }

    public String getD2FormatConc() {
        return d2FormatConc;
    }

    public void setD2FormatConc(String d2FormatConc) {
        this.d2FormatConc = d2FormatConc;
    }

    public String getCsDespacho() {
        return csDespacho;
    }

    public void setCsDespacho(String csDespacho) {
        this.csDespacho = csDespacho;
    }

    public String getDtDiaUtilPagto() {
        return dtDiaUtilPagto;
    }

    public void setDtDiaUtilPagto(String dtDiaUtilPagto) {
        this.dtDiaUtilPagto = dtDiaUtilPagto;
    }

    public String getNmRecebedor() {
        return nmRecebedor;
    }

    public void setNmRecebedor(String nmRecebedor) {
        this.nmRecebedor = nmRecebedor;
    }

    public String getDnRecebedor() {
        return dnRecebedor;
    }

    public void setDnRecebedor(String dnRecebedor) {
        this.dnRecebedor = dnRecebedor;
    }

    public String getNuCpfR() {
        return nuCpfR;
    }

    public void setNuCpfR(String nuCpfR) {
        this.nuCpfR = nuCpfR;
    }

    public String getCsSexoR() {
        return csSexoR;
    }

    public void setCsSexoR(String csSexoR) {
        this.csSexoR = csSexoR;
    }

    public String getNmTitularBenefT() {
        return nmTitularBenefT;
    }

    public void setNmTitularBenefT(String nmTitularBenefT) {
        this.nmTitularBenefT = nmTitularBenefT;
    }

    public String getNmMaeT() {
        return nmMaeT;
    }

    public void setNmMaeT(String nmMaeT) {
        this.nmMaeT = nmMaeT;
    }

    public String getNuCpfT() {
        return nuCpfT;
    }

    public void setNuCpfT(String nuCpfT) {
        this.nuCpfT = nuCpfT;
    }

    public String getIdNitT() {
        return idNitT;
    }

    public void setIdNitT(String idNitT) {
        this.idNitT = idNitT;
    }

    public String getDtNascimentoT() {
        return dtNascimentoT;
    }

    public void setDtNascimentoT(String dtNascimentoT) {
        this.dtNascimentoT = dtNascimentoT;
    }

    public String getCtpsT() {
        return ctpsT;
    }

    public void setCtpsT(String ctpsT) {
        this.ctpsT = ctpsT;
    }

    public String getCtpsSerieT() {
        return ctpsSerieT;
    }

    public void setCtpsSerieT(String ctpsSerieT) {
        this.ctpsSerieT = ctpsSerieT;
    }

    public String getCtpsUfT() {
        return ctpsUfT;
    }

    public void setCtpsUfT(String ctpsUfT) {
        this.ctpsUfT = ctpsUfT;
    }

    public String getNuIdentidadeT() {
        return nuIdentidadeT;
    }

    public void setNuIdentidadeT(String nuIdentidadeT) {
        this.nuIdentidadeT = nuIdentidadeT;
    }

    public String getIdentidadeUfT() {
        return identidadeUfT;
    }

    public void setIdentidadeUfT(String identidadeUfT) {
        this.identidadeUfT = identidadeUfT;
    }

    public String getCsEmissorT() {
        return csEmissorT;
    }

    public void setCsEmissorT(String csEmissorT) {
        this.csEmissorT = csEmissorT;
    }

    public String getNuTitEleitor() {
        return nuTitEleitor;
    }

    public void setNuTitEleitor(String nuTitEleitor) {
        this.nuTitEleitor = nuTitEleitor;
    }

    public String getCsValCnis() {
        return csValCnis;
    }

    public void setCsValCnis(String csValCnis) {
        this.csValCnis = csValCnis;
    }

    public String getCsSexoT() {
        return csSexoT;
    }

    public void setCsSexoT(String csSexoT) {
        this.csSexoT = csSexoT;
    }

    public String getTeEnderecoT() {
        return teEnderecoT;
    }

    public void setTeEnderecoT(String teEnderecoT) {
        this.teEnderecoT = teEnderecoT;
    }

    public String getNmBairroT() {
        return nmBairroT;
    }

    public void setNmBairroT(String nmBairroT) {
        this.nmBairroT = nmBairroT;
    }

    public String getNuCepT() {
        return nuCepT;
    }

    public void setNuCepT(String nuCepT) {
        this.nuCepT = nuCepT;
    }

    public String getNuDddT() {
        return nuDddT;
    }

    public void setNuDddT(String nuDddT) {
        this.nuDddT = nuDddT;
    }

    public String getNuTelefoneT() {
        return nuTelefoneT;
    }

    public void setNuTelefoneT(String nuTelefoneT) {
        this.nuTelefoneT = nuTelefoneT;
    }

    public String getIdMunSinpasT() {
        return idMunSinpasT;
    }

    public void setIdMunSinpasT(String idMunSinpasT) {
        this.idMunSinpasT = idMunSinpasT;
    }

    public String getIdMunIbgeT() {
        return idMunIbgeT;
    }

    public void setIdMunIbgeT(String idMunIbgeT) {
        this.idMunIbgeT = idMunIbgeT;
    }

    public String getNmMunicipioT() {
        return nmMunicipioT;
    }

    public void setNmMunicipioT(String nmMunicipioT) {
        this.nmMunicipioT = nmMunicipioT;
    }

    public String getNmUfMunicipioT() {
        return nmUfMunicipioT;
    }

    public void setNmUfMunicipioT(String nmUfMunicipioT) {
        this.nmUfMunicipioT = nmUfMunicipioT;
    }

    public String getD2ObitoT() {
        return d2ObitoT;
    }

    public void setD2ObitoT(String d2ObitoT) {
        this.d2ObitoT = d2ObitoT;
    }

    public String getNmInstituidorI() {
        return nmInstituidorI;
    }

    public void setNmInstituidorI(String nmInstituidorI) {
        this.nmInstituidorI = nmInstituidorI;
    }

    public String getNmMaeI() {
        return nmMaeI;
    }

    public void setNmMaeI(String nmMaeI) {
        this.nmMaeI = nmMaeI;
    }

    public String getNuCpfI() {
        return nuCpfI;
    }

    public void setNuCpfI(String nuCpfI) {
        this.nuCpfI = nuCpfI;
    }

    public String getIdNitI() {
        return idNitI;
    }

    public void setIdNitI(String idNitI) {
        this.idNitI = idNitI;
    }

    public String getDtNascimentoI() {
        return dtNascimentoI;
    }

    public void setDtNascimentoI(String dtNascimentoI) {
        this.dtNascimentoI = dtNascimentoI;
    }

    public String getCtpsI() {
        return ctpsI;
    }

    public void setCtpsI(String ctpsI) {
        this.ctpsI = ctpsI;
    }

    public String getCtpsSerieI() {
        return ctpsSerieI;
    }

    public void setCtpsSerieI(String ctpsSerieI) {
        this.ctpsSerieI = ctpsSerieI;
    }

    public String getCtpsUfI() {
        return ctpsUfI;
    }

    public void setCtpsUfI(String ctpsUfI) {
        this.ctpsUfI = ctpsUfI;
    }

    public String getNuIdentidadeI() {
        return nuIdentidadeI;
    }

    public void setNuIdentidadeI(String nuIdentidadeI) {
        this.nuIdentidadeI = nuIdentidadeI;
    }

    public String getIdentidadeUfI() {
        return identidadeUfI;
    }

    public void setIdentidadeUfI(String identidadeUfI) {
        this.identidadeUfI = identidadeUfI;
    }

    public String getCsEmissorI() {
        return csEmissorI;
    }

    public void setCsEmissorI(String csEmissorI) {
        this.csEmissorI = csEmissorI;
    }

    public String getNuTitEleitorI() {
        return nuTitEleitorI;
    }

    public void setNuTitEleitorI(String nuTitEleitorI) {
        this.nuTitEleitorI = nuTitEleitorI;
    }

    public String getCsValCnisI() {
        return csValCnisI;
    }

    public void setCsValCnisI(String csValCnisI) {
        this.csValCnisI = csValCnisI;
    }

    public String getCsSexoI() {
        return csSexoI;
    }

    public void setCsSexoI(String csSexoI) {
        this.csSexoI = csSexoI;
    }

    public String getD2ObitoI() {
        return d2ObitoI;
    }

    public void setD2ObitoI(String d2ObitoI) {
        this.d2ObitoI = d2ObitoI;
    }

    public String getNmProcuradorP() {
        return nmProcuradorP;
    }

    public void setNmProcuradorP(String nmProcuradorP) {
        this.nmProcuradorP = nmProcuradorP;
    }

    public String getNmMaeP() {
        return nmMaeP;
    }

    public void setNmMaeP(String nmMaeP) {
        this.nmMaeP = nmMaeP;
    }

    public String getNuCpfP() {
        return nuCpfP;
    }

    public void setNuCpfP(String nuCpfP) {
        this.nuCpfP = nuCpfP;
    }

    public String getIdNitP() {
        return idNitP;
    }

    public void setIdNitP(String idNitP) {
        this.idNitP = idNitP;
    }

    public String getDtNascimentoP() {
        return dtNascimentoP;
    }

    public void setDtNascimentoP(String dtNascimentoP) {
        this.dtNascimentoP = dtNascimentoP;
    }

    public String getCtpsP() {
        return ctpsP;
    }

    public void setCtpsP(String ctpsP) {
        this.ctpsP = ctpsP;
    }

    public String getCtpsSerieP() {
        return ctpsSerieP;
    }

    public void setCtpsSerieP(String ctpsSerieP) {
        this.ctpsSerieP = ctpsSerieP;
    }

    public String getCtpsUfP() {
        return ctpsUfP;
    }

    public void setCtpsUfP(String ctpsUfP) {
        this.ctpsUfP = ctpsUfP;
    }

    public String getNuIdentidadeP() {
        return nuIdentidadeP;
    }

    public void setNuIdentidadeP(String nuIdentidadeP) {
        this.nuIdentidadeP = nuIdentidadeP;
    }

    public String getIdentidadeUfP() {
        return identidadeUfP;
    }

    public void setIdentidadeUfP(String identidadeUfP) {
        this.identidadeUfP = identidadeUfP;
    }

    public String getCsEmissorP() {
        return csEmissorP;
    }

    public void setCsEmissorP(String csEmissorP) {
        this.csEmissorP = csEmissorP;
    }

    public String getCsSexoP() {
        return csSexoP;
    }

    public void setCsSexoP(String csSexoP) {
        this.csSexoP = csSexoP;
    }

    public String getNmBairroP() {
        return nmBairroP;
    }

    public void setNmBairroP(String nmBairroP) {
        this.nmBairroP = nmBairroP;
    }

    public String getNuCepP() {
        return nuCepP;
    }

    public void setNuCepP(String nuCepP) {
        this.nuCepP = nuCepP;
    }

    public String getTeEnderecoP() {
        return teEnderecoP;
    }

    public void setTeEnderecoP(String teEnderecoP) {
        this.teEnderecoP = teEnderecoP;
    }

    public String getNmMunicipioP() {
        return nmMunicipioP;
    }

    public void setNmMunicipioP(String nmMunicipioP) {
        this.nmMunicipioP = nmMunicipioP;
    }

    public String getNmUfMunicipioP() {
        return nmUfMunicipioP;
    }

    public void setNmUfMunicipioP(String nmUfMunicipioP) {
        this.nmUfMunicipioP = nmUfMunicipioP;
    }

    public String getMunicipNascP() {
        return municipNascP;
    }

    public void setMunicipNascP(String municipNascP) {
        this.municipNascP = municipNascP;
    }

    public String getNmRepresentanteR() {
        return nmRepresentanteR;
    }

    public void setNmRepresentanteR(String nmRepresentanteR) {
        this.nmRepresentanteR = nmRepresentanteR;
    }

    public String getNmMaeR() {
        return nmMaeR;
    }

    public void setNmMaeR(String nmMaeR) {
        this.nmMaeR = nmMaeR;
    }

    public String getIdNitR() {
        return idNitR;
    }

    public void setIdNitR(String idNitR) {
        this.idNitR = idNitR;
    }

    public String getDtNascimentoR() {
        return dtNascimentoR;
    }

    public void setDtNascimentoR(String dtNascimentoR) {
        this.dtNascimentoR = dtNascimentoR;
    }

    public String getCtpsR() {
        return ctpsR;
    }

    public void setCtpsR(String ctpsR) {
        this.ctpsR = ctpsR;
    }

    public String getCtpsSerieR() {
        return ctpsSerieR;
    }

    public void setCtpsSerieR(String ctpsSerieR) {
        this.ctpsSerieR = ctpsSerieR;
    }

    public String getCtpsUfR() {
        return ctpsUfR;
    }

    public void setCtpsUfR(String ctpsUfR) {
        this.ctpsUfR = ctpsUfR;
    }

    public String getNuIdentidadeR() {
        return nuIdentidadeR;
    }

    public void setNuIdentidadeR(String nuIdentidadeR) {
        this.nuIdentidadeR = nuIdentidadeR;
    }

    public String getIdentidadeUfR() {
        return identidadeUfR;
    }

    public void setIdentidadeUfR(String identidadeUfR) {
        this.identidadeUfR = identidadeUfR;
    }

    public String getCsEmissorR() {
        return csEmissorR;
    }

    public void setCsEmissorR(String csEmissorR) {
        this.csEmissorR = csEmissorR;
    }

    public String getCsTipoR() {
        return csTipoR;
    }

    public void setCsTipoR(String csTipoR) {
        this.csTipoR = csTipoR;
    }

    public Integer getQtDepIr() {
        return qtDepIr;
    }

    public void setQtDepIr(Integer qtDepIr) {
        this.qtDepIr = qtDepIr;
    }

    public Integer getQtDepValNb() {
        return qtDepValNb;
    }

    public void setQtDepValNb(Integer qtDepValNb) {
        this.qtDepValNb = qtDepValNb;
    }

    public Integer getQtDepCadastro() {
        return qtDepCadastro;
    }

    public void setQtDepCadastro(Integer qtDepCadastro) {
        this.qtDepCadastro = qtDepCadastro;
    }

    public Integer getQtRubricaReg() {
        return qtRubricaReg;
    }

    public void setQtRubricaReg(Integer qtRubricaReg) {
        this.qtRubricaReg = qtRubricaReg;
    }

    public Integer getCsRubrica1() {
        return csRubrica1;
    }

    public void setCsRubrica1(Integer csRubrica1) {
        this.csRubrica1 = csRubrica1;
    }

    public Integer getCsRubrica2() {
        return csRubrica2;
    }

    public void setCsRubrica2(Integer csRubrica2) {
        this.csRubrica2 = csRubrica2;
    }

    public Integer getCsRubrica3() {
        return csRubrica3;
    }

    public void setCsRubrica3(Integer csRubrica3) {
        this.csRubrica3 = csRubrica3;
    }

    public Integer getCsRubrica4() {
        return csRubrica4;
    }

    public void setCsRubrica4(Integer csRubrica4) {
        this.csRubrica4 = csRubrica4;
    }

    public Integer getCsRubrica5() {
        return csRubrica5;
    }

    public void setCsRubrica5(Integer csRubrica5) {
        this.csRubrica5 = csRubrica5;
    }

    public Integer getCsRubrica6() {
        return csRubrica6;
    }

    public void setCsRubrica6(Integer csRubrica6) {
        this.csRubrica6 = csRubrica6;
    }

    public Integer getCsRubrica7() {
        return csRubrica7;
    }

    public void setCsRubrica7(Integer csRubrica7) {
        this.csRubrica7 = csRubrica7;
    }

    public Integer getCsRubrica8() {
        return csRubrica8;
    }

    public void setCsRubrica8(Integer csRubrica8) {
        this.csRubrica8 = csRubrica8;
    }

    public Integer getCsRubrica9() {
        return csRubrica9;
    }

    public void setCsRubrica9(Integer csRubrica9) {
        this.csRubrica9 = csRubrica9;
    }

    public Integer getCsRubrica10() {
        return csRubrica10;
    }

    public void setCsRubrica10(Integer csRubrica10) {
        this.csRubrica10 = csRubrica10;
    }

    public BigDecimal getVlRubrica1() {
        return vlRubrica1;
    }

    public void setVlRubrica1(BigDecimal vlRubrica1) {
        this.vlRubrica1 = vlRubrica1;
    }

    public BigDecimal getVlRubrica2() {
        return vlRubrica2;
    }

    public void setVlRubrica2(BigDecimal vlRubrica2) {
        this.vlRubrica2 = vlRubrica2;
    }

    public BigDecimal getVlRubrica3() {
        return vlRubrica3;
    }

    public void setVlRubrica3(BigDecimal vlRubrica3) {
        this.vlRubrica3 = vlRubrica3;
    }

    public BigDecimal getVlRubrica4() {
        return vlRubrica4;
    }

    public void setVlRubrica4(BigDecimal vlRubrica4) {
        this.vlRubrica4 = vlRubrica4;
    }

    public BigDecimal getVlRubrica5() {
        return vlRubrica5;
    }

    public void setVlRubrica5(BigDecimal vlRubrica5) {
        this.vlRubrica5 = vlRubrica5;
    }

    public BigDecimal getVlRubrica6() {
        return vlRubrica6;
    }

    public void setVlRubrica6(BigDecimal vlRubrica6) {
        this.vlRubrica6 = vlRubrica6;
    }

    public BigDecimal getVlRubrica7() {
        return vlRubrica7;
    }

    public void setVlRubrica7(BigDecimal vlRubrica7) {
        this.vlRubrica7 = vlRubrica7;
    }

    public BigDecimal getVlRubrica8() {
        return vlRubrica8;
    }

    public void setVlRubrica8(BigDecimal vlRubrica8) {
        this.vlRubrica8 = vlRubrica8;
    }

    public BigDecimal getVlRubrica9() {
        return vlRubrica9;
    }

    public void setVlRubrica9(BigDecimal vlRubrica9) {
        this.vlRubrica9 = vlRubrica9;
    }

    public BigDecimal getVlRubrica10() {
        return vlRubrica10;
    }

    public void setVlRubrica10(BigDecimal vlRubrica10) {
        this.vlRubrica10 = vlRubrica10;
    }

    public BigDecimal getVlBruto() {
        return vlBruto;
    }

    public void setVlBruto(BigDecimal vlBruto) {
        this.vlBruto = vlBruto;
    }

    public BigDecimal getTotDescontos() {
        return totDescontos;
    }

    public void setTotDescontos(BigDecimal totDescontos) {
        this.totDescontos = totDescontos;
    }

    public BigDecimal getVlLiquido() {
        return vlLiquido;
    }

    public void setVlLiquido(BigDecimal vlLiquido) {
        this.vlLiquido = vlLiquido;
    }

    public String getNuCpf() {
        return nuCpf;
    }

    public void setNuCpf(String nuCpf) {
        this.nuCpf = nuCpf;
    }

    public String getCsSexo() {
        return csSexo;
    }

    public void setCsSexo(String csSexo) {
        this.csSexo = csSexo;
    }

    public String getDtUltimaAlter() {
        return dtUltimaAlter;
    }

    public void setDtUltimaAlter(String dtUltimaAlter) {
        this.dtUltimaAlter = dtUltimaAlter;
    }

    public String getD2Limite() {
        return d2Limite;
    }

    public void setD2Limite(String d2Limite) {
        this.d2Limite = d2Limite;
    }

    public String getDsErro() {
        return dsErro;
    }

    public void setDsErro(String dsErro) {
        this.dsErro = dsErro;
    }

    public String getDtAtualizacaoEtl() {
        return dtAtualizacaoEtl;
    }

    public void setDtAtualizacaoEtl(String dtAtualizacaoEtl) {
        this.dtAtualizacaoEtl = dtAtualizacaoEtl;
    }

    public String getNmArquivo() {
        return nmArquivo;
    }

    public void setNmArquivo(String nmArquivo) {
        this.nmArquivo = nmArquivo;
    }

    public String getAnoMesRef() {
        return anoMesRef;
    }

    public void setAnoMesRef(String anoMesRef) {
        this.anoMesRef = anoMesRef;
    }

    public String getDtUltimaPericia() {
        return dtUltimaPericia;
    }

    public void setDtUltimaPericia(String dtUltimaPericia) {
        this.dtUltimaPericia = dtUltimaPericia;
    }

    public String getFaseUltimaPericia() {
        return faseUltimaPericia;
    }

    public void setFaseUltimaPericia(String faseUltimaPericia) {
        this.faseUltimaPericia = faseUltimaPericia;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (nuNb != null ? nuNb.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof RubricaInss)) {
            return false;
        }
        RubricaInss other = (RubricaInss) object;
        if ((this.nuNb == null && other.nuNb != null) || (this.nuNb != null && !this.nuNb.equals(other.nuNb))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RubricaInss[ nuNb=" + nuNb +", vlLiquido="+vlLiquido+ ", nmRecebedor="+nmRecebedor+"  ]";
    }
    
}
