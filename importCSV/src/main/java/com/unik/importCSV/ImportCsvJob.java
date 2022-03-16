package com.unik.importCSV;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.unik.importCSV.model.RubricaInss;
import com.unik.importCSV.repository.RubricaInssRepository;

@Component
public class ImportCsvJob {
	
	private Logger log = LoggerFactory.getLogger(ImportCsvJob.class);
	
	@Autowired
	private RubricaInssRepository rp;

	@Scheduled(fixedDelay = 1000)
	public void execute() {
		System.out.println("Startou em: "+LocalTime.now());
	    File arquivos[];
	    File diretorio = new File("/home/unik/Desenvolvimento/temp/arquivoslote");
	    File diretorioDestino = new File("/home/unik/Desenvolvimento/temp/arquivoslote/processados");
	    FileFilter filter = new FileFilter() {
	        public boolean accept(File file) {
	            return file.getName().endsWith(".csv");
	        }
	    };        
	    arquivos = diretorio.listFiles(filter);
	    if(arquivos == null) {
	    	return;
	    }
	    if(!diretorioDestino.exists()){
	        //cria a pasta
	    	diretorioDestino.mkdir();
	    }        
	    for(int i=0;arquivos.length > i; i++){
	       log.info("Arquivo sendo Processado: "+arquivos[i]);
	       
	       //Chamada da Importação
	       try {
	    	   lerArquivo(arquivos[i]);
		       boolean ok = arquivos[i].renameTo(new File(diretorioDestino.getPath()+"/"+ arquivos[i].getName()));
    	       if(ok){
    	    	   log.info("Arquivo foi movido com sucesso");
    	       } else {
    	    	   log.warn("Nao foi possivel mover o arquivo"); 
    	       }           
	       } catch(Exception e) {
	    	   
	    	   log.error(e.toString());
	       }
	    		   
		}        
	}

	private void lerArquivo(File arquivo) throws IOException, CsvValidationException {
	    log.info("Começando o processamento do arquivo "+arquivo);
	
	    //CSVReader csvReader = new CSVReader(new FileReader(arquivo));
	    //String[] headers = csvReader.readNext();
	    CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(arquivo), "ISO-8859-1"));
	    String[] headers =  csvReader.readNext();
    	headers[0] = "nu_nb";
    	for(int i=0;i < headers.length;i++) {
    		headers[i] = headers[i].toLowerCase();
    	}
    	
	    String[] colunas = null;
	    
        while ((colunas = csvReader.readNext()) != null) {
        	Map<String, String> valores = new HashMap<String, String>();
        	for(int i=0;i < headers.length;i++) {
        		valores.put( headers[i], colunas[i]);
        	}
        	try {
	        	RubricaInss rbri = new RubricaInss(
	        			valores.get("nu_nb"),
	        			valores.get("id_ol_concessao"), 
	        			valores.get("id_ol_manutencao"), 
	        			valores.get("id_ol_manutant"), 
	        			valores.get("cs_pa"),
	        			new BigDecimal(valores.get("vl_mr_atu")), 
	        			new BigDecimal(valores.get("vl_rmi")), 
	        			valores.get("cs_tratamento"), 
	        			valores.get("cs_especie"), 
	        			valores.get("cs_ramo_atividade"),
	        			valores.get("cs_forma_filiacao"), 
	        			valores.get("cs_doc_empregador"), 
	        			valores.get("nu_doc_empregador"), 
	        			valores.get("nu_nb_ant"), 
	        			valores.get("d2_der"),
	        			valores.get("d2_dib"), 
	        			valores.get("d2_ddb"), 
	        			valores.get("d2_dcb"), 
	        			valores.get("d2_dip"), 
	        			valores.get("d2_ini_incapac"), 
	        			valores.get("d2_inicio_doenca"),
	        			valores.get("d2_drd"), 
	        			valores.get("d2_obito_reclusao"), 
	        			valores.get("cs_cliente_la"), 
	        			valores.get("nu_matr_concessor"), 
	        			valores.get("nu_matr_habilitador"),
	        			valores.get("cs_situacao_benef"), 
	        			valores.get("id_banco"), 
	        			valores.get("id_orgao_pagador"), 
	        			valores.get("cs_meio_pagto"), 
	        			valores.get("nu_agencia_pag"),
	        			valores.get("nu_conta_corrente"), 
	        			valores.get("cs_diagnostico_n"), 
	        			valores.get("cs_diagnostico_1"), 
	        			valores.get("nu_matr_mp1"), 
	        			valores.get("nu_matr_mp2"),
	        			valores.get("d2_format_conc"), 
	        			valores.get("cs_despacho"), 
	        			valores.get("dt_dia_util_pagto"), 
	        			valores.get("nm_recebedor"), 
	        			valores.get("dn_recebedor"),
	        			valores.get("nu_cpf_r"), 
	        			valores.get("cs_sexo_r"), 
	        			valores.get("nm_titular_benef_t"), 
	        			valores.get("nm_mae_t"), 
	        			valores.get("nu_cpf_t"), 
	        			valores.get("id_nit_t"),
	        			valores.get("dt_nascimento_t"), 
	        			valores.get("ctps_t"), 
	        			valores.get("ctps_serie_t"), 
	        			valores.get("ctps_uf_t"), 
	        			valores.get("nu_identidade_t"),
	        			valores.get("id_entidade_uf_t"), 
	        			valores.get("cs_emissor_t"), 
	        			valores.get("nu_tit_eleitor"), 
	        			valores.get("cs_val_cnis"), 
	        			valores.get("cs_sexo_t"),
	        			valores.get("te_endereco_t"), 
	        			valores.get("nm_bairro_t"), 
	        			valores.get("nu_cep_t"), 
	        			valores.get("nu_ddd_t"), 
	        			valores.get("nu_telefone_t"), 
	        			valores.get("id_mun_sinpas_t"),
	        			valores.get("id_mun_ibge_t"), 
	        			valores.get("nm_municipio_t"), 
	        			valores.get("nm_uf_municipio_t"), 
	        			valores.get("d2_obito_t"), 
	        			valores.get("nm_instituidor_i"),
	        			valores.get("nm_mae_i"), 
	        			valores.get("nu_cpf_i"), 
	        			valores.get("id_nit_i"), 
	        			valores.get("dt_nascimento_i"), 
	        			valores.get("ctps_i"), 
	        			valores.get("ctps_serie_i"),
	        			valores.get("ctps_uf_i"), 
	        			valores.get("nu_identidade_i"), 
	        			valores.get("id_entidade_uf_i"), 
	        			valores.get("cs_emissor_i"), 
	        			valores.get("nu_tit_eleitor_i"),
	        			valores.get("cs_val_cnis_i"), 
	        			valores.get("cs_sexo_i"), 
	        			valores.get("d2_obito_i"), 
	        			valores.get("nm_procurador_p"), 
	        			valores.get("nm_mae_p"), 
	        			valores.get("nu_cpf_p"),
	        			valores.get("id_nit_p"), 
	        			valores.get("dt_nascimento_p"), 
	        			valores.get("ctps_p"), 
	        			valores.get("ctps_serie_p"), 
	        			valores.get("ctps_uf_p"), 
	        			valores.get("nu_identidade_p"),
	        			valores.get("id_entidade_uf_p"), 
	        			valores.get("cs_emissor_p"), 
	        			valores.get("cs_sexo_p"), 
	        			valores.get("nm_bairro_p"), 
	        			valores.get("nucepp"),
	        			valores.get("te_endereco_p"), 
	        			valores.get("nm_municipio_p"), 
	        			valores.get("nm_uf_municipio_p"), 
	        			valores.get("municip_nasc_p"),
	        			valores.get("nm_representante_r"), 
	        			valores.get("nm_mae_r"), 
	        			valores.get("id_nit_r"), 
	        			valores.get("dt_nascimento_r"), 
	        			valores.get("ctps_r"),
	        			valores.get("ctps_serie_r"), 
	        			valores.get("ctps_uf_r"), 
	        			valores.get("nu_identidade_r"), 
	        			valores.get("id_entidade_uf_r"), 
	        			valores.get("cs_emissor_r"),
	        			valores.get("cs_tipo_r"), 
	        			Integer.parseInt( valores.get("qt_dep_ir")), 
	        			Integer.parseInt( valores.get("qt_dep_val_nb")), 
	        			Integer.parseInt( valores.get("qt_dep_cadastro")), 
	        			Integer.parseInt( valores.get("qt_rubrica_reg")),
	        			Integer.parseInt( valores.get("cs_rubrica_1")), 
	        			Integer.parseInt( valores.get("cs_rubrica_2")), 
	        			Integer.parseInt( valores.get("cs_rubrica_3")), 
	        			Integer.parseInt( valores.get("cs_rubrica_4")), 
	        			Integer.parseInt( valores.get("cs_rubrica_5")),
	        			Integer.parseInt( valores.get("cs_rubrica_6")), 
	        			Integer.parseInt( valores.get("cs_rubrica_7")), 
	        			Integer.parseInt( valores.get("cs_rubrica_8")), 
	        			Integer.parseInt( valores.get("cs_rubrica_9")), 
	        			Integer.parseInt( valores.get("cs_rubrica_10")),
	        			new BigDecimal(valores.get("vl_rubrica_1")), 
	        			new BigDecimal(valores.get("vl_rubrica_2")), 
	        			new BigDecimal(valores.get("vl_rubrica_3")), 
	        			new BigDecimal(valores.get("vl_rubrica_4")),
	        			new BigDecimal(valores.get("vl_rubrica_5")), 
	        			new BigDecimal(valores.get("vl_rubrica_6")), 
	        			new BigDecimal(valores.get("vl_rubrica_7")), 
	        			new BigDecimal(valores.get("vl_rubrica_8")),
	        			new BigDecimal(valores.get("vl_rubrica_9")), 
	        			new BigDecimal(valores.get("vl_rubrica_10")), 
	        			new BigDecimal(valores.get("vl_bruto")), 
	        			new BigDecimal(valores.get("tot_descontos")),
	        			new BigDecimal(valores.get("vl_liquido")), 
	        			valores.get("nu_cpf"), 
	        			valores.get("cs_sexo"), 
	        			valores.get("dt_ultima_alter"), 
	        			valores.get("d2_limite"), 
	        			valores.get("ds_erro"),
	        			valores.get("dt_atualizacao_etl"), 
	        			valores.get("nm_arquivo"), 
	        			valores.get("ano_mes_ref"), 
	        			valores.get("dt_ultima_pericia"),
	        			valores.get("fase_ultima_pericia")        			
	        			);
	        	rp.save(rbri);
	    	    //log.info(rbri.toString());
        	} catch (Exception e) {
        		log.error(e.toString());
        	}
    	    valores.clear();
        	
        }
        csvReader.close();
	    //itemReader.setResource(new ClassPathResource("file:"+diretorioDestino+"/"+arquivos[0].getName())); ///tmp/arquivoslote/Atualização RUBRICAS INSS 202112 (2).csv"
	}
	

}
