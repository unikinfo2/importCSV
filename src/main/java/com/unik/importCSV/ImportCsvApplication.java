package com.unik.importCSV;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
@Configuration
public class ImportCsvApplication {

	@Value("${nomebancodados}")
    private String nomebancodados;    
    

	public static void main(String[] args) {
		SpringApplication.run(ImportCsvApplication.class, args);
	}
	
    @Bean
    public DataSource dataSource() {
    	System.out.println("Abrindo banco de dados: "+nomebancodados);
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		if(nomebancodados.equals("postgresql")) {
			dataSource.setDriverClassName("org.postgresql.Driver");
			dataSource.setUrl("jdbc:postgresql://localhost:5432/avantdb");
			dataSource.setUsername("postgres");
			dataSource.setPassword("postgres");
		} else {
			dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
			dataSource.setUrl("jdbc:mysql://localhost:3306/unikinfo_dbneosunik");
			dataSource.setUsername("unik");
			dataSource.setPassword("unik3001");
		}
		
		//ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(); 
		//databasePopulator.addScript(new ClassPathResource("org/springframework/batch/core/schema-drop-mysql.sql"));
		//databasePopulator.addScript(new ClassPathResource("org/springframework/batch/core/schema-mysql.sql"));
		//DatabasePopulatorUtils.execute(databasePopulator, dataSource);
    	
        return dataSource; 
    }


}
