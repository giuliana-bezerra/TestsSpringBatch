package com.example.transactions.steps;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.jdbc.AutoConfigureDataJdbc;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.example.transactions.Account;
import com.example.transactions.BatchConfig;
import com.example.transactions.Customer;
import com.example.transactions.CustomerValidator;
import com.example.transactions.DatasourceConfig;

// This enables all the Spring goodness in JUnit 5.
@ExtendWith(SpringExtension.class)
// Coloca as classes que criam os beans necessários para os testes no contexto de execução do Spring.
@ContextConfiguration(classes = {BatchConfig.class, DatasourceConfig.class, CustomerValidator.class})
// Informa o arquivo de propriedades para o teste
@PropertySource("classpath:application.properties")
// Configura automaticamente os bancos descritos no application properties
@AutoConfigureDataJdbc
// Utiliza o profile de teste para não externalizar as propriedades
@ActiveProfiles("test")
// Provides utilities for testing Spring Batch jobs. The specific one
// we care about in this example is the JobLauncherTestUtils.
@SpringBatchTest
public class CustomerFlatFileItemReaderIT {
	@Autowired
	private FlatFileItemReader<Object> flatFileItemReader;
	
	public StepExecution getStepExecution() {
		JobParameters jobParameters = new JobParametersBuilder()
				.addString("customersFile", "classpath:customersFile.csv").toJobParameters();
		return MetaDataInstanceFactory.createStepExecution(jobParameters);
	}
	
	@Test
	public void testTypeConversion() throws Exception {
	        this.flatFileItemReader.open(new ExecutionContext());
	        assertTrue(this.flatFileItemReader.read() instanceof Customer);
	        assertTrue(this.flatFileItemReader.read() instanceof Account);
	}
}
