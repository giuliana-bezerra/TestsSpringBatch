package com.example.transactions.steps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.jdbc.AutoConfigureDataJdbc;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.example.transactions.BatchConfig;
import com.example.transactions.CustomerValidator;
import com.example.transactions.DatasourceConfig;

//This enables all the Spring goodness in JUnit 5.
@ExtendWith(SpringExtension.class)
//Coloca as classes que criam os beans necessários para os testes no contexto de execução do Spring.
@ContextConfiguration(classes = { BatchConfig.class, DatasourceConfig.class, CustomerValidator.class })
//Informa o arquivo de propriedades para o teste
@PropertySource("classpath:application.properties")
//Configura automaticamente os bancos descritos no application properties
@AutoConfigureDataJdbc
//Utiliza o profile de teste para não externalizar as propriedades
@ActiveProfiles("test")
//Provides utilities for testing Spring Batch jobs. The specific one
//we care about in this example is the JobLauncherTestUtils.
@SpringBatchTest
public class TransactionsIT {
	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	/**
	 * Testar o job inteiro é interessante para verificar se o resultado final foi o
	 * esperado, por exemplo, conferir metadados do spring batch para saber quantos
	 * itens foram lidos, escritos, ...
	 */
	@Test
	public void test() throws Exception {
		JobParameters jobParameters = new JobParametersBuilder()
				.addString("customersFile", "file:src/test/resources/customersFile.csv").toJobParameters();
		JobExecution jobExecution = this.jobLauncherTestUtils.launchJob(jobParameters);
		assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
		StepExecution stepExecution = jobExecution.getStepExecutions().iterator().next();
		assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus());
		assertEquals(4, stepExecution.getReadCount());
		assertEquals(4, stepExecution.getWriteCount());
	}
}
