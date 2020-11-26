package com.example.transactions;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.PeekableItemReader;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@EnableBatchProcessing
@Configuration
public class BatchConfig {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Value( "${app.chunkSize}" )
	private Integer chunkSize;
	
	@Bean
	public Job job() {
		return jobBuilderFactory.get("transactionsJob")
				.start(stepImportCustomers())
				.next(stepCustomerReport())
				.validator(validator())
				.incrementer(new RunIdIncrementer())
				.build();
	}
	
/** Início parte 5 **/
	public Step stepCustomerReport() {
		return stepBuilderFactory.get("stepCustomerReport").<Customer, Customer>chunk(1).reader(customerMultilinePagingReader())
				.writer(customerMultiresourceWriter()).build();
	}

	public CustomerMultilineReader customerMultilinePagingReader() {
		return new CustomerMultilineReader(customerPagingReader(null, null));
	}

	class CustomerMultilineReader implements PeekableItemReader<Customer>, ItemStream {
		private JdbcPagingItemReader<Customer> delegate;
		private Customer current;

		public CustomerMultilineReader(JdbcPagingItemReader<Customer> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Customer read() throws Exception {
			if (current == null)
				current = delegate.read();

			if (current == null)
				return null;
			else
				current.getAccounts().add(current.getAccount());

			while (true) {
				Customer nextCustomer = peek();
				if (nextCustomer == null) {
					Customer customer = (Customer) current;
					current = null;
					return customer;
				}

				boolean matches = nextCustomer.getName().equals(current.getName());
				if (matches)
					current.getAccounts().add(nextCustomer.getAccount());
				else {
					Customer customer = (Customer) current;
					current = nextCustomer;
					return customer;
				}
			}
		}

		@Override
		public void open(ExecutionContext executionContext) throws ItemStreamException {
			delegate.open(executionContext);
		}

		@Override
		public void update(ExecutionContext executionContext) throws ItemStreamException {
			delegate.update(executionContext);
		}

		@Override
		public void close() throws ItemStreamException {
			delegate.close();
		}

		@Override
		public Customer peek() throws Exception, UnexpectedInputException, ParseException {
			Customer customer = delegate.read();
			return customer;
		}

	}
	
	@Bean
	public JdbcPagingItemReader<Customer> customerPagingReader(@Qualifier("appDatasource") DataSource dataSource, PagingQueryProvider queryProvider) {
		return new JdbcPagingItemReaderBuilder<Customer>().name("customerPagingReader")
				.dataSource(dataSource)
				.queryProvider(queryProvider)
				.pageSize(1)
				.rowMapper(new CustomerRowMapper()).build();
	}
	
	@Bean
	public SqlPagingQueryProviderFactoryBean customerQueryProvider(DataSource dataSource) {
		SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
		factoryBean.setDataSource(dataSource);
		factoryBean.setSelectClause("select *");
		factoryBean.setFromClause("from customer join account on (name = customer)");
		Map<String, Order> keys = new HashMap<>();
		keys.put("name", Order.ASCENDING);
		keys.put("id", Order.ASCENDING);
		factoryBean.setSortKeys(keys);
		return factoryBean;
	}

	class CustomerRowMapper implements RowMapper<Customer> {
		@Override
		public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
			Customer customer = new Customer();
			customer.setName(rs.getString("name"));
			customer.setAccount(new Account());
			customer.getAccount().setId(rs.getString("id"));
			customer.getAccount().setCustomer(rs.getString("customer"));
			return customer;
		}
	}
	
	public MultiResourceItemWriter<Customer> customerMultiresourceWriter() {
		return new MultiResourceItemWriterBuilder<Customer>().name("customerMultiresourceWriter")
				.resource(new FileSystemResource("files/customer"))
				.resourceSuffixCreator((int arg0) -> arg0 + ".txt")
				.itemCountLimitPerResource(1) // Por chunk gera um arquivo
				.delegate(customerFlatFileItemWriter()).build();
	}
	
	public FlatFileItemWriter<Customer> customerFlatFileItemWriter() {
		FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();
		itemWriter.setName("customerFlatFileItemWriter");
		itemWriter.setHeaderCallback(new CustomerHeaderCallback());
		itemWriter.setLineAggregator(new CustomerLineAggregator());
		return itemWriter;
	}
	
	class CustomerHeaderCallback implements FlatFileHeaderCallback {
		@Override
		public void writeHeader(Writer writer) throws IOException {
			writer.write(String.format("%60s\n\n", "Accounts Summary"));
		}
	}
	
	class CustomerLineAggregator implements LineAggregator<Customer> {
		@Override
		public String aggregate(Customer customer) {
			StringBuilder output = new StringBuilder();
			formatHeader(customer, output);
			formatCustomer(customer, output);
			return output.toString();
		}
		
		public void formatHeader(Customer customer, StringBuilder output) {
			output.append(String.format("Name: %s\n", customer.getName()));
		}

		public void formatCustomer(Customer customer, StringBuilder output) {
			for (Account account : customer.getAccounts()) {
				output.append(String.format("Id: %s\n", account.getId()));
			}
		}
	}
/** Fim parte 5 **/

	/** Início parte 4 **/
	@Bean
	public BeanValidatingItemProcessor<Customer> customerCamposValidatorItemProcessor() {
		BeanValidatingItemProcessor<Customer> itemProcessor = new BeanValidatingItemProcessor<>();
		itemProcessor.setFilter(true);
		return itemProcessor;
	}

	@Bean
	public ValidatingItemProcessor customerValidatingItemProcessor(CustomerValidator validator) {
		ValidatingItemProcessor itemProcessor = new ValidatingItemProcessor<>(validator);
		// Para não gerar exceção
		itemProcessor.setFilter(true);
		return itemProcessor;

	}

	@Bean
	public ValidatingItemProcessor<Account> accountValidatingItemProcessor(AccountValidator validator) {
		return new ValidatingItemProcessor<>(validator);
	}

	@Component
	class AccountValidator implements Validator {
		private final NamedParameterJdbcTemplate jdbcTemplate;
		private static final String FIND_CONTA = "SELECT COUNT(*) FROM account WHERE id = :id";

		public AccountValidator(@Qualifier("appDatasource") DataSource dataSource) {
			this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
		}

		@Override
		public void validate(Object obj) throws ValidationException {
			if (obj instanceof Account) {
				Account account = (Account) obj;
				Map<String, String> parameterMap = Collections.singletonMap("id", account.getId());
				Long count = jdbcTemplate.queryForObject(FIND_CONTA, parameterMap, Long.class);
				if (count > 0) {
					throw new ValidationException(String.format("Account id %s já existe!", account.getId()));
				}
			}
		}
	}

	public CompositeItemProcessor itemProcessor() {
		CompositeItemProcessor compositeItemProcessor = new CompositeItemProcessor();
		compositeItemProcessor.setDelegates(Arrays.asList(customerCamposValidatorItemProcessor(),
				accountValidatingItemProcessor(null), customerValidatingItemProcessor(null)));
		return compositeItemProcessor;
	}

	/** Fim parte 4 **/

	/** Início parte 3 **/
	@SuppressWarnings("unchecked")
	public Step stepImportCustomers() {
		return stepBuilderFactory.get("stepImportCustomers").chunk(chunkSize).reader(customerMultilineReader())
				.processor(itemProcessor()).writer(customerAccountWriter()).build();
	}

	public CompositeItemWriter customerAccountWriter() {
		return new CompositeItemWriterBuilder()
				.delegates(Arrays.asList(customerWriter(null), new AccountWriter(accountWriter(null)))).build();
	}

	@Bean
	@StepScope
	public JdbcBatchItemWriter<Customer> customerWriter(@Qualifier("appDatasource") DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Customer>().dataSource(dataSource).sql(
				"INSERT INTO customer (name, age, state, city, address, cell_phone, email, work_phone) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
				.itemPreparedStatementSetter(new CustomerPreparedStatementSetter()).build();
	}

	class CustomerPreparedStatementSetter implements ItemPreparedStatementSetter<Customer> {
		@Override
		public void setValues(Customer customer, PreparedStatement ps) throws SQLException {
			ps.setString(1, customer.getName());
			ps.setInt(2, customer.getAge());
			ps.setString(3, customer.getState());
			ps.setString(4, customer.getCity());
			ps.setString(5, customer.getAddress());
			ps.setString(6, customer.getCellPhone());
			ps.setString(7, customer.getEmail());
			ps.setString(8, customer.getWorkPhone());
		}
	}

	@Bean
	@StepScope
	public JdbcBatchItemWriter<Account> accountWriter(@Qualifier("appDatasource") DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Account>().dataSource(dataSource)
				.sql("INSERT INTO account (id, customer) VALUES (?, ?)")
				.itemPreparedStatementSetter(new AccountPreparedStatementSetter()).build();
	}

	class AccountPreparedStatementSetter implements ItemPreparedStatementSetter<Account> {
		@Override
		public void setValues(Account account, PreparedStatement ps) throws SQLException {
			ps.setString(1, account.getId());
			ps.setString(2, account.getCustomer());
		}
	}

	class AccountWriter implements ItemWriter<Customer> {
		private ItemWriter<Account> delegate;

		public AccountWriter(ItemWriter<Account> delegate) {
			this.delegate = delegate;
		}

		@Override
		public void write(List<? extends Customer> customers) throws Exception {
			final List<Account> accounts = new ArrayList<Account>();
			for (Customer customer : customers) {
				for (Account account : customer.getAccounts()) {
					account.setCustomer(customer.getName());
					accounts.add(account);
				}
			}
			delegate.write(accounts);
		}
	}

	/** Fim parte 3 **/

	/** Início parte 2 **/
	public CustomerReader customerMultilineReader() {
		return new CustomerReader(customerReader(null));
	}

	// Reader multiline
	class CustomerReader implements ItemStreamReader<Customer> {
		private Object curItem = null;
		private ItemStreamReader<Object> delegate;

		public CustomerReader(ItemStreamReader<Object> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Customer read()
				throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
			if (curItem == null)
				curItem = delegate.read();

			Customer customer = (Customer) curItem;
			curItem = null;

			if (customer != null) {
				while (peek() instanceof Account) {
					customer.getAccounts().add((Account) curItem);
					curItem = null;
				}
			}
			return customer;
		}

		private Object peek() throws Exception {
			if (curItem == null)
				curItem = delegate.read();
			return curItem;
		}

		@Override
		public void open(ExecutionContext executionContext) throws ItemStreamException {
			delegate.open(executionContext);
		}

		@Override
		public void update(ExecutionContext executionContext) throws ItemStreamException {
			delegate.update(executionContext);
		}

		@Override
		public void close() throws ItemStreamException {
			delegate.close();
		}
	}

	// Reader customizado de diferentes tipos de objeto
	@SuppressWarnings("unchecked")
	@Bean
	@StepScope
	public FlatFileItemReader<Object> customerReader(
			@Value("#{jobParameters['customersFile']}") Resource customerFile) {
		return new FlatFileItemReaderBuilder<Customer>().name("customerReader").lineMapper(lineTokenizer())
				.resource(customerFile).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public PatternMatchingCompositeLineMapper lineTokenizer() {
		// Line tokenizers
		Map<String, LineTokenizer> lineTokenizers = new HashMap<>(2);
		lineTokenizers.put("0*", customerTokenizer());
		lineTokenizers.put("1*", accountTokenizer());

		// FieldSetMappers
		Map<String, FieldSetMapper> fieldSetMappers = new HashMap<>(2);
		BeanWrapperFieldSetMapper<Customer> customerFieldSetMapper = new BeanWrapperFieldSetMapper<>();
		customerFieldSetMapper.setTargetType(Customer.class);
		BeanWrapperFieldSetMapper<Account> accountFieldSetMapper = new BeanWrapperFieldSetMapper<>();
		accountFieldSetMapper.setTargetType(Account.class);
		fieldSetMappers.put("0*", customerFieldSetMapper);
		fieldSetMappers.put("1*", accountFieldSetMapper);

		// Pattern matching
		PatternMatchingCompositeLineMapper lineMappers = new PatternMatchingCompositeLineMapper<>();
		lineMappers.setTokenizers(lineTokenizers);
		lineMappers.setFieldSetMappers(fieldSetMappers);
		return lineMappers;
	}

	public DelimitedLineTokenizer customerTokenizer() {
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames("name", "age", "city", "state", "address", "cellPhone", "email", "workPhone");
		lineTokenizer.setIncludedFields(1, 2, 3, 4, 5, 6, 7, 8);
		return lineTokenizer;
	}

	public DelimitedLineTokenizer accountTokenizer() {
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames("id");
		lineTokenizer.setIncludedFields(1);
		return lineTokenizer;
	}

	/** Fim parte 2 **/

	@Bean
	@Profile("default")
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		PropertySourcesPlaceholderConfigurer properties = new PropertySourcesPlaceholderConfigurer();
		properties.setLocation(new FileSystemResource("/etc/config/treinamento_springbatch/application.properties"));
		properties.setIgnoreResourceNotFound(false);
		return properties;
	}

	// Bonus de validação
	public DefaultJobParametersValidator defaultJobParametersValidator() {
		DefaultJobParametersValidator validator = new DefaultJobParametersValidator();
		validator.setRequiredKeys(new String[] { "customersFile" });
		return validator;
	}

	class ParameterValidator implements JobParametersValidator {
		@Override
		public void validate(JobParameters parameters) throws JobParametersInvalidException {
			String fileName = parameters.getString("customersFile");
			if (!StringUtils.hasText(fileName)) {
				throw new JobParametersInvalidException("customersFile parameter is missing");
			} else if (!StringUtils.endsWithIgnoreCase(fileName, "csv")) {
				throw new JobParametersInvalidException("customersFile parameter does not use the csv file extension");
			} else if (!new File(fileName.substring(5)).exists())
				throw new JobParametersInvalidException(fileName.substring(5) + " does not exist");
		}
	}

	public CompositeJobParametersValidator validator() {
		CompositeJobParametersValidator validator = new CompositeJobParametersValidator();
		DefaultJobParametersValidator defaultJobParametersValidator = defaultJobParametersValidator();
		defaultJobParametersValidator.afterPropertiesSet();
		validator.setValidators(Arrays.asList(new ParameterValidator(), defaultJobParametersValidator));
		return validator;
	}
	
// Sem customizar duração da coleta de métricas
//	@Bean
//	LoggingMeterRegistry loggingMeterRegistry() {
//		return new LoggingMeterRegistry();
//	}
	
//	@Bean
//	LoggingMeterRegistry loggingMeterRegistry() {
//	    return new LoggingMeterRegistry(new LoggingRegistryConfig() {
//	        @Override
//	        public Duration step() {
//	            return Duration.ofSeconds(2); // loga a cada 2 segundos
//	        }
//
//	        @Override
//	        public String get(String key) {
//	            return null;
//	        }
//	    }, Clock.SYSTEM);
//	}
}
