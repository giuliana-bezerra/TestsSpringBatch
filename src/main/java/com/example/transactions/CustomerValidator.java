package com.example.transactions;

import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class CustomerValidator implements Validator {
	private final NamedParameterJdbcTemplate jdbcTemplate;
	private static final String FIND_USUARIO = "SELECT COUNT(*) FROM customer WHERE name = :name";

	public CustomerValidator(@Qualifier("appDatasource") DataSource dataSource) {
		this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}

	@Override
	public void validate(Object obj) throws ValidationException {
		if (obj instanceof Customer) {
			Customer customer = (Customer) obj;
			Map<String, String> parameterMap = Collections.singletonMap("name", customer.getName());
			Long count = jdbcTemplate.queryForObject(FIND_USUARIO, parameterMap, Long.class);
			if (count > 0) {
				throw new ValidationException(String.format("Customer %s já existe!", customer.getName()));
			}
		}
	}
}
