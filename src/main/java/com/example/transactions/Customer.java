package com.example.transactions;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

public class Customer {
	@NotNull
	private String name;
	private Integer age;
	private String state;
	private String city;
	private String address;
	private String cellPhone;
	private String email;
	private String workPhone;
	private Account account;
	private List<Account> accounts = new ArrayList<>();

	@Override
	public String toString() {
		String customer = "Customer{name='" + name + ", age='" + age + ", state=" + state + ", city=" + city
				+ ", address=" + address + ", cellPhone=" + cellPhone + ", email=" + email + ", workPhone=" + workPhone
				+ "'";
		String accounts = ", accounts=[";
		for (Account account : this.accounts)
			accounts += account + ", ";
		accounts = accounts.substring(0, accounts.length() - 2) + "]}";
		return customer + accounts;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCellPhone() {
		return cellPhone;
	}

	public void setCellPhone(String cellPhone) {
		this.cellPhone = cellPhone;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getWorkPhone() {
		return workPhone;
	}

	public void setWorkPhone(String workPhone) {
		this.workPhone = workPhone;
	}

	public Account getAccount() {
		return account;
	}

	public void setAccount(Account account) {
		this.account = account;
	}

	public List<Account> getAccounts() {
		return accounts;
	}

	public void setAccounts(List<Account> accounts) {
		this.accounts = accounts;
	}

}
