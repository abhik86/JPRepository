package com.project.example.JPProject;

public class ProcessedMessage {

	
	private String productType;

	private int noOfSale;

	private int price;
	
	private int total;

	private int counter;
	
	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	public ProcessedMessage(String productType, int noOfSale, int price, int total) {
		super();
		this.productType = productType;
		this.noOfSale = noOfSale;
		this.price = price;
		this.total = total;
	}
	

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public String getProductType() {
		return productType;
	}

	public void setProductType(String productType) {
		this.productType = productType;
	}

	public int getNoOfSale() {
		return noOfSale;
	}

	public void setNoOfSale(int noOfSale) {
		this.noOfSale = noOfSale;
	}

	public int getPrice() {
		return price;
	}

	public void setPrice(int price) {
		this.price = price;
	}

}
