package com.project.example.JPProject;

public class IncomingMessage {

	private String messageType;

	private String operationType;

	private String productType;

	private int noOfSale = 0;

	private int price = 0;
	
	private int counter;

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	public String getOperationType() {
		return operationType;
	}

	public void setOperationType(String operationType) {
		this.operationType = operationType;
	}

	public String getMessageType() {
		return messageType;
	}

	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}
	
	public IncomingMessage() {
		super();
		
	}

	public IncomingMessage(String messageType, String operationType, String productType, int noOfSale, int price) {
		super();
		this.messageType = messageType;
		this.operationType = operationType;
		this.productType = productType;
		this.noOfSale = noOfSale;
		this.price = price;
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
