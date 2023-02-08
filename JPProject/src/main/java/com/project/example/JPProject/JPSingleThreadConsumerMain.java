package com.project.example.JPProject;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;

public class JPSingleThreadConsumerMain {
	
	public static void main(String a[]) throws InterruptedException, StreamReadException, DatabindException, FileNotFoundException, IOException {
		
		JPSingleThreadConsumer t = new JPSingleThreadConsumer();
		t.init();
		t.consume();
		 
	}

}
