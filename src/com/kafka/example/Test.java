package com.kafka.example;

import java.util.ArrayList;

public class Test {
	public static void main(String args) {
		System.out.println("test");
	}
	public static void main(int args) {
		System.out.println("int values");
	}
	public static void main(String[] args) {
		try {

			ArrayList<String> list=DataDump.getData();
       	 for(int i=0;i<list.size();i++) {
       		System.out.println(list.get(i));
       		       	 }
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
