package org.kafkaspark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer extends Thread {

	private Properties prop = new Properties();
	private KafkaProducer<String, String> producer;
	private ProducerRecord<String, String> record;
	
	public MyProducer(){
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(prop);
	}
	
	public void run(){
		int msgNo = 1;
		System.out.println("Started");
		while (true){
			
			String msg = "message_" + msgNo;
			record = new ProducerRecord<String, String>("HelloWorld", "My Message", msg);
			producer.send(record);
			System.out.println(msg);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			msgNo++;
		}
	}
	
	public static void main(String[] args){
		System.out.println("I'm here");
		MyProducer myProducer = new MyProducer();
		myProducer.start();
	}
	
}
