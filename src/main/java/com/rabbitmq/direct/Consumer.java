package com.rabbitmq.direct;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.commons.ExchangeType;
import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.AMQP.BasicProperties;

public class Consumer {
	private static final String QUEUE_NAME = "BOYS";
	private static final String QUEUE_NAME_2 = "GRILS";
	private static ConnectionFactory factory;
	private static Connection connection;
	private static Channel channel;
	static{
		try {
			factory = new ConnectionFactory();
			factory.setHost("localhost");
			factory.setPort(5672);
			factory.setVirtualHost("/");
			factory.setUsername("guest");
			factory.setPassword("guest");
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.exchangeDeclare(ExchangeType.DIRECT.getName(), ExchangeType.DIRECT.getType(),true);
			/*1.队列名称，2.是否可持久化，3.是否排他列队，4.是否自动删除（没有消费者订阅时）*/
			channel.queueDeclare(QUEUE_NAME, true, false, true, null);
			channel.queueBind(QUEUE_NAME, ExchangeType.DIRECT.getName(), "boys");
			channel.queueDeclare(QUEUE_NAME_2, true, false, true, null);
			channel.queueBind(QUEUE_NAME_2, ExchangeType.DIRECT.getName(), "girls");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}
	
	public static void receive() throws ShutdownSignalException, ConsumerCancelledException, InterruptedException, IOException{

		
		com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String msg = new String(body);
				System.out.println("cousumer received message:" + msg);
			}
		};
		com.rabbitmq.client.Consumer consumer2 = new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				String msg = new String(body);
				System.out.println("cousumer2 received message:" + msg);
				//channel.basicAck(envelope.getDeliveryTag(), true);
			}
		};
		/*1.队列名称；2.是否自动发送ack；3.消费者*/
		//channel.basicConsume(QUEUE_NAME, true, consumer);
		//channel.basicConsume(QUEUE_NAME_2, false, consumer2);
		

		GetResponse basicGet = channel.basicGet(QUEUE_NAME_2, false);
		System.out.println(JSON.toJSONString(basicGet,true));
		System.out.println(new String(basicGet.getBody()));
		
		//第二个参数true表示接受成功队列可以删除该条消息
		channel.basicAck(basicGet.getEnvelope().getDeliveryTag(), false);
	}
	
	public static void main(String[] args) throws IOException, TimeoutException {
		try {
			receive();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
