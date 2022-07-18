package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                fraudService::parse)) {
            service.run();

        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-----");
        System.out.println("Processando new order, checking for fraud");
        System.out.println("key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");

    }

    private static Properties properties() {
        var properties = new Properties();

        return properties;
    }
}
