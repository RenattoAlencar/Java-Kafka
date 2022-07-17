package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        //Consumir mensagem do Topico com uma Lista
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        // Buscar as mensaggens no bloco while(true), até encontrar
        while (true) {

            //Verificar se existe mensagem por um periodo dias/tempo
            var records = consumer.poll(Duration.ofMillis(100));

            //validação de registros encontrados e a quantidade.

            if (!records.isEmpty()) {
                System.out.println(" Encontrei "+records.count() + " registros");

                for (var record : records) {
                    System.out.println("-----");
                    System.out.println("Processando new order, checking for fraud");
                    System.out.println("key: "+record.key());
                    System.out.println("Value: "+record.value());
                    System.out.println("partition: "+record.partition());
                    System.out.println("offset: "+record.offset());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Order processed");
                }
            }

        }
    }

    private static Properties properties() {
        var properties = new Properties();

        //dentro do bloco while(truue) para rodar ate encontrar as mensagens

            //Onde está rodando o Kafka - ex:localhost:9092
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

            //Deserializado de byte para String
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            //Corrção InvalidropExceptions
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,FraudDetectorService.class.getSimpleName());


            return properties;
        }
}
