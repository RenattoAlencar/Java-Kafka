package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        //Consumir qualquer topico qe comece com ECOMMERCE.
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        // Buscar as mensaggens no bloco while(true), até encontrar
        while (true) {

            //Verificar se existe mensagem por um periodo dias/tempo
            var records = consumer.poll(Duration.ofMillis(100));

            //validação de registros encontrados e a quantidade.

            if (!records.isEmpty()) {
                System.out.println(" Encontrei " + records.count() + " registros");

                for (var record : records) {
                    System.out.println("-----");
                    System.out.println("LOG" + record.topic());
                    System.out.println("key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("partition: " + record.partition());
                    System.out.println("offset: " + record.offset());

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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());


        return properties;
    }
}
