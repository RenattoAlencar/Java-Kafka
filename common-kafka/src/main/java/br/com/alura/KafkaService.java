package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);

        //Consumir mensagem do Topico com uma Lista
        consumer.subscribe(Collections.singletonList(topic));

    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);
        consumer.subscribe(topic);
    }

    public KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));

    }

    public void run() {
        // Buscar as mensaggens no bloco while(true)
        while (true) {

            //Verificar se existe mensagem por um periodo dias/tempo
            var records = consumer.poll(Duration.ofMillis(100));

            //validação de registros encontrados e a quantidade.
            if (!records.isEmpty()) {
                System.out.println(" Encontrei " + records.count() + " registros");

                for (var record : records) {
                    parse.consume(record);

                }

            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overriderProperties) {
        var properties = new Properties();

        //Onde está rodando o Kafka - ex:localhost:9092
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //Deserializado de byte para String
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());

        //Corrção InvalidropExceptions
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //Nome para o consumidor grupo - Configurar no docker
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        //Consumir mensagens 1 em 1 - Configurar no docker
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        //Deserializer
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        properties.putAll(overriderProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
