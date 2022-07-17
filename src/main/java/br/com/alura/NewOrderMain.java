package br.com.alura;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    private static Callback callback;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //Enviar/Produzir uma mensagem no Kafka
        var producer = new KafkaProducer<String, String>(properties());


        //Teste enviar mensagem 100x - obs: numero de particões maior ou igual numero de consumidores detro de um grupo.
        for (var i = 0; i < 100; i++) {

        //ver como trabalhar em particoes diferentes, precisou criar UUID para não ir o mesmo dados
        var key = UUID.randomUUID().toString();
        //Dados serão enviados para a Fila
        var value = key + " 0349, 30495 ";

        //Enviar os dados para nosso Topic.
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

        //Retornar mensaegm ou CallBack
        callback = (data, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }
            System.out.println("Sucesso Enviando nesse topico: " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        producer.send(record, callback).get();
            var email = "Thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
            producer.send(emailRecord, callback).get();
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        //Onde está rodando o Kafka - ex:localhost:9092
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //Configurar tanto a Chave quanto o Valor será transformados/serializados  de String's para bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}