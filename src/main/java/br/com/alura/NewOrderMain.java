package br.com.alura;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var dispatcher = new KafkaDispatcher()) {
            //Teste enviar mensagem 100x - obs: numero de particões maior ou igual numero de consumidores detro de um grupo.
            for (var i = 0; i < 10; i++) {

                //ver como trabalhar em particoes diferentes, precisou criar UUID para não ir o mesmo dados
                var key = UUID.randomUUID().toString();
                //Dados serão enviados para a Fila
                var value = key + " 0349, 30495 ";

                //Enviar os dados para nosso Topic.
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, value);
            }
        }
    }
}