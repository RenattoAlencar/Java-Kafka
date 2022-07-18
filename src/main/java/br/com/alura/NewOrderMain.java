package br.com.alura;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {


                //Teste enviar mensagem 10x - obs: numero de particões maior ou igual numero de consumidores detro de um grupo.
                for (var i = 0; i < 10; i++) {

                    //Dados serão enviados para a Fila
                    //ver como trabalhar em particoes diferentes, precisou criar UUID para não ir o mesmo dados
                    //Criando uma Order
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(userId, orderId, amount);

                    //Enviar dados para os Topicos abaixo;
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    var email = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}
