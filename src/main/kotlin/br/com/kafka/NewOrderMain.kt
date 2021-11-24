package br.com.kafka

import java.math.BigDecimal
import java.util.*

fun main(args: Array<String>){

    val dispatcherOrder = KafkaDispatcher<Order>()
    val order = Order(UUID.randomUUID().toString(), UUID.randomUUID().toString(), BigDecimal(Math.random() * 500 + 1))
    val dispatcherEmail = KafkaDispatcher<Email>()
    val email = Email("Processing order ${order.orderId}", "We are processing your order!")

    dispatcherOrder.send("ECOMMERCER_NEW_ORDER", UUID.randomUUID().toString(), order)
    dispatcherEmail.send("ECOMMERCER_SEND_EMAIL", UUID.randomUUID().toString(), email)
}