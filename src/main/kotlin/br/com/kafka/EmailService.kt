package br.com.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

fun main(args: Array<String>) {
    val emailService = EmailService()
    val service = KafkaService(emailService::class.simpleName!!,"ECOMMERCER_SEND_EMAIL", emailService::parse, Email::class.java)
    service.run()
}

class EmailService {

    internal fun parse(record: ConsumerRecord<String, Email>){
        println("${record.key()} ${record.value()} ${record.partition()} ${record.offset()}")
        Thread.sleep(2000)
        println("Mail send! We are processing your order ${record.offset()}! ")
    }
}