package br.com.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

fun main(args: Array<String>) {
    val fraudDetectorService = FraudDetectorService()
    val service = KafkaService(fraudDetectorService::class.simpleName!!, "ECOMMERCER_NEW_ORDER", fraudDetectorService::parse, Order::class.java)
    service.run()
}

class FraudDetectorService {

    internal fun parse(record: ConsumerRecord<String, Order>) {
        println("${record.key()} ${record.value()} ${record.partition()} ${record.offset()}")
        Thread.sleep(5000)
        println("Order ${record.offset()} processed")
    }
}