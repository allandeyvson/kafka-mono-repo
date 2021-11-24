package br.com.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.regex.Pattern

fun main(args: Array<String>) {
    val logService = LogService()
    val service = KafkaService(
        logService::class.simpleName!!,
        Pattern.compile("ECOMMERCE.*"),
        logService::parse,
        String::class.java,
        mapOf(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.qualifiedName)
    )
    service.run()
}

class LogService {
    internal fun parse(record: ConsumerRecord<String, String>) {
        println("${record.topic()} ${record.key()} ${record.value()} ${record.partition()} ${record.offset()}")
        println("--------------------------------------------------------------------------------------------")
    }
}