package br.com.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import java.util.regex.Pattern

class KafkaService<T : Any>{

    private var parse: (record: ConsumerRecord<String, T>) -> Unit
    private var group: String
    private var consumer: KafkaConsumer<String, T>


    constructor(group: String, topic: Pattern?, parse: (record: ConsumerRecord<String, T>) -> Unit, type: Class<T>, mapConfig: Map<String, String?> = mapOf()){
        this.group = group
        this.parse = parse
        consumer = KafkaConsumer(getProperties(type, mapConfig))
        consumer.subscribe(topic)
    }
    constructor(group: String, topic: String, parse: (record: ConsumerRecord<String, T>) -> Unit, type: Class<T>, mapConfig: Map<String, String?> = mapOf()) {
        this.group = group
        this.parse = parse
        consumer = KafkaConsumer(getProperties(type, mapConfig))
        consumer.subscribe(listOf(topic))
    }

    fun run() {
        runCatching {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(100))
                if (!records.isEmpty) {
                    records.forEach { record ->
                        parse(record)
                    }
                }
            }
        }.onFailure {
            consumer.close()
        }
    }

    private fun getProperties(type: Class<T>, overrideProperties: Map<String, String?>): Properties {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "${UUID.randomUUID()}")
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.name)

        properties.putAll(overrideProperties)

        return properties
    }

}