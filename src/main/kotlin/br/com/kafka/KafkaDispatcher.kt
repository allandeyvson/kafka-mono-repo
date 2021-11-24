package br.com.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.Exception
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*

class KafkaDispatcher<T> {

    private var producer = KafkaProducer<String, T>(properties())

    fun send(topic: String, key: String, value: T) {
        val callBack: (RecordMetadata, Exception?) -> Unit = { data, ex ->
            ex?.let {
                ex.printStackTrace()
                return@let
            }
            println(
                "${data.topic()} partition::${data.partition()}  offset::${data.offset()} ${
                    data.timestamp().toLocalDataTime()
                }"
            )
        }

        runCatching {
            val record = ProducerRecord(topic, key, value)
            producer.send(record, callBack).get()
        }.onFailure {
            producer.close()
        }

    }


    private fun properties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.qualifiedName)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.qualifiedName)
        return properties
    }

    private fun Long.toLocalDataTime() = LocalDateTime.ofInstant(Instant.ofEpochMilli(this), ZoneId.systemDefault())
}