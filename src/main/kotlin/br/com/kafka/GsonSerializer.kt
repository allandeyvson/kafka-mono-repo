package br.com.kafka

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer

class GsonSerializer<T> : Serializer<T> {

    private val gson = GsonBuilder().create()

    override fun serialize(s: String?, obj: T): ByteArray {
        return gson.toJson(obj).toByteArray()
    }
}