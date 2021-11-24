package br.com.kafka

import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class GsonDeserializer<T> : Deserializer<T> {

    private lateinit var type: Class<T>

    companion object {
        const val TYPE_CONFIG = "br.com.kafka.type.config"
    }

    private val gson = GsonBuilder().create()

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        val typeName = configs?.get(TYPE_CONFIG)!!.toString()
        runCatching {
             this.type = Class.forName(typeName) as Class<T>
        }.onFailure {
            throw RuntimeException("Type for deserialization does not exist in the classpath")
        }
    }

    override fun deserialize(s: String?, bytes: ByteArray?): T {
        return gson.fromJson(String(bytes!!), type)
    }
}