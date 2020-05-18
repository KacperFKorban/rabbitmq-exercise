import com.rabbitmq.client.*
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import java.io.BufferedReader
import java.io.InputStreamReader

@ImplicitReflectionSerializer
fun main(argv: Array<String>) {
    val br = BufferedReader(InputStreamReader(System.`in`))
    val json = Json(JsonConfiguration.Stable)

    val channel = ConnectionFactory().apply { host = "localhost" }.newConnection().createChannel()
    channel.exchangeDeclare("spaceServices", BuiltinExchangeType.TOPIC)

    val consumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: ByteArray) {
            val message = String(body, charset("UTF-8"))
            println("[Admin] Received $message")
        }
    }

    listOf("carriers.*", "agencies.*").forEach {
        channel.queueDeclare(it, false, false, false, null)
        channel.queueBind(it, "spaceServices", it)
    }
    listOf("carriers.*", "agencies.*").forEach {
        channel.basicConsume(it, true, consumer)
    }

    while(true) {
        println("""
            Which entities do You want to send a message to?
            1) Agencies
            2) Carriers
            3) All
            Enter number:
        """.trimIndent())
        val choice = br.readLine()
        val entities = when (choice) {
            "1" -> listOf("msg.agencies")
            "2" -> listOf("msg.carriers")
            "3" -> listOf("msg.agencies", "msg.carriers")
            else -> listOf()
        }

        println("Enter message: ")
        val message = br.readLine()

        entities.forEach {
            channel.basicPublish("spaceServices", it, null, message.toByteArray())
            println("[Admin] Sent $message")
        }

        channel.exchangeDeclare("spaceServices", BuiltinExchangeType.TOPIC)
        entities.forEach {
            channel.queueDeclare(it, false, false, false, null)
            channel.queueBind(it, "spaceServices", it)
        }
    }

}