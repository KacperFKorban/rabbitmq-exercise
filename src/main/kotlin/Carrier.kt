import com.rabbitmq.client.*
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import kotlinx.serialization.parseList
import java.io.BufferedReader
import java.io.InputStreamReader

@ImplicitReflectionSerializer
fun main(argv: Array<String>) {
    println("Enter carrier name: ")
    val br = BufferedReader(InputStreamReader(System.`in`))
    val name = br.readLine()
    val json = Json(JsonConfiguration.Stable)

    println(
        """
        What services does the carrier provide?
        1) People and Cargo
        2) People and Satellite
        3) Cargo and Satellite
        Enter number:
    """.trimIndent()
    )
    val choice = br.readLine()
    val services = when (choice) {
        "1" -> listOf("carriers.people", "carriers.cargo")
        "2" -> listOf("carriers.people", "carriers.satellite")
        "3" -> listOf("carriers.cargo", "carriers.satellite")
        else -> listOf()
    }

    val channel = ConnectionFactory().apply { host = "localhost" }.newConnection().createChannel()

    channel.exchangeDeclare("spaceServices", BuiltinExchangeType.TOPIC)
    services.forEach {
        channel.queueDeclare(it, false, false, false, null)
        channel.queueBind(it, "spaceServices", it)
    }

    channel.queueDeclare("msg.carriers.$name", false, false, false, null)
    channel.queueBind("msg.carriers.$name", "spaceServices", "msg.carriers")

    val consumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: ByteArray) {
            val message = json.parseList<String>(String(body, charset("UTF-8")))
            println("[$name] Received $message")
            channel.basicPublish("spaceServices", "agencies.${message[0]}", null, "Task ${message[2]} done!".toByteArray())
        }
    }
    services.forEach {
        channel.basicConsume(it, true, consumer)
    }

    val msgConsumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: ByteArray) {
            val message = String(body, charset("UTF-8"))
            println("[$name] Received $message")
        }
    }

    channel.basicConsume("msg.carriers.$name", true, msgConsumer)
}