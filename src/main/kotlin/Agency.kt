import com.rabbitmq.client.*
import java.io.BufferedReader
import java.io.InputStreamReader
import kotlinx.serialization.*
import kotlinx.serialization.json.*

@ImplicitReflectionSerializer
fun main(argv: Array<String>) {
    var internalNumber = 1

    val json = Json(JsonConfiguration.Stable)

    println("Enter agency name: ")
    val br = BufferedReader(InputStreamReader(System.`in`))
    val name = br.readLine()

    val channel = ConnectionFactory().apply { host = "localhost" }.newConnection().createChannel()
    channel.exchangeDeclare("spaceServices", BuiltinExchangeType.TOPIC)

    channel.queueDeclare("agencies.$name", false, false, false, null)
    channel.queueBind("agencies.$name", "spaceServices", "agencies.$name")

    channel.queueDeclare("msg.agencies.$name", false, false, false, null)
    channel.queueBind("msg.agencies.$name", "spaceServices", "msg.agencies")

    val consumer = object : DefaultConsumer(channel) {
        override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: ByteArray) {
            val message = String(body, charset("UTF-8"))
            println("[$name] Received $message")
        }
    }

    channel.basicConsume("agencies.$name", true, consumer)
    channel.basicConsume("msg.agencies.$name", true, consumer)

    loop@ while(true) {
        println("""
        What service do You want to request?
        1) People
        2) Cargo
        3) Satellite
        Enter number:
    """.trimIndent())
        val choice = br.readLine()

        val service = when(choice) {
            "1" -> "carriers.people"
            "2" -> "carriers.cargo"
            "3" -> "carriers.satellite"
            else -> break@loop
        }
        val msg = when(choice) {
            "1" -> "Transport people"
            "2" -> "Transport cargo"
            "3" -> "Place satellite"
            else -> break@loop
        }

        val message: List<String> = listOf(name, msg, internalNumber.toString())
        channel.basicPublish("spaceServices", service, null, json.stringify(message).toByteArray())
        println("[$name] Sent $message")

        internalNumber++
    }

    channel.close()
}