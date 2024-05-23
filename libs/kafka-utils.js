const { Kafka ,  } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
})

// async function produce(topic , message) {
//     const producer = kafka.producer()
//
//     await producer.connect()
//     await producer.send({
//         topic,
//         messages: [
//             {value : message}
//         ],
//     })
//
//     await producer.disconnect()
// }
//
//
// /**
//  *
//  * @param topic the topic name in which we should subscribe
//  * @param callback a function with three parameters topic , partition and message (message.value.toString())
//  * @returns {Promise<void>}
//  */
// async function consume(topic , callback) {
//     const consumer = kafka.consumer({groupId : "test"})
//     await consumer.connect()
//     await consumer.subscribe({ topic , fromBeginning: true })
//
//
//     await consumer.run({
//         eachMessage: callback
//     })
// }
//

module.exports = {kafka}