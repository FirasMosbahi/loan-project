const { Kafka ,  } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'credit service',
    brokers: ['localhost:9092'],
})
const producer = kafka.producer()


const consumer = kafka.consumer({ groupId: 'credit group' })


async function main() {

    await producer.connect()

    await consumer.connect()

    await consumer.subscribe({ topic: 'credit-service', fromBeginning: true })

    console.log("application connected")

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            // console.log({
            //     value: message.value.toString(),
            // })
            const {id ,request, isApproved} = JSON.parse(message.value.toString())
            if(id) {
                console.log(`received message from ${id} with document ${request} and decision ${isApproved ? "is approved" : "is not approved"}`)

                if(isApproved) {
                    console.log('creating argument')
                    await producer.send({
                        topic: 'argument',
                        messages: [
                            { value: JSON.stringify({id , request , argument : 'argument'}) },
                        ],
                    })
                    console.log("decision message sent")
                }
            }else {
                console.log("an error has occured")
            }
        },
    })
}

main()
