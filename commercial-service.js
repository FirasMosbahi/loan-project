const {kafka} = require("./libs/kafka-utils");

const producer = kafka.producer()


const consumer = kafka.consumer({ groupId: 'test-group' })


async function main() {

    await producer.connect()

    await consumer.connect()

    await consumer.subscribe({ topic: 'commercial-service', fromBeginning: true })

    console.log("application connected")

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            // console.log({
            //     value: message.value.toString(),
            // })
            const {id , request} = JSON.parse(message.value.toString())
            if(id) {
                console.log(`received message from ${id} with document ${request}`)

                console.log('checking eligibility')
                console.log('assigning score')

                const score = Math.floor(100 * Math.random())

                console.log('sending message to risk management service')

                await producer.send({
                    topic: 'risk-management-service',
                    messages: [
                        { value: JSON.stringify({id , request , score}) },
                    ],
                })
                console.log("message sent")
            }else {
                console.log("an error has occured")
            }
        },
    })
}

main()
