const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "risk management service",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

const consumer = kafka.consumer({ groupId: "risk management group" });

async function main() {
  await producer.connect();

  await consumer.connect();

  await consumer.subscribe({
    topic: "risk-management-service",
    fromBeginning: true,
  });

  console.log("application connected");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { id, request, score } = JSON.parse(message.value.toString());
      if (id) {
        console.log(
          `received message from ${id} with document ${request} and score ${score}`,
        );

        console.log("checking suggested debt ratio");
        console.log("debt ratio checked successfully");
        console.log("connecting to central bank database");

        const finalScore = (Math.floor(100 * Math.random()) + score) / 2;

        let isApproved;

        if (finalScore > 50) {
          console.log(
            "debt ratio is enought and borrower have not any unpaid commitment",
          );
          isApproved = true;
        } else {
          console.log(
            "debt ratio is not enought or borrower have an unpaid commitment",
          );
          isApproved = false;
        }

        console.log("sending message to risk management service");

        await producer.send({
          topic: "decision",
          messages: [{ value: JSON.stringify({ id, request, isApproved }) }],
        });
        console.log("decision message sent");
        await producer.send({
          topic: "credit-service",
          messages: [{ value: JSON.stringify({ id, isApproved, request }) }],
        });
        console.log("message sent");
      } else {
        console.log("an error has occured");
      }
    },
  });
}

main();
