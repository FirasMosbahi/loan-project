const express = require("express");
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "client service",
  brokers: ["localhost:9092"],
});
const app = express();
app.use(express.json());

const producer = kafka.producer();

async function main() {
  const consumer = kafka.consumer({ groupId: "client service group" });

  await consumer.connect();

  await consumer.subscribe({ topic: "argument", fromBeginning: true });

  await consumer.subscribe({ topic: "decision", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (topic === "decision") {
        const { id, isApproved, request } = JSON.parse(
          message.value.toString(),
        );
        if (id) {
          console.log(
            `received message from ${id} with document ${request} and decision ${isApproved ? "is approved" : "is not approved"}`,
          );

          if (isApproved) {
            console.log("the loan request has been approved");
            console.log("please wait while the argument document is generated");
          }
        } else {
          console.log("an error has occured");
        }
      }
      if (topic === "argument") {
        const { id, argument, request } = JSON.parse(message.value.toString());
        if (id) {
          console.log(
            `received message from ${id} with document ${request} and argument ${argument}`,
          );
          console.log("your argument is ready");
          console.log("please verify the argument and sign it");
        } else {
          console.log("an error has occured");
        }
      }
    },
  });
}

app.post("/", async (req, res) => {
  const { id, request } = req.body;
  console.log("receiving client borrow request");
  console.log("uploading client request document");
  console.log("sending request to commercial servie");

  await producer.connect();

  await producer.send({
    topic: "commercial-service",
    messages: [{ value: JSON.stringify({ id, request }) }],
  });

  res.send({ message: "request send successfully!" });
});

app.listen(3000, () => {
  console.log("Server listening on port 3000");
});

main();
