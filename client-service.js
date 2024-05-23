const express = require('express');
const {kafka} = require('./libs/kafka-utils')
const app = express();
app.use(express.json());

const producer = kafka.producer()



    app.post('/', async (req, res) => {
        const {id , request} = req.body;
        console.log("receiving client borrow request")
        console.log("uploading client request document")
        console.log("sending request to commercial servie")

        await producer.connect()

        await producer.send({
            topic: 'commercial-service',
            messages: [
                { value: {id , request} },
            ],
        })

        res.send({message : 'request send successfully!'});
    });


app.listen(3000, () => {
    console.log('Server listening on port 3000');
});