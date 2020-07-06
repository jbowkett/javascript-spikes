const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const { CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'order-producer',
    brokers: ['localhost:9092']
})


class OrderItem{
    constructor(productCode, quantity){
        this.productCode = productCode;
        this.quantity = quantity;
    }
}

class Order{
    constructor(orderId, items, orderDate){
        this.orderId = orderId;
        this.items = items;
        this.orderDate = orderDate;
    }
}


function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

const ACKS_ALL = -1;

async function writeOrder(producer) {
    console.log("sending message...")

    const order = new Order(uuidv4(), [], new Date())

    //todo: I don't like the lack of a versioned schema in the messages being sent here:
    await producer.send({
        topic: 'orders',
        acks: ACKS_ALL,
        compression: CompressionTypes.GZIP, //todo: snappy is not supported currently
        messages: [
            {   key: order.orderId,
                value: JSON.stringify(order)},
        ],
    })
    console.log("Message sent.")
}

async function writeOrders(producer) {
    for (let i = 0; i < 5000; i++) {
        writeOrder(producer);
        await(sleep(2000));
    }
}

async function openConnection() {
    const producer = kafka.producer(
        {
            idempotent: true
            // maxInFlightRequests: MAX_SAFE_INTEGER // todo: in most - if not all - cases in TheBox, we won't worry about
            // in-flight requests, as ordering of Orders is likely to be unimportant
        }
    )
    await producer.connect()
    return producer;
}

async function closeConnection(producer) {
    await producer.disconnect()
}

async function startProducer() {
    const producer = await openConnection();
    await writeOrders(producer);

    await closeConnection(producer);

}

startProducer()