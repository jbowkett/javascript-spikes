const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

//todo: pay attention to these figures:

const batchIntervalMins = 60;
const batchIntervalMillis = batchIntervalMins * 60 * 1000;

const howLongWeThinkItWillTakeMins = 10;
const howLongWeThinkItWillTakeMillis = howLongWeThinkItWillTakeMins * 60 * 1000;
const sessionTimeout = batchIntervalMillis  + howLongWeThinkItWillTakeMillis;

//todo: will need to be careful to ensure that Kafka doesn't think we've gone away
//todo: heartbeat.interval & session.timeout will be important here
const consumer = kafka.consumer(
    {//sessionTimeout: sessionTimeout,
           groupId: 'my-group'},
)

async function runBatchedConsumer() {
    await consumer.connect()

    await consumer.subscribe({topic: 'orders', fromBeginning: true})

    //todo: will need to be careful to ensure that Kafka doesn't think we've gone away
    //todo: heartbeat.interval & session.timeout will be important here
    await consumer.run({
        autoCommit:false, //todo: very important - we must manage the batching/transaction boundaries between this process and iServer
        eachMessage: async ({topic, partition, message}) => {

            //sleep for batch_interval ms
            //how long should we consume for at most?
            //figure out how what time to consume until
            //if it's now that time, then we send the batch off to the rules engine
            //
            function getInterBatchSleep() {
                return undefined;
            }

            //should I sleep due to batch? (i.e. am I between batches?)
            //when does consumption end?
            //if time is before when consumption ends, then
            //  consume and store order in buffer
            //  record offset for the partition
            //else
            //  send batch
            //  record offsets
            //  calculate batch sleep end


            // const intraBatchSleep = getIntraBatchSleep();
            // await(sleep(intraBatchSleep));
            // const endTimeForConsumption = new Date() + consumptionWindowMs;



            console.log({
                key: message.key.toString(),
                value: message.value.toString(),
                headers: message.headers,
                offset: message.offset
            })
        },
    })

    //todo: could possibly also use kafka transactions if the driver supports it correctly
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}


runBatchedConsumer();