const { Kafka } = require('kafkajs');
const R = require('ramda');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
})

const topic = 'orders';

//todo: pay attention to these figures:

/*
const batchIntervalMins = 60;
const batchIntervalMillis = batchIntervalMins * 60 * 1000;
*/

/*
const howLongWeThinkItWillTakeMins = 10;
const howLongWeThinkItWillTakeMillis = howLongWeThinkItWillTakeMins * 60 * 1000;
const sessionTimeout = batchIntervalMillis  + howLongWeThinkItWillTakeMillis;
*/

//the time between batches
const batchIntervalMinutes = 0.5;
const batchIntervalSeconds = batchIntervalMinutes * 60;
const batchIntervalMillis = batchIntervalSeconds * 1000;

const bufferingIntervalMinutes = 0.1;
const bufferingIntervalSeconds = bufferingIntervalMinutes * 60;
const bufferingIntervalMillis = bufferingIntervalSeconds * 1000;

log(`Batch interval : ${batchIntervalSeconds} Seconds, of which ${bufferingIntervalSeconds} will be spent buffering messages.`)

//todo: will need to be careful to ensure that Kafka doesn't think we've gone away
//todo: heartbeat.interval & session.timeout will be important here
const consumer = kafka.consumer(
    {//sessionTimeout: sessionTimeout,
           groupId: 'my-group'},
)


async function runBatchedConsumer() {

    //todo: figure out logging

    await consumer.connect()

    await consumer.subscribe({topic: topic, fromBeginning: true})

    //todo: will need to be careful to ensure that Kafka doesn't think we've gone away

    //todo: heartbeat.interval & session.timeout will be important here

    let batchStartTime = Date.now();
    let bufferingEndTime = batchStartTime + bufferingIntervalMillis;
    let ordersBuffer = [];

    let offsetsHash = {};

    await consumer.run({
        autoCommit:false, //todo: very important - we must manage the batching/transaction boundaries between this process and iServer
        eachMessage: async ({topic, partition, message}) => {

            // store order in buffer
            // record offset locally for the partition
            // if batching time is over
            //   send batch
            //   record offsets
            //   sleep due to batch

            ordersBuffer = R.append(message, ordersBuffer);
            offsetsHash = R.update(partition, message.offset, offsetsHash);

            if(Date.now() >= bufferingEndTime){
                log("Buffering complete.");
                //start kafka transaction
                sendbatch(ordersBuffer);
                commitOffsetsInKafka(consumer, offsetsHash);
                //commit kafka transaction
                await sleepUntilNextBatch(batchStartTime);
                batchStartTime = Date.now();
                bufferingEndTime = batchStartTime + bufferingIntervalMillis;
                ordersBuffer = [];
                log(`Buffering until ${formatDate(new Date(bufferingEndTime))}...`)
            }

            function sendbatch(messageBuffer) {
                //todo: this will need to run the rules

                log("Sending messages buffer to rules engine topic...")
                //consult the rule engine with the batch of orders (batch of NewOrder topic)
                //store these on their own topic (PickListReady)
                //another process' (iServer client) problem to send each order individually
                log("======================================================================================================");
                log(`Buffer contains ${messageBuffer.length} messages. First message:`);
                logMessage(messageBuffer[0]);
                log("Last message in Buffer:");
                logMessage(messageBuffer[messageBuffer.length - 1]);
                log("======================================================================================================");

                function logMessage(message) {
                    console.log({key: message.key.toString(), value: message.value.toString(), offset: message.offset,});
                }
            }

            function commitOffsetsInKafka(consumer, offsetsHash) {
                const offsets = R.pipe(
                    R.mapObjIndexed(
                        (val, key) => ({ topic, partition: key, offset: val })
                    ),
                    R.values
                )(offsetsHash);
                consumer.commitOffsets(offsets);
            }

            async function sleepUntilNextBatch(startTime) {
                const endTime = Date.now();
                const timeElapsed = endTime - startTime;
                const sleepTime = batchIntervalMillis - timeElapsed;
                if(sleepTime > 0){
                    await sleep(sleepTime);
                }
            }
        },
    })

    //todo: could possibly also use kafka transactions if the driver supports it correctly.  Not really necessary
}

function sleep(ms) {
    return new Promise((resolve) => {
        log("Sleeping until next batch start at:"+formatDate(Date.now()+ms));
        setTimeout(resolve, ms);
    });
}

function formatDate(date) {
    return new Intl.DateTimeFormat('en-GB', options).format(date);
}

function log(message) {
    options = {
        hour: 'numeric', minute: 'numeric', second: 'numeric', millisecond: 'numeric', ms: 'numeric',
    }
    console.log(formatDate(Date.now()) + "::"+ message);
}

runBatchedConsumer();


/**

 //in iserver client:
 //let response = send(message);
 if (response.code == 200){
                //commit offset
                //log correlation Id into kafka: new WarehouseResponse(response, message)

            }
 else{
                if (error.isRecoverable()){

                }

                //backaway for a bit
                //retry
                //dead letter queue

            }



 **/