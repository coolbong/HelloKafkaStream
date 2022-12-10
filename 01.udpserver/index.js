const {Worker, isMainThread, parentPort} = require('worker_threads');


if (isMainThread) {
    const worker = new Worker(__filename);
    const server_ip = '127.0.0.1';
    const server_port = 9000;

    const dgram = require('dgram');
    const server = dgram.createSocket('udp4');
    server.on('listening', () => {
        const address = server.address();
        console.log('UDP server listening on ' + address.server + ': ' + address.port);
    });

    server.on('message', (msg, remote) => {
        let obj = {
            //msg: msg.toString('utf-8'),
            msg: msg.toString('hex').toUpperCase(),
            remote: remote
        };
        worker.postMessage(obj);
    });

    server.bind(server_port, server_ip);

} else {

    // part 1 write file
    const fs = require('fs');

    // part 2 kafka producer
    const topic = 'quickstart';
    //const topic = 'forza4-event-raw';

    const Kafka = require('node-rdkafka');
    const partition = -1;

    const producer = new Kafka.Producer({
        'metadata.broker.list': 'localhost:9092',
        'dr_cb': true  //delivery report callback
    });

    producer.on('ready', function(arg) {
        console.log('producer ready.' + JSON.stringify(arg));
    });

    producer.on('event.log', function(log) {
        console.log(log);
    });

    //logging all errors
    producer.on('event.error', function(err) {
        console.error('Error from producer');
        console.error(err);
    });

    producer.on('delivery-report', function(err, report) {
        console.log('delivery-report: ' + JSON.stringify(report));
    });

    producer.connect();

    parentPort.on('message', (obj) => {
        //console.log('We got a message from the main thread: ', msg);

        const data = JSON.stringify(obj);
        fs.appendFile(`${topic}.txt`, data + '\n', (err) => {
            if (err) {
                console.log(err);
            }
        });

        const value = Buffer.from(data);
        producer.produce(topic, partition, value);
    });
}

