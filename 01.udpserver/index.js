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
        processRawPacket(worker, msg, remote);

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

    parentPort.on('message', (msg) => {
        //console.log('We got a message from the main thread: ', msg);
        const value = Buffer.from(JSON.stringify(msg));
        producer.produce(topic, partition, value);
    });

}

const fs = require('fs');
function processRawPacket(worker, msg, remote) {
    let dataOut = {
        gameOn: msg.readInt32LE(0), //4
        timestamp: msg.readUInt32LE(4), //4
        //rpm
        maxRPM: msg.readFloatLE(8), //4
        idleRPM: msg.readFloatLE(12), //4
        currentRPM: msg.readFloatLE(16), //4
        //accel
        accelX: msg.readFloatLE(20), //4
        accelY: msg.readFloatLE(24), //4
        accelZ: msg.readFloatLE(28), //4
        //vel
        velX: msg.readFloatLE(32), //4
        velY: msg.readFloatLE(36), //4
        velZ: msg.readFloatLE(40), //4
        //angvel
        angX: msg.readFloatLE(44), //4
        angY: msg.readFloatLE(48), //4
        angZ: msg.readFloatLE(52), //4
        //ang
        yaw: msg.readFloatLE(56), //4
        pitch: msg.readFloatLE(60), //4
        roll: msg.readFloatLE(64), //4
    
        //ignoring tech stuff i don't care about, skip 144 bytes
    
        carID: msg.readInt32LE(212), //4
        carClass: msg.readInt32LE(216), //4
        carPreform: msg.readInt32LE(220), //4
        drivetrainType: msg.readInt32LE(224), //4
        numCyl: msg.readInt32LE(228), //4
    
        //v2 info
        //pos
        posX: msg.readFloatLE(244), //4
        posY: msg.readFloatLE(248), //4
        posZ: msg.readFloatLE(252), //4
    
        //spd
        speed: msg.readFloatLE(256), //4 // meters per second
        power: msg.readFloatLE(260), //4 // watts
        torque: msg.readFloatLE(264), //4 // newton meter
    
        flTemp: msg.readFloatLE(268), //4
        frTemp: msg.readFloatLE(272), //4
        blTemp: msg.readFloatLE(276), //4
        brTemp: msg.readFloatLE(280), //4
    
        boost: msg.readFloatLE(284), //4
        fuel: msg.readFloatLE(288), //4
        travel: msg.readFloatLE(292), //4
        bestLap: msg.readFloatLE(296), //4
        lastLap: msg.readFloatLE(300), //4
        currentLap: msg.readFloatLE(304), //4
        currentRaceTime: msg.readFloatLE(308), //4
    
        lapNumber: msg.readInt16LE(312), //2
        racePos: msg.readUInt8(314), //1
    
        accel: msg.readUInt8(315), //1
        brake: msg.readUInt8(316), //1
        clutch: msg.readUInt8(317), //1
        handBrake: msg.readUInt8(318), //1
        gear: msg.readUInt8(319), //1
        steer: msg.readInt8(320), //1
    
        drivingLine: msg.readInt8(321), //1
        brakeDifference: msg.readInt8(322), //1
    };

    const data = JSON.stringify(dataOut);
    console.log(data);

    const topic = 'quickstart';
    fs.appendFile(`${topic}.txt`, data + '\n', (err) => {
        if (err) {
            console.log(err);
        }
    });

    worker.postMessage(dataOut);
}
