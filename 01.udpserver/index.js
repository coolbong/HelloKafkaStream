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

    server.on('message', (msg, remote) {
        let obj = {
            msg: msg,
            remote: remote
        };
        worker.postMessage(obj);
    });

    server.bind(server_port, server_ip);

} else {
    //assert.strictEqual(workerData.num, 42);
    parentPort.on('message', (msg) => {
        console.log('We got a message from the main thread: ', msg);
    });
    parentPort.postMessage('Hello, World');
}

