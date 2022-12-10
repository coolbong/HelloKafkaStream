const dgram = require('dgram');


const server_ip = '127.0.0.1';
const server_port = 9000;

const list = [
    'data 1',
    'data 2'
];



list.forEach((line) => {
    const buffer = Buffer.from(line);
    const udp = dgram.createSocket('udp4');
    udp.send(buffer, 0, buffer.length, server_port, server_ip, (err, bytes) => {
        if(err) throw err;
        udp.close();
    });

})
