
const kafka = require('kafka-node');

const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
const producer = new kafka.Producer(client);

const fs = require('fs');

const catfact_arr = [
    'Unlike dogs, cats do not have a sweet tooth. Scientists believe this is due to a mutation in a key taste receptor.',
    'When a cat chases its prey, it keeps its head level. Dogs and humans bob their heads up and down.',
    'The technical term for a cat’s hairball is a “bezoar.”',
    'A group of cats is called a “clowder.”',
    'A cat can’t climb head first down a tree because every claw on a cat’s paw points the same way. To get down from a tree, a cat must back down.',
    'Cats make about 100 different sounds. Dogs make only about 10.',
    'Every year, nearly four million cats are eaten in Asia.',
    'There are more than 500 million domestic cats in the world, with approximately 40 recognized breeds.',
    'Approximately 24 cat skins can make a coat.',
    'While it is commonly thought that the ancient Egyptians were the first to domesticate cats, the oldest known pet cat was recently found in a 9,500-year-old grave on the Mediterranean island of Cyprus. This grave predates early Egyptian art depicting cats by 4,000 years or more.'
];

producer.on('ready', () => {
    console.log('kafka ready');
});

producer.on('error', (err) => {
    console.error(err);
});


function produce_catfact() {
    //catfact_arr
    const payloads = [
        { topic: 'wordcount-input', messages: catfact_arr }
    ];
    producer.send(payloads, (err, data) => {
        if (err) {
            console.error(err);
        }
    });
}

function produce_iplog() {
    const readline = require('readline');
    try {
        const rl = readline.createInterface({
            input: fs.createReadStream('./assets/iplog.log'), 
            crlfDelay : Infinity
        });

        rl.on('line', (line) => {
            const payloads = [
                { topic: 'iplog-input', messages: [line] }
            ];
            producer.send(payloads, (err, data) => {
                if (err) {
                    console.error(err);
                }
            });
        })
    } catch (err) {
        console.error(err);
    }
    
}

function produce_username() {
    
    const user_name = [
        'angie',
        'guy',
        'kate',
        'mark'
    ];
    const payloads = [
        { topic: 'users-topic', messages: user_name }
    ];
    producer.send(payloads, (err, data) => {
        console.log(data);
        if (err) {
            console.error(err);
        }
    });
}


//produce_catfact();
//produce_iplog();

produce_username();
