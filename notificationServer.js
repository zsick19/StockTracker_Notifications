const amqp = require('amqplib')
const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors')

const app = express();
const server = http.createServer(app);

app.use(cors())

const io = new Server(server, {
    cors: {
        origin: ['http://localhost:5173'],
        methods: ["GET", "POST"]
    }
})

const rabbitQueueNames = {
    loggedInEnterExitPlanQueue: 'enterExitWatchListPrice',
    loggedInActiveTradeQueue: 'activeTradePrice',
    pricePointHitQueue: 'pricePointHit_queue',
    removeTempTickerQueue: 'removeTempTicker'
}

let rabbitConnection = undefined
let rabbitChannel = undefined
let stockMonitorConnection
const loggedInUsers = {}

io.on("connection", (socket) =>
{
    socket.on("user_logon", (userId) =>
    {
        loggedInUsers[userId] = socket
        console.log(`FrontEndUser: ${userId} connected via SocketId: ${socket.id}`)
    });

    socket.on('monitorServerConnected', (data) =>
    {
        stockMonitorConnection = socket
        console.log(`Monitor Server Established Connection: ${data.connectionId}`)
    })

    socket.on('tradeStream', (data) =>
    {
        relayTrade(data)
    })

    socket.on('disconnectedStream', (data) =>
    {
        if (stockMonitorConnection) stockMonitorConnection.emit('removeTempTradeStream', { data })
    })

    socket.on('disconnectTempStream', (data) =>
    {
        const msg = JSON.stringify(data)
        rabbitChannel.sendToQueue(rabbitQueueNames.removeTempTickerQueue, Buffer.from(msg), { persistent: true })
    })

    console.log('Connection Established With Notification Server, details pending...')
});


consumeMessages()


async function consumeMessages()
{
    try
    {
        rabbitConnection = await amqp.connect('amqp://localhost');
        rabbitChannel = await rabbitConnection.createChannel();
        await rabbitChannel.assertQueue(rabbitQueueNames.pricePointHitQueue, { durable: true });
        await rabbitChannel.assertQueue(rabbitQueueNames.loggedInEnterExitPlanQueue, { durable: true })
        await rabbitChannel.assertQueue(rabbitQueueNames.loggedInActiveTradeQueue, { durable: true })

        rabbitChannel.prefetch(1)

        console.log('Consumer connected to RabbitMQ. Waiting for message....')

        rabbitChannel.consume(rabbitQueueNames.pricePointHitQueue, (message) =>
        {
            if (message !== null)
            {
                const content = JSON.parse(message.content.toString());
                console.log(content)
                let recipientId = content.userId
                console.log(`Buffer Hit Message Received for user ${recipientId} as ${content?._id}`)

                try
                {
                    let socket = loggedInUsers['6952bd331482f8927092ddcc']
                    if (socket) { socket.emit('bufferHit_msg', content) }
                    else { console.log("socket recipient not"); }
                } catch (error)
                {
                    console.log(error)
                }
                rabbitChannel.ack(message);
            }
        }, { noAck: false });


        rabbitChannel.consume(rabbitQueueNames.loggedInEnterExitPlanQueue, (message) =>
        {
            if (message !== null)
            {
                const content = JSON.parse(message.content.toString())

                try
                {
                    let socket = loggedInUsers[content.userId]
                    if (socket) { socket.emit('enterExitWatchListPrice', content) }
                    else { throw new Error("Socket recipient not found"); }
                } catch (error)
                {
                    console.log(error)
                }
                rabbitChannel.ack(message);
            }
        }, { noAck: false })
        rabbitChannel.consume(rabbitQueueNames.loggedInActiveTradeQueue, (message) =>
        {
            if (message !== null)
            {
                const content = JSON.parse(message.content.toString())
                try
                {
                    let socket = loggedInUsers[content.userId]
                    if (socket) { socket.emit('activeTradePrice', content) }
                    else { throw new Error("Socket recipient not found"); }
                } catch (error)
                {
                    console.log(error)
                }
                rabbitChannel.ack(message);
            }
        }, { noAck: false })



    } catch (error)
    {
        console.error("Failed to connect to RabbitMQ or consume messages:", error);
        process.exit(1);
    }

}

async function relayTrade(data)
{
    data.users.forEach(userId =>
    {
        try
        {
            let userSocket = loggedInUsers[userId]
            if (userSocket) { userSocket.emit('singleLiveChart', data.trade) }
            else { console.log("socket recipient not"); }
        } catch (error)
        {
            console.log(error)
        }
    });
}


server.listen(8080, () => { console.log('Notification server connected on port 8080.'); });

