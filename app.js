const net = require('net');

// Create a proxy server
const proxyServer = net.createServer();

// MongoDB server address and port
const mongoDBHost = '127.0.0.1'; // localhost
const mongoDBPort = 27017; // default MongoDB port

// Function to handle data transfer between client and MongoDB server
function handleData(clientSocket, mongoDBSocket) {
    let headerBuffer = Buffer.alloc(16); // Buffer to store the header

    clientSocket.on('data', data => {
        // Append data to the header buffer
        headerBuffer = Buffer.concat([headerBuffer, data]);

        // If header buffer is complete
        if (headerBuffer.length >= 16) {
            // Extract header fields
            const messageLength = headerBuffer.readInt32LE(0);
            const requestID = headerBuffer.readInt32LE(4);
            const responseTo = headerBuffer.readInt32LE(8);
            const opCode = headerBuffer.readInt32LE(12);

            console.log(opCode);

            // Forward the data to MongoDB server
            mongoDBSocket.write(headerBuffer);

            // Reset the header buffer for next message
            headerBuffer = Buffer.alloc(0);

            // If there's additional body data, forward it to MongoDB
            const bodyData = data.slice(16);
            if (bodyData.length > 0) {
                mongoDBSocket.write(bodyData);
            }
        }
    });

    mongoDBSocket.on('data', data => {
        // Forward MongoDB server response to the client
        clientSocket.write(data);
    });

    // Handle client and MongoDB server disconnection
    clientSocket.on('end', () => {
        mongoDBSocket.end();
    });

    mongoDBSocket.on('end', () => {
        clientSocket.end();
    });

    clientSocket.on('error', err => {
        console.error('Client socket error:', err);
    });

    mongoDBSocket.on('error', err => {
        console.error('MongoDB socket error:', err);
        // Optionally attempt to reconnect to the MongoDB server
        // Close the existing socket and create a new connection
        mongoDBSocket.destroy();
        const newMongoDBSocket = net.connect(mongoDBPort, mongoDBHost);
        handleData(clientSocket, newMongoDBSocket);
    });
}

// Event listener for new client connections
proxyServer.on('connection', clientSocket => {
    // Connect to the MongoDB server
    const mongoDBSocket = net.connect(mongoDBPort, mongoDBHost);

    // Handle data transfer between client and MongoDB server
    handleData(clientSocket, mongoDBSocket);
});

// Error handler for the proxy server
proxyServer.on('error', err => {
    console.error('Proxy server error:', err);
});

// Start the proxy server
const proxyPort = 3000; // You can change this port as needed
proxyServer.listen(proxyPort, () => {
    console.log(`Proxy server is running on port ${proxyPort}`);
});
