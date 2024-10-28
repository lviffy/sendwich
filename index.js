const express = require('express');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

// Serve static files from the public directory
app.use(express.static('public'));

// Serve index.html for the root path
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve receiver.html for /receive path
app.get('/receive', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'receiver.html'));
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'healthy', uptime: process.uptime() });
});

// Add these routes
app.get('/sender', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'sender.html'));
});

app.get('/about', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'about.html'));
});

// Replace YOUR_CLIENT_ID with your actual Socket.io Cloud Client ID
const io = new Server({
    cors: {
        origin: "*"
    }
});

io.on('connection', (socket) => {
    console.log('a user connected');
    
    socket.on('message', (data) => {
        socket.broadcast.emit('message', data);
    });

    socket.on('file', (data) => {
        socket.broadcast.emit('file', data);
    });
});

// Start server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});

// Handle graceful shutdown
process.on('SIGTERM', () => {
    console.log('SIGTERM received. Shutting down gracefully...');
    app.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});

