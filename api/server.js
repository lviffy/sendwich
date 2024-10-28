const WebSocket = require('ws');

// Constants
const MAX_CONNECTIONS = 100;
const HEARTBEAT_INTERVAL = 30000;
const CLIENT_TIMEOUT = 35000;
const MAX_MESSAGE_SIZE = 16 * 1024; // 16KB max message size
const SESSION_CLEANUP_INTERVAL = 60 * 60 * 1000; // 1 hour

// Store active sessions and their connections
const sessions = new Map();
const clients = new Map();

// Add statistics tracking
const stats = {
    totalConnections: 0,
    activeConnections: 0,
    totalSessions: 0,
    activeSessions: 0,
    bytesTransferred: 0
};

function handleUpgrade(req, socket, head) {
    const wss = new WebSocket.Server({ noServer: true });

    wss.on('connection', function connection(ws, req) {
        const clientId = generateClientId();
        clients.set(ws, {
            id: clientId,
            isAlive: true,
            sessionKey: null,
            lastPing: Date.now()
        });

        console.log(`Client connected: ${clientId}`);

        // Set up ping-pong for connection health check
        ws.on('pong', () => {
            const client = clients.get(ws);
            if (client) {
                client.isAlive = true;
                client.lastPing = Date.now();
            }
        });

        // Handle incoming messages
        ws.on('message', function incoming(message) {
            if (message.length > MAX_MESSAGE_SIZE) {
                sendError(ws, 'Message too large');
                return;
            }
            try {
                const data = JSON.parse(message);
                handleMessage(ws, data);
            } catch (error) {
                console.error('Error processing message:', error);
                sendError(ws, 'Invalid message format');
            }
        });

        // Handle client disconnect
        ws.on('close', () => handleDisconnect(ws));

        // Handle errors
        ws.on('error', (error) => {
            console.error('WebSocket error:', error);
            handleDisconnect(ws);
        });

        stats.totalConnections++;
        stats.activeConnections++;
    });

    wss.handleUpgrade(req, socket, head, function done(ws) {
        wss.emit('connection', ws, req);
    });
}

// Message handler
function handleMessage(ws, data) {
    const client = clients.get(ws);
    if (!client) return;

    switch (data.type) {
        case 'create-session':
            handleCreateSession(ws);
            break;
        case 'join-session':
            handleJoinSession(ws, data.sessionKey);
            break;
        case 'chat-message':
            handleChatMessage(ws, data);
            break;
        default:
            // Forward WebRTC signaling messages
            forwardSignalingMessage(ws, data);
            break;
    }
}

// Session handlers
function handleCreateSession(ws) {
    const sessionKey = generateSessionKey();
    const client = clients.get(ws);
    
    if (client) {
        client.sessionKey = sessionKey;
        sessions.set(sessionKey, {
            sender: ws,
            receiver: null,
            created: Date.now()
        });

        ws.send(JSON.stringify({
            type: 'session-created',
            sessionKey
        }));

        console.log(`Session created: ${sessionKey}`);
        stats.totalSessions++;
        stats.activeSessions++;
    }
}

function handleJoinSession(ws, sessionKey) {
    const session = sessions.get(sessionKey);
    const client = clients.get(ws);

    if (!session) {
        sendError(ws, 'Invalid session key');
        return;
    }

    if (session.receiver) {
        sendError(ws, 'Session already has a receiver');
        return;
    }

    session.receiver = ws;
    if (client) {
        client.sessionKey = sessionKey;
    }

    ws.send(JSON.stringify({ type: 'session-joined' }));
    if (session.sender) {
        session.sender.send(JSON.stringify({ type: 'receiver-joined' }));
    }

    console.log(`Client joined session: ${sessionKey}`);
}

function handleChatMessage(ws, data) {
    const session = sessions.get(data.sessionKey);
    if (!session) return;

    const target = session.sender === ws ? session.receiver : session.sender;
    if (target && target.readyState === WebSocket.OPEN) {
        target.send(JSON.stringify({
            type: 'chat-message',
            message: data.message
        }));
    }
}

function forwardSignalingMessage(ws, data) {
    const session = sessions.get(data.sessionKey);
    if (!session) return;

    const target = session.sender === ws ? session.receiver : session.sender;
    if (target && target.readyState === WebSocket.OPEN) {
        target.send(JSON.stringify(data));
    }
}

// Utility functions
function generateSessionKey() {
    return Math.random().toString(36).substring(2, 8).toUpperCase();
}

function generateClientId() {
    return Math.random().toString(36).substring(2, 15);
}

function sendError(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'error',
            message
        }));
    }
}

function handleDisconnect(ws) {
    const client = clients.get(ws);
    if (!client) return;

    console.log(`Client disconnected: ${client.id}`);

    if (client.sessionKey) {
        const session = sessions.get(client.sessionKey);
        if (session) {
            const peer = session.sender === ws ? session.receiver : session.sender;
            if (peer && peer.readyState === WebSocket.OPEN) {
                peer.send(JSON.stringify({ type: 'peer-disconnected' }));
            }
            sessions.delete(client.sessionKey);
        }
    }

    clients.delete(ws);
    stats.activeConnections--;
    if (client.sessionKey) {
        stats.activeSessions--;
    }
}

// Cleanup intervals
setInterval(() => {
    const now = Date.now();

    // Clean up inactive clients
    clients.forEach((client, ws) => {
        if (now - client.lastPing > CLIENT_TIMEOUT) {
            console.log(`Client timeout: ${client.id}`);
            ws.terminate();
            clients.delete(ws);
        } else {
            ws.ping();
        }
    });

    // Clean up old sessions
    sessions.forEach((session, key) => {
        if (now - session.created > 24 * 60 * 60 * 1000) { // 24 hours
            sessions.delete(key);
        }
    });
}, HEARTBEAT_INTERVAL);

// Add health check endpoint
function handleHealthCheck(req, res) {
    res.status(200).json({
        status: 'healthy',
        stats: {
            ...stats,
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            sessions: sessions.size,
            clients: clients.size
        }
    });
}

// Export for Vercel serverless function
module.exports = (req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
        handleHealthCheck(req, res);
        return;
    }
    if (req.method === 'POST') {
        // Handle potential HTTP POST requests
        res.status(200).json({ status: 'WebSocket server is running' });
        return;
    }

    if (!req.headers.upgrade || req.headers.upgrade.toLowerCase() !== 'websocket') {
        res.status(426).json({ error: 'Upgrade Required' });
        return;
    }

    if (clients.size >= MAX_CONNECTIONS) {
        res.status(503).json({ error: 'Server is at capacity' });
        return;
    }

    try {
        handleUpgrade(req, req.socket, Buffer.alloc(0));
    } catch (error) {
        console.error('Upgrade error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
};
