// Constants
const CHUNK_SIZE = 256 * 1024; // 256KB chunks for better performance
const BUFFER_THRESHOLD = CHUNK_SIZE * 8; // Buffer threshold before throttling
const MAX_RECONNECT_ATTEMPTS = 5;
const MAX_CHANNEL_RETRIES = 3;

// DOM Elements
const fileInput = document.getElementById("fileInput");
const sendFileButton = document.getElementById("sendFile");
const status = document.getElementById("status");
const progressBar = document.getElementById("progressBar");
const progressText = document.getElementById("progressText");
const copySessionKeyButton = document.getElementById("copySessionKey");
const chatWindow = document.getElementById("chatWindow");

// WebRTC Configuration
const configuration = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
    ]
};

// State variables
const peerConnection = new RTCPeerConnection(configuration);
let ws;
let dataChannel;
let fileQueue = [];
let currentFile = null;
let receivedSize = 0;
let fileSize = 0;
let fileName = '';
let fileChunks = [];
let wsReconnectAttempts = 0;
let sessionKey = null;
let transferStartTime = 0;
let lastUpdateTime = 0;
let bytesPerSecond = 0;
const messageQueue = [];
let isProcessingQueue = false;

// WebSocket Connection
function connectWebSocket() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsPath = window.location.hostname === 'localhost' ? '' : '/api/ws';
    ws = new WebSocket(`${wsProtocol}//${window.location.host}${wsPath}`);
    setupWebSocket();
    return ws;
}

// Message Queue System
async function processMessageQueue() {
    if (isProcessingQueue || messageQueue.length === 0) return;
    
    isProcessingQueue = true;
    
    while (messageQueue.length > 0) {
        const message = messageQueue.shift();
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(message);
            await new Promise(resolve => setTimeout(resolve, 10));
        }
    }
    
    isProcessingQueue = false;
}

function sendWSMessage(message) {
    messageQueue.push(JSON.stringify(message));
    processMessageQueue();
}

// WebSocket event handlers
function setupWebSocket() {
    ws.onopen = () => {
        console.log("Connected to server");
        status.textContent = "Connected to server";
        enableFileInput(true);
        wsReconnectAttempts = 0;
        
        // Create session on connect
        sendWSMessage({ type: 'create-session' });
    };

    ws.onclose = () => {
        console.log("Disconnected from server");
        status.textContent = "Disconnected from server";
        enableFileInput(false);
        document.getElementById('sessionInfo').style.display = 'none';

        if (wsReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            setTimeout(() => {
                wsReconnectAttempts++;
                status.textContent = `Reconnecting... (${wsReconnectAttempts})`;
                connectWebSocket();
            }, 3000 * wsReconnectAttempts);
        } else {
            status.textContent = "Connection failed. Please refresh the page.";
        }
    };

    ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        status.textContent = "Connection error";
        enableFileInput(false);
        if (ws.readyState !== WebSocket.CLOSED) {
            ws.close();
        }
    };

    ws.onmessage = async (event) => {
        try {
            const message = JSON.parse(event.data);
            if (message.type === 'session-created') {
                sessionKey = message.sessionKey;
                status.textContent = `Session created: ${sessionKey}`;
                document.getElementById('sessionKey').textContent = sessionKey;
                document.getElementById('sessionInfo').style.display = 'flex';
            } else if (message.type === 'peer-connected') {
                status.textContent = "Peer connected";
            } else if (message.type === 'peer-disconnected') {
                status.textContent = "Peer disconnected";
                resetFileTransfer();
            } else {
                handleSignalingMessage(message);
            }
        } catch (error) {
            console.error("Error processing message:", error);
        }
    };
}

// WebRTC event handlers
peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
        sendWSMessage({ 
            type: 'candidate',
            candidate: event.candidate,
            sessionKey 
        });
    }
};

peerConnection.onconnectionstatechange = () => {
    console.log("Connection state:", peerConnection.connectionState);
    switch (peerConnection.connectionState) {
        case "connected":
            status.textContent = "Peer connection established";
            break;
        case "disconnected":
        case "failed":
            status.textContent = "Peer connection lost";
            resetFileTransfer();
            break;
    }
};

peerConnection.ondatachannel = (event) => {
    console.log("Data channel received");
    setupDataChannel(event.channel);
};

// File input and send button handlers
fileInput.onchange = () => {
    const file = fileInput.files[0];
    if (file) {
        status.textContent = `Selected file: ${file.name} (${formatFileSize(file.size)})`;
    }
};

sendFileButton.onclick = async () => {
    const file = fileInput.files[0];
    if (!file) {
        status.textContent = "Please select a file first";
        return;
    }
    
    try {
        await initializeFileTransfer(file);
    } catch (error) {
        console.error("Error initializing file transfer:", error);
        status.textContent = "Failed to initialize file transfer";
    }
};

// Copy session key functionality
copySessionKeyButton.onclick = () => {
    const sessionKeyElement = document.getElementById('sessionKey');
    navigator.clipboard.writeText(sessionKeyElement.textContent)
        .then(() => {
            const originalText = copySessionKeyButton.textContent;
            copySessionKeyButton.textContent = 'Copied!';
            setTimeout(() => {
                copySessionKeyButton.textContent = originalText;
            }, 2000);
        })
        .catch(err => {
            console.error('Failed to copy:', err);
        });
};

// File transfer functions
async function initializeFileTransfer(file) {
    if (!sessionKey) {
        status.textContent = "No active session";
        return;
    }

    file.startTime = Date.now();
    fileQueue.push(file);
    
    if (!currentFile) {
        if (!peerConnection || peerConnection.connectionState !== "connected") {
            dataChannel = peerConnection.createDataChannel("fileChannel");
            setupDataChannel(dataChannel);
            
            const offer = await peerConnection.createOffer();
            await peerConnection.setLocalDescription(offer);
            sendWSMessage({ 
                type: 'offer',
                offer,
                sessionKey 
            });
        } else {
            processNextFile();
        }
    }
}

function processNextFile() {
    if (fileQueue.length === 0 || currentFile) return;
    
    currentFile = fileQueue.shift();
    fileName = currentFile.name;
    fileSize = currentFile.size;
    receivedSize = 0;
    transferStartTime = Date.now();
    lastUpdateTime = transferStartTime;
    bytesPerSecond = 0;
    
    if (dataChannel && dataChannel.readyState === "open") {
        sendFileMetadata();
        sendFileChunks();
    } else {
        status.textContent = "Waiting for connection...";
    }
}

function sendFileMetadata() {
    const metadata = {
        type: 'metadata',
        fileName: currentFile.name,
        fileSize: currentFile.size,
        fileType: currentFile.type,
        sessionKey: sessionKey
    };
    dataChannel.send(JSON.stringify(metadata));
    createProgressBar(); // Add this line
}

async function sendFileChunks() {
    if (!currentFile || !dataChannel) return;
    
    let offset = 0;
    transferStartTime = Date.now();
    lastUpdateTime = transferStartTime;

    const sendChunk = async () => {
        if (!currentFile) return;

        if (dataChannel.bufferedAmount > BUFFER_THRESHOLD) {
            await new Promise(resolve => setTimeout(resolve, 100));
            sendChunk();
            return;
        }

        const slice = currentFile.slice(offset, offset + CHUNK_SIZE);
        if (slice.size === 0) {
            finalizeTransfer('sent');
            return;
        }

        try {
            const arrayBuffer = await slice.arrayBuffer();
            dataChannel.send(arrayBuffer);
            offset += slice.size;
            updateProgress(offset, currentFile.size, 'Sending');
            
            const now = Date.now();
            if (now - lastUpdateTime >= 1000) {
                bytesPerSecond = (offset / (now - transferStartTime)) * 1000;
                lastUpdateTime = now;
            }

            requestAnimationFrame(() => sendChunk());
        } catch (error) {
            console.error("Error sending chunk:", error);
            status.textContent = "Error sending file chunk";
            resetFileTransfer();
        }
    };

    sendChunk();
}

function receiveFileChunk(data) {
    if (typeof data === 'string') {
        try {
            const metadata = JSON.parse(data);
            if (metadata.type === 'metadata') {
                fileName = metadata.fileName;
                fileSize = metadata.fileSize;
                receivedSize = 0;
                fileChunks = [];
                status.textContent = `Receiving ${fileName} (${formatFileSize(fileSize)})`;
            }
        } catch (error) {
            console.error("Error processing metadata:", error);
        }
        return;
    }
    
    fileChunks.push(new Uint8Array(data));
    receivedSize += data.byteLength;
    updateProgress(receivedSize, fileSize, 'Receiving');
    
    if (receivedSize === fileSize) {
        saveReceivedFile();
    }
}

function saveReceivedFile() {
    const blob = new Blob(fileChunks, { type: 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName || 'received_file';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    finalizeTransfer('received');
}

function setupDataChannel(channel) {
    channel.binaryType = "arraybuffer";
    
    channel.onopen = () => {
        console.log("Data channel opened");
        status.textContent = "Ready to transfer files";
        processNextFile();
    };
    
    channel.onclose = () => {
        console.log("Data channel closed");
        status.textContent = "Connection closed";
        resetFileTransfer();
    };
    
    channel.onerror = (error) => {
        console.error("Data channel error:", error);
        status.textContent = "Transfer error occurred";
        resetFileTransfer();
    };

    // Add this missing handler
    channel.onmessage = (event) => {
        receiveFileChunk(event.data);
    };

    try {
        channel.priority = 'high';
    } catch (e) {
        console.warn('Priority setting not supported');
    }

    dataChannel = channel;
}

async function handleSignalingMessage(message) {
    try {
        if (message.offer) {
            await peerConnection.setRemoteDescription(new RTCSessionDescription(message.offer));
            const answer = await peerConnection.createAnswer();
            await peerConnection.setLocalDescription(answer);
            sendWSMessage({ 
                type: 'answer',
                answer,
                sessionKey 
            });
        } else if (message.answer) {
            await peerConnection.setRemoteDescription(new RTCSessionDescription(message.answer));
        } else if (message.candidate) {
            await peerConnection.addIceCandidate(new RTCIceCandidate(message.candidate));
        }
    } catch (error) {
        console.error("Signaling error:", error);
        status.textContent = "Connection error occurred";
    }
}

function updateProgress(current, total, action) {
    const percentage = Math.round((current / total) * 100);
    
    if (progressBar) {
        progressBar.value = percentage;
    }

    const speed = bytesPerSecond || (current / (Date.now() - transferStartTime)) * 1000;
    const timeElapsed = (Date.now() - transferStartTime) / 1000;
    const timeRemaining = (total - current) / speed;

    if (progressText) {
        progressText.textContent = `${action} ${formatFileSize(current)} of ${formatFileSize(total)} ` +
            `(${percentage}%) • ${formatFileSize(speed)}/s • ` +
            `${formatTime(timeElapsed)} elapsed • ${formatTime(timeRemaining)} remaining`;
    }
}

function formatTime(seconds) {
    if (!isFinite(seconds)) return 'calculating...';
    
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    seconds = Math.floor(seconds % 60);

    const parts = [];
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);

    return parts.join(' ');
}

function finalizeTransfer(type) {
    status.textContent = `File ${type} successfully!`;
    if (progressBar) progressBar.value = 0;
    if (progressText) progressText.textContent = '';
    resetFileTransfer();
    processNextFile();
}

function resetFileTransfer() {
    if (currentFile) {
        currentFile = null;
        bytesPerSecond = 0;
        transferStartTime = 0;
        lastUpdateTime = 0;
    }
    fileChunks = [];
    receivedSize = 0;
    fileSize = 0;
    fileName = '';
}

function enableFileInput(enabled) {
    fileInput.disabled = !enabled;
    sendFileButton.disabled = !enabled;
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Add this function
function createProgressBar() {
    const container = document.createElement('div');
    container.className = 'progress-container';
    container.innerHTML = `
        <div class="progress-label"></div>
        <div class="progress-bar">
            <div class="progress-fill"></div>
        </div>
        <div class="progress-text"></div>
    `;
    chatWindow.appendChild(container);
    return container;
}

// Initialize WebSocket
connectWebSocket();

// Cleanup on page unload
window.onbeforeunload = () => {
    if (ws.readyState === WebSocket.OPEN) {
        ws.close();
    }
    if (peerConnection) {
        peerConnection.close();
    }
    if (dataChannel) {
        dataChannel.close();
    }
    resetFileTransfer();
};

