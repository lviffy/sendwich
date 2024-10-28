// Constants
const CHUNK_SIZE = 256 * 1024; // 256KB chunks for better performance
const BUFFER_THRESHOLD = CHUNK_SIZE * 8; // Buffer threshold before throttling
const MAX_RECONNECT_ATTEMPTS = 5;
const MAX_CHANNEL_RETRIES = 3;

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
let dataChannel = null;
let reconnectAttempts = 0;
let sessionKey;
let currentFileTransfer = null;
let transferStartTime = 0;
let lastUpdateTime = 0;
let bytesPerSecond = 0;
const messageQueue = [];
let isProcessingQueue = false;

// DOM Elements
const sessionKeyInput = document.getElementById("sessionKeyInput");
const joinSessionButton = document.getElementById("joinSession");
const chatInput = document.getElementById("chatInput");
const sendMessageButton = document.getElementById("sendMessage");
const chatWindow = document.getElementById("chatWindow");
const status = document.getElementById("status");

// Initial state
chatInput.disabled = true;
sendMessageButton.disabled = true;

// WebSocket Connection
function connectWebSocket() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsPath = window.location.hostname === 'localhost' ? '' : '/api/ws';
    ws = new WebSocket(`${wsProtocol}//${window.location.host}${wsPath}`);
    setupWebSocketHandlers();
}

// WebRTC Event Handlers
peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
        sendWSMessage(JSON.stringify({
            type: 'candidate',
            candidate: event.candidate,
            sessionKey
        }));
    }
};

peerConnection.ondatachannel = (event) => {
    console.log("Data channel received");
    setupDataChannel(event.channel);
};

function setupDataChannel(channel) {
    channel.binaryType = "arraybuffer";
    
    channel.onopen = () => {
        console.log("Data channel opened");
        displaySystemMessage("Ready to receive files");
    };
    
    channel.onclose = () => {
        console.log("Data channel closed");
        displaySystemMessage("File transfer connection closed");
    };
    
    channel.onerror = (error) => {
        console.error("Data channel error:", error);
        displaySystemMessage("Transfer error occurred");
    };
    
    channel.onmessage = (event) => {
        if (typeof event.data === 'string') {
            try {
                const metadata = JSON.parse(event.data);
                handleFileMetadata(metadata);
            } catch (error) {
                console.error("Error processing metadata:", error);
            }
        } else {
            handleFileData(event.data);
        }
    };

    dataChannel = channel;
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
    messageQueue.push(message);
    processMessageQueue();
}

// WebSocket Handlers
function setupWebSocketHandlers() {
    ws.onopen = () => {
        console.log('WebSocket connected');
        reconnectAttempts = 0;
        displaySystemMessage("Connected to server!");
        updateConnectionStatus('connected');
        
        if (sessionKey) {
            sendWSMessage(JSON.stringify({ type: "join-session", sessionKey }));
        }
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected');
        updateConnectionStatus('disconnected');
        chatInput.disabled = true;
        sendMessageButton.disabled = true;
        
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            setTimeout(() => {
                reconnectAttempts++;
                displaySystemMessage(`Reconnecting... Attempt ${reconnectAttempts}`);
                connectWebSocket();
            }, 3000 * reconnectAttempts);
        } else {
            displaySystemMessage("Cannot connect to server. Please refresh the page.");
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        displaySystemMessage("Connection error occurred.");
        updateConnectionStatus('error');
        if (ws.readyState !== WebSocket.CLOSED) {
            ws.close();
        }
    };

    ws.onmessage = handleWebSocketMessage;
}

// Session Management
joinSessionButton.onclick = () => {
    sessionKey = sessionKeyInput.value.trim();
    if (sessionKey) {
        if (ws.readyState === WebSocket.OPEN) {
            sendWSMessage(JSON.stringify({ type: "join-session", sessionKey }));
            sessionKeyInput.disabled = true;
            joinSessionButton.disabled = true;
        } else {
            displaySystemMessage("Connection not ready. Please wait...");
        }
    } else {
        displaySystemMessage("Please enter a valid session key.");
    }
};

sessionKeyInput.onkeypress = (e) => {
    if (e.key === "Enter") {
        e.preventDefault();
        joinSessionButton.click();
    }
};

// WebSocket Message Handler
async function handleWebSocketMessage(event) {
    try {
        const message = JSON.parse(event.data);
        
        switch (message.type) {
            case "session-joined":
                handleSessionJoined();
                break;
            case "offer":
                await handleOffer(message);
                break;
            case "candidate":
                await handleCandidate(message);
                break;
            case "chat-message":
                handleChatMessage(message);
                break;
            case "peer-disconnected":
                handlePeerDisconnected();
                break;
            case "error":
                handleError(message);
                break;
            default:
                console.warn('Unknown message type:', message.type);
        }
    } catch (error) {
        console.error('Error processing message:', error);
        displaySystemMessage("Error processing message from server");
    }
}

// WebRTC Message Handlers
async function handleOffer(message) {
    try {
        await peerConnection.setRemoteDescription(new RTCSessionDescription(message.offer));
        const answer = await peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answer);
        sendWSMessage(JSON.stringify({
            type: 'answer',
            answer,
            sessionKey
        }));
    } catch (error) {
        console.error('Error handling offer:', error);
        displaySystemMessage("Error establishing connection");
    }
}

async function handleCandidate(message) {
    try {
        await peerConnection.addIceCandidate(new RTCIceCandidate(message.candidate));
    } catch (error) {
        console.error('Error adding ICE candidate:', error);
    }
}

// File Transfer Handlers
function handleFileMetadata(metadata) {
    if (metadata.type === 'metadata') {
        displaySystemMessage(`Incoming file: ${metadata.fileName} (${formatFileSize(metadata.fileSize)})`);
        currentFileTransfer = {
            fileName: metadata.fileName,
            fileSize: metadata.fileSize,
            receivedSize: 0,
            chunks: [],
            startTime: Date.now()
        };
        transferStartTime = Date.now();
        lastUpdateTime = transferStartTime;
        bytesPerSecond = 0;
        createProgressBar(metadata.fileName);
    }
}

function handleFileData(data) {
    if (!currentFileTransfer) {
        displaySystemMessage("Error: No file transfer in progress");
        return;
    }

    try {
        currentFileTransfer.chunks.push(new Uint8Array(data));
        currentFileTransfer.receivedSize += data.byteLength;
        
        const now = Date.now();
        if (now - lastUpdateTime >= 1000) {
            bytesPerSecond = (currentFileTransfer.receivedSize / (now - transferStartTime)) * 1000;
            lastUpdateTime = now;
        }

        updateProgressBar(
            currentFileTransfer.receivedSize,
            currentFileTransfer.fileSize
        );

        if (currentFileTransfer.receivedSize >= currentFileTransfer.fileSize) {
            completeFileTransfer();
        }
    } catch (error) {
        displaySystemMessage("Error processing file chunk: " + error.message);
        currentFileTransfer = null;
        removeProgressBar();
    }
}

// UI Functions
function createProgressBar(fileName) {
    const progressContainer = document.createElement("div");
    progressContainer.className = "progress-container";
    progressContainer.innerHTML = `
        <div class="progress-label">${escapeHtml(fileName)}</div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: 0%"></div>
        </div>
        <div class="progress-text">0%</div>
    `;
    chatWindow.appendChild(progressContainer);
    scrollToBottom();
}

function updateProgressBar(received, total) {
    const progressBar = document.querySelector(".progress-container:last-child");
    if (progressBar) {
        const percentage = Math.round((received / total) * 100);
        const speed = bytesPerSecond || (received / (Date.now() - transferStartTime)) * 1000;
        const timeElapsed = (Date.now() - transferStartTime) / 1000;
        const timeRemaining = (total - received) / speed;

        progressBar.querySelector(".progress-fill").style.width = `${percentage}%`;
        progressBar.querySelector(".progress-text").textContent = 
            `${percentage}% • ${formatFileSize(received)} of ${formatFileSize(total)} • ` +
            `${formatFileSize(speed)}/s • ` +
            `${formatTime(timeElapsed)} elapsed • ${formatTime(timeRemaining)} remaining`;
    }
}

function removeProgressBar() {
    const progressBar = document.querySelector(".progress-container:last-child");
    if (progressBar) {
        progressBar.remove();
    }
}

function completeFileTransfer() {
    if (!currentFileTransfer) return;

    const blob = new Blob(currentFileTransfer.chunks);
    const url = URL.createObjectURL(blob);
    
    const downloadContainer = document.createElement("div");
    downloadContainer.className = "download-container";
    
    const downloadLink = document.createElement("a");
    downloadLink.href = url;
    downloadLink.download = currentFileTransfer.fileName;
    downloadLink.className = "download-link";
    downloadLink.innerHTML = `
        <span class="download-icon">⬇️</span>
        Download ${escapeHtml(currentFileTransfer.fileName)}
        (${formatFileSize(currentFileTransfer.fileSize)})
    `;
    
    downloadContainer.appendChild(downloadLink);
    chatWindow.appendChild(downloadContainer);
    scrollToBottom();

    downloadLink.onclick = () => {
        setTimeout(() => URL.revokeObjectURL(url), 100);
    };

    const transferTime = (Date.now() - currentFileTransfer.startTime) / 1000;
    const speed = currentFileTransfer.fileSize / transferTime;
    displaySystemMessage(
        `File transfer completed!\n` +
        `Time: ${formatTime(transferTime)}\n` +
        `Average Speed: ${formatFileSize(speed)}/s`
    );

    currentFileTransfer = null;
    removeProgressBar();
}

// Chat Functions
function handleSessionJoined() {
    displaySystemMessage("Connected to sender!");
    enableChatInterface();
}

function handleChatMessage(data) {
    displayMessage("Sender", data.message);
}

function handlePeerDisconnected() {
    displaySystemMessage("Sender disconnected");
    chatInput.disabled = true;
    sendMessageButton.disabled = true;
    sessionKeyInput.disabled = false;
    joinSessionButton.disabled = false;
    sessionKey = null;
}

function handleError(data) {
    displaySystemMessage(`Error: ${data.message}`);
}

// Utility Functions
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

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// Display message function for chat
function displayMessage(sender, message) {
    const messageElement = document.createElement("div");
    messageElement.className = "message";
    messageElement.innerHTML = `<strong>${escapeHtml(sender)}:</strong> ${escapeHtml(message)}`;
    chatWindow.appendChild(messageElement);
    scrollToBottom();
}

// Display system message function
function displaySystemMessage(message) {
    const systemMessage = document.createElement("div");
    systemMessage.className = "system-message";
    systemMessage.textContent = message;
    chatWindow.appendChild(systemMessage);
    scrollToBottom();
}

// Enable chat interface
function enableChatInterface() {
    chatInput.disabled = false;
    sendMessageButton.disabled = false;
}

// Scroll chat to bottom
function scrollToBottom() {
    chatWindow.scrollTop = chatWindow.scrollHeight;
}

// Update connection status
function updateConnectionStatus(status) {
    const statusElement = document.getElementById('status');
    if (statusElement) {
        const statusText = {
            'connected': 'Connected',
            'disconnected': 'Disconnected',
            'error': 'Connection Error'
        }[status] || status;
        
        statusElement.className = `status-${status}`;
        statusElement.textContent = statusText;
    }
}

// Add chat message sending functionality
sendMessageButton.onclick = () => {
    const message = chatInput.value.trim();
    if (message && sessionKey) {
        sendWSMessage(JSON.stringify({
            type: "chat-message",
            message,
            sessionKey
        }));
        displayMessage("You", message);
        chatInput.value = "";
    }
};

chatInput.onkeypress = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessageButton.click();
    }
};

// Initialize
connectWebSocket();

// Cleanup
window.onbeforeunload = () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close();
    }
    if (peerConnection) {
        peerConnection.close();
    }
    if (dataChannel) {
        dataChannel.close();
    }
    if (currentFileTransfer) {
        currentFileTransfer.chunks = [];
        currentFileTransfer = null;
    }
};
