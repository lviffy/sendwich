// WebSocket setup
const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsPath = window.location.hostname === 'localhost' ? '' : '/api/ws';
const ws = new WebSocket(`${wsProtocol}//${window.location.host}${wsPath}`);

// Constants
const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB limit
const CHUNK_SIZE = 1024 * 1024; // 1MB chunks
const MAX_RECONNECT_ATTEMPTS = 5;

// DOM Elements
const createSessionButton = document.getElementById("createSession");
const sessionInfo = document.getElementById("sessionInfo");
const sessionKeyDisplay = document.getElementById("sessionKey");
const copySessionKeyButton = document.getElementById("copySessionKey");
const fileInput = document.getElementById("fileInput");
const sendFileButton = document.getElementById("sendFile");
const status = document.getElementById("status");
const chatInput = document.getElementById("chatInput");
const sendMessageButton = document.getElementById("sendMessage");
const chatWindow = document.getElementById("chatWindow");

// State variables
let sessionKey = null;
let currentFileTransfer = null;
let wsReconnectAttempts = 0;
const messageQueue = [];
let isProcessingQueue = false;

// WebSocket setup and reconnection
function setupWebSocket() {
    ws.onopen = () => {
        console.log("Connected to server");
        status.textContent = "Connected";
        wsReconnectAttempts = 0;
        processMessageQueue(); // Process any pending messages
    };

    ws.onclose = () => {
        console.log("Disconnected from server");
        status.textContent = "Disconnected";
        
        if (wsReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            wsReconnectAttempts++;
            setTimeout(() => {
                displaySystemMessage(`Attempting to reconnect... (${wsReconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
                setupWebSocket();
            }, 3000);
        } else {
            displaySystemMessage("Connection lost. Please refresh the page.");
        }
    };

    ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        status.textContent = "Connection error";
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            switch(data.type) {
                case "session-created":
                    handleSessionCreated(data);
                    break;
                case "receiver-joined":
                    displaySystemMessage("Receiver connected!");
                    break;
                case "chat-message":
                    displayMessage("Receiver", data.message);
                    break;
                case "chunk-received":
                    handleChunkReceived(data);
                    break;
                case "file-received":
                    handleFileReceived(data);
                    break;
                case "error":
                    handleError(data);
                    break;
            }
        } catch (error) {
            console.error("Error processing message:", error);
        }
    };
}

// Message queue processing
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

// Session handlers
createSessionButton.onclick = () => {
    sendWSMessage(JSON.stringify({ type: "create-session" }));
    createSessionButton.disabled = true;
};

function handleSessionCreated(data) {
    sessionKey = data.sessionKey;
    sessionKeyDisplay.textContent = `Session Key: ${sessionKey}`;
    sessionInfo.style.display = "block";
    createSessionButton.style.display = "none";
    displaySystemMessage("Session created! Share the session key with the receiver.");
}

// Copy session key functionality
copySessionKeyButton.onclick = async () => {
    try {
        await navigator.clipboard.writeText(sessionKey);
        displaySystemMessage("Session key copied to clipboard!");
    } catch (err) {
        displaySystemMessage("Failed to copy session key. Please copy it manually.");
    }
};

// Chat message handling
sendMessageButton.onclick = sendMessage;
chatInput.onkeypress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
};

function sendMessage() {
    const message = chatInput.value.trim();
    if (!message || !sessionKey) return;

    sendWSMessage(JSON.stringify({
        type: "chat-message",
        message,
        sessionKey
    }));

    displayMessage("You", message);
    chatInput.value = "";
}

// File transfer handlers
sendFileButton.onclick = async () => {
    if (!fileInput.files.length || !sessionKey) {
        displaySystemMessage("Please select a file first.");
        return;
    }

    const file = fileInput.files[0];
    
    if (file.size > MAX_FILE_SIZE) {
        displaySystemMessage(`File too large. Maximum size is ${formatFileSize(MAX_FILE_SIZE)}`);
        return;
    }
    
    try {
        sendFileButton.disabled = true;
        displaySystemMessage(`Preparing to send ${file.name} (${formatFileSize(file.size)})...`);

        currentFileTransfer = {
            file: file,
            name: file.name,
            size: file.size,
            sentChunks: 0,
            totalChunks: Math.ceil(file.size / CHUNK_SIZE),
            startTime: Date.now()
        };

        sendWSMessage(JSON.stringify({
            type: "file-offer",
            sessionKey,
            fileName: file.name,
            fileSize: file.size,
            totalChunks: currentFileTransfer.totalChunks
        }));

        await sendNextChunk();
    } catch (error) {
        console.error("Error sending file:", error);
        displaySystemMessage("Error sending file. Please try again.");
        resetFileTransfer();
    }
};

async function sendNextChunk() {
    if (!currentFileTransfer) return;

    const { file, sentChunks, totalChunks } = currentFileTransfer;
    const start = sentChunks * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, file.size);
    const chunk = file.slice(start, end);

    try {
        const buffer = await chunk.arrayBuffer();
        const chunkData = JSON.stringify({
            type: "file-chunk",
            sessionKey,
            fileName: currentFileTransfer.name,
            chunkIndex: sentChunks,
            totalChunks,
            data: Array.from(new Uint8Array(buffer))
        });
        
        sendWSMessage(chunkData);
        
        if (!currentFileTransfer.timeoutId) {
            currentFileTransfer.timeoutId = setTimeout(() => {
                displaySystemMessage("File transfer timed out. Please try again.");
                resetFileTransfer();
            }, 30000);
        }
    } catch (error) {
        console.error("Error sending chunk:", error);
        displaySystemMessage("Error during file transfer. Please try again.");
        resetFileTransfer();
    }
}

function handleChunkReceived(data) {
    if (!currentFileTransfer) return;
    
    currentFileTransfer.sentChunks++;
    
    if (currentFileTransfer.sentChunks < currentFileTransfer.totalChunks) {
        sendNextChunk();
    }
}

function handleFileReceived(data) {
    if (!currentFileTransfer) return;
    
    const transferTime = (Date.now() - currentFileTransfer.startTime) / 1000;
    const speed = currentFileTransfer.size / transferTime;
    
    displaySystemMessage(
        `File "${data.fileName}" sent successfully!\n` +
        `Time: ${transferTime.toFixed(1)}s\n` +
        `Speed: ${formatFileSize(speed)}/s`
    );
    
    resetFileTransfer();
}

function resetFileTransfer() {
    if (currentFileTransfer?.timeoutId) {
        clearTimeout(currentFileTransfer.timeoutId);
    }
    currentFileTransfer = null;
    sendFileButton.disabled = false;
    fileInput.value = null;
}

// Utility functions
function formatFileSize(bytes) {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unitIndex = 0;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
    }
    
    return `${size.toFixed(1)} ${units[unitIndex]}`;
}

function displayMessage(sender, message) {
    const messageElement = document.createElement("div");
    messageElement.className = "message";
    messageElement.textContent = `${sender}: ${message}`;
    chatWindow.appendChild(messageElement);
    chatWindow.scrollTop = chatWindow.scrollHeight;
}

function displaySystemMessage(message) {
    const messageElement = document.createElement("div");
    messageElement.className = "system-message";
    messageElement.textContent = message;
    chatWindow.appendChild(messageElement);
    chatWindow.scrollTop = chatWindow.scrollHeight;
}

function handleError(data) {
    displaySystemMessage(`Error: ${data.message}`);
    if (currentFileTransfer) {
        resetFileTransfer();
    }
}

// Initialize WebSocket
setupWebSocket();

// Cleanup
window.onbeforeunload = () => {
    if (sessionKey) {
        ws.close();
    }
};