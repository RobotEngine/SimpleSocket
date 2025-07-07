// SimpleSocket Client V2
// ©2025 Exotek

class SimpleSocket {
  constructor(init) {
    this.id = init.project_id;
    this.token = init.project_token;

    this.socketURL = init.socket_url ?? "wss://simplesocket.net/socket/v2"; // "ws://localhost:3000/socket/v2"
    this.supportsETF = init.useBinary || typeof(TextEncoder) != "undefined";
    this.showDebug = init.showDebug ?? false;
    this.debugStyle = init.debugStyle ?? true;

    this.operations = {};
    this.subscribes = {};
    this.missedMessages = {};
    this.receivedCount = 0;
    this.lastMissedCheck = Date.now();

    this.totalMessages = 0;
    this.timeout = 120000;

    this.remotes = {};
    this.callbacks = {};

    window.addEventListener("offline", () => {
      this.close("Lost connection.");
    });

    this.connectSocket();
  }

  debug = (message, data, force, error) => {
    if (data != null) {
      message += " " + JSON.stringify(data);
    }
    if (this.showDebug == true || force == true) {
      if (this.debugStyle == true) {
        if (error == true) {
          console.error("%cSimpleSocket%c " + message, `color: #FF3D77; font-family: sans-serif; font-weight: 900; font-size: 12px`, "color: white");
        } else {
          console.log("%cSimpleSocket%c " + message, `color: #FF3D77; font-family: sans-serif; font-weight: 900; font-size: 12px`, "color: white");
        }
      } else {
        if (error == true) {
          console.error(message);
        } else {
          console.log(message);
        }
      }
    }
  }

  send = (oper, data, callback, useID) => {
    let messID = useID;
    if (useID == null && oper != null) {
      this.totalMessages += 1;
      messID = parseInt(oper.toString() + this.totalMessages.toString());
    }
    let sendData = [];

    if (messID != null) {
      sendData.push(messID);
    }
    for (let i = 0; i < (data ?? []).length; i++) {
      sendData[i + 1] = data[i];
    }

    if (oper > 1) {
      let storeOp = [oper, data, callback];
      if (oper == 2) {
        let hash = this.hash(data[0]);
        storeOp[3] = hash;
        if (this.subscribes[hash] == null) {
          this.subscribes[hash] = {};
        }
        this.subscribes[hash][messID] = "";
      } else if (callback != null) {
        this.callbacks[messID] = [oper, Date.now(), callback];
      }
      this.operations[messID] = storeOp;
    }

    if (this.socket != null && this.socket.readyState == WebSocket.OPEN && (this.clientID != null || oper == 1)) {
      this.debug("SENT:", sendData);
      let sendStr = JSON.stringify(sendData);
      sendStr = sendStr.substring(1, sendStr.length - 1);
      
      if (this.supportsETF == true) {
        sendStr = new TextEncoder("utf-8").encode(sendStr);
      }
      
      this.socket.send(sendStr);
      
      if (callback == null && this.operations[messID] != null && oper < 7) {
        delete this.operations[messID];
      }
    } else if (this.socket != null && this.socket.readyState == WebSocket.CLOSED) {
      this.closed();
    }

    return messID;
  }

  processMessageData = (data, missed) => {
    if (data.length < 2 || this.lastMissedCheck + 30000 < Date.now()) { // PONG
      this.lastMissedCheck = Date.now();

      let callbackKeys = Object.keys(this.callbacks);
      let checkTime = Date.now() - 30000;
      for (let i = 0; i < callbackKeys.length; i++) {
        let key = callbackKeys[i];
        let check = this.callbacks[key];
        if (check[1] < checkTime) {
          this.debug("CLOSING CALLBACK: Reason: Timeout");
          delete this.operations[key];
          delete this.callbacks[key];
          if (check[0] == 6) {
            check[2]({ done: false, error: "Response timed out (over 30 seconds)." });
          }
        }
      }

      let sendRetry = [];
      let missingKeys = Object.keys(this.missedMessages);
      for (let i = 0; i < missingKeys.length; i++) {
        let count = missingKeys[i];
        let data = this.missedMessages[count];
        if (typeof data != "number") {
          continue;
        }
        if (data > 2) { // 3 Retries
          delete this.missedMessages[count];
          continue;
        }
        this.missedMessages[count]++;
        sendRetry.push(count);
      }
      this.handleMissedMessages([]);
      if (data.length < 2) {
        this.debug("PONG");
        for (let i = 1; i <= data[0] - this.receivedCount; i++) {
          let count = this.receivedCount + i;
          this.missedMessages[count] = 1;
          sendRetry.push(count);
        }
        this.receivedCount = data[0];
      }
      if (sendRetry.length > 0) {
        this.debug("RETRYING:", sendRetry);
        this.send(9, [1, sendRetry], this.handleMissedMessages);
      }
      if (data.length < 2) {
        return;
      }
    }

    let taskString = (data[0] ?? "").toString();
    let task = taskString[taskString.length - 1];
    let sendCount = parseInt(taskString.substring(0, taskString.length - 1) ?? "0");

    let newMessage = sendCount > this.receivedCount;
    if (newMessage == true && this.clientID != null) {
      let sendRetry = [];
      for (let i = 1; i < sendCount - this.receivedCount; i++) {
        let count = this.receivedCount + i;
        this.missedMessages[count] = 1;
        sendRetry.push(count);
      }
      if (sendRetry.length > 0) {
        this.debug("RETRYING:", sendRetry);
        this.send(9, [1, sendRetry], this.handleMissedMessages);
      }
      this.receivedCount = sendCount;
    }
    
    switch (task) {
      case "2":
        // SUBSCRIBE
        if (newMessage == true || missed == true) {
          let config = data[3] ?? {};
          if (config.ordered == true) {
            let missingIDs = Object.keys(this.missedMessages);
            for (let i = 0 ; i < missingIDs.length; i++) {
              if (missingIDs[i] < sendCount) { // Missing a previous message that must be processed first
                this.missedMessages[sendCount] = data;
                return;
              }
            }
          }
          if (data[4] == null) {
            let opKeys = Object.keys(this.subscribes[data[1]] ?? []);
            for (let i = 0; i < opKeys.length; i++) {
              let oper = this.operations[opKeys[i]];
              if (oper != null && oper[2] != null) { // oper[3] == data[2]
                oper[2](data[2], config);
              }
            }
          } else if (this.remotes[data[4]] != null) {
            this.remotes[data[4]](data[2], config);
          }
        }
        return;
      case "3":
        // RESPONSE
        if (this.operations[data[1]] != null) {
          this.operations[data[1]][2](data[2]);
          delete this.operations[data[1]];
          delete this.callbacks[data[1]];
        }
        return;
      case "1":
        // CONNECT
        this.debug("CONNECTED: ClientID: " + data[1]);
        this.clientID = data[1];
        this.serverID = data[2];
        this.secureID = data[1] + "-" + data[3];
        this.subscribes = {};
        this.missedMessages = {};
        this.receivedCount = 1;
        this.lastMissedCheck = Date.now();
        if (this.onopen != null) {
          this.onopen();
        }
        // Reconnect Previous Events
        let opKeys = Object.keys(this.operations);
        for (let i = 0; i < opKeys.length; i++) {
          let operation = {...this.operations[opKeys[i]]};
          delete this.operations[opKeys[i]];
          delete this.callbacks[opKeys[i]];
          this.send(operation[0], operation[1], operation[2], parseInt(opKeys[i]));
        }
        return;
      case "0":
        // ERROR
        this.debug(data[2], null, true, true);
        if (this.operations[data[1]] != null) {
          delete this.operations[data[1]];
          delete this.callbacks[data[1]];
        }
        if (data[3] == true) {
          this.expectClose = true;
        } else if (this.operations[data[3]] != null) {
          this.operations[data[3]][3] = this.hash(data[4]);
          this.operations[data[3]][1][0] = data[4];
        }
    }
  }
  handleMessage = (recData) => {
    clearTimeout(this.timeoutTimeout);
    this.timeoutTimeout = setTimeout(() => {
      this.close("Failed to receive PONG message.");
    }, this.timeout);

    if (typeof recData === "object") {
      recData = new TextDecoder("utf-8").decode(recData);
    }

    let messData = JSON.parse("[" + recData + "]");
    this.debug("RECIEVED:", messData);
    this.processMessageData(messData);
  }
  handleMissedMessages = (response) => {
    let processMessages = [];
    for (let i = 0; i < response.length; i++) {
      let data = response[i] ?? [];
      if (this.missedMessages[data[0]] != null) {
        delete this.missedMessages[data[0]];
        processMessages.push(data);
      }
    }
    let sortedMissingKeys = Object.keys(this.missedMessages).sort((a, b) => { return a - b; });
    for (let i = 0; i < sortedMissingKeys.length; i++) {
      let count = sortedMissingKeys[i];
      let data = this.missedMessages[count];
      if (typeof data == "number") {
        break;
      }
      processMessages.push([count, data]);
      delete this.missedMessages[count];
    }
    processMessages.sort((a, b) => { return a[0] - b[0]; });
    for (let i = 0; i < processMessages.length; i++) {
      let message = (processMessages[i] ?? [])[1];
      if (message != null) {
        this.processMessageData(message, true);
      }
    }
  }

  connectSocket = () => {
    let intervalConnect = () => {
      this.debug("CONNECTING");

      let ending = "";
      if (this.supportsETF == true) {
        ending = "?en=etf";
      }
      this.close("Closing old socket connection.");
      this.socket = new WebSocket(this.socketURL + ending); // + "&comp=t"
      if (this.supportsETF == true) {
        this.socket.binaryType = "arraybuffer";
      }
      this.socket.onopen = () => {
        this.socket.onmessage = (message) => {
          this.handleMessage(message.data);
          if (this.intervalTryConnect != null) {
            clearInterval(this.intervalTryConnect);
            this.intervalTryConnect = null;
          }
        }
        this.socket.onclose = () => {
          this.closed();
        }
        this.send(1, [this.id, this.token]);
      }
    }
    clearInterval(this.intervalTryConnect);
    this.intervalTryConnect = setInterval(intervalConnect, 10000);
    intervalConnect();
  }

  close = (reason) => {
    if (this.socket == null) {
      return;
    }
    this.socket.close(1000, reason);
    this.closed();
  }

  closed = () => {
    if (this.socket == null || this.clientID == null) {
      return;
    }
    this.socket = null;
    this.debug("CONNECTION LOST");
    this.clientID = null;
    this.serverID = null;
    this.secureID = null;
    if (this.onclose != null) {
      this.onclose();
    }
    if (this.expectClose != true) {
      this.connectSocket();
    }
  }

  hash = (text) => {
    if (typeof text === "object") {
      text = JSON.stringify(text);
    }
    let hash = 0;
    for (let i = 0; i < text.length; i++) {
      let char = text.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash;
  }

  setDefaultConfig = (newSet) => {
    this.debug("NEW CONFIG: Config:", newSet);
    if (this.defaultConfig != null && this.operations[this.defaultConfig]) {
      delete this.operations[this.defaultConfig];
    }
    this.defaultConfig = this.send(7, [newSet]);
  }

  setDisconnectEvent = (filter, data, config) => {
    let sendData = [filter, data];
    if (config != null) {
      sendData[2] = config;
    }
    this.debug("Setting Disconnect Event:", sendData);
    if (this.disconnectEvent != null && this.operations[this.disconnectEvent]) {
      delete this.operations[this.disconnectEvent];
      this.disconnectEvent = null;
    }
    if (filter != null) {
      this.disconnectEvent = this.send(8, sendData);
    } else {
      delete this.operations[this.send(8, [null])];
    }
  }

  subscribe = (filter, callback, config) => {
    this.debug("SUBSCRIBING: Filter:", filter);
    let sendData = [filter];
    if (config != null) {
      sendData[1] = config;
    }
    if (callback.length < 2) {
      if (config == null) {
        sendData[1] = true;
      } else {
        sendData[2] = true;
      }
    }
    let subID = this.send(2, sendData, callback);
    return {
      id: subID,
      edit: (newFilter) => {
        let oper = this.operations[subID];
        if (oper != null) {
          let newHash = this.hash(newFilter);
          if (oper[3] != newHash) {
            this.debug("EDITING: Filter:", newFilter);
            oper[1][0] = newFilter;
            this.send(4, [subID, oper[3], newFilter]);
            if (this.subscribes[oper[3]] != null) {
              delete this.subscribes[oper[3]][subID];
              if (Object.keys(this.subscribes[oper[3]]).length < 1) {
                delete this.subscribes[oper[3]];
              }
            }
            oper[3] = newHash;
            if (this.subscribes[newHash] == null) {
              this.subscribes[newHash] = {};
            }
            this.subscribes[newHash][subID] = "";
          }
        }
      },
      close: () => {
        let oper = this.operations[subID];
        if (oper != null) {
          this.debug("CLOSING: " + subID);
          this.send(5, [oper[3]]);
          delete this.operations[subID];
          if (this.subscribes[oper[3]] != null) {
            delete this.subscribes[oper[3]][subID];
            if (Object.keys(this.subscribes[oper[3]]).length < 1) {
              delete this.subscribes[oper[3]];
            }
          }
        }
      }
    }
  }

  publish = (filter, data, config) => {
    let sendData = [filter, data];
    if (config != null) {
      sendData[2] = config;
    }
    this.debug("PUBLISHING: Filter:", sendData);
    this.send(3, sendData);
  }
}
