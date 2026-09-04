// 렉시오(LEXIO) 온라인 방 릴레이 서버
// -----------------------------------------------------------------------------
// 이 서버는 게임 규칙을 전혀 모릅니다. 그냥 같은 방(room)에 들어온 클라이언트끼리
// 메시지를 그대로 전달(relay)만 하는 역할입니다. 실제 카드 게임 로직은 방장(host)의
// 브라우저가 전부 계산하고, 그 결과를 이 서버를 통해 나머지 사람들에게 뿌립니다.
//
// 배포 방법 (Render):
//   1) 이 server.js와 게임 html 파일(lexio_online.html)을 같은 폴더(lexio-server)에 넣고
//      GitHub 저장소에 함께 올립니다. (같은 폴더에 있어야 아래에서 자동으로 찾아서 서빙합니다)
//   2) Render 대시보드 -> New -> Web Service -> 해당 저장소 선택
//   3) Build Command: npm install / Start Command: npm start (Render가 자동 인식)
//   4) 배포가 끝나면 https://<앱이름>.onrender.com 주소가 생기고,
//      그 주소로 바로 접속하면 게임 화면(lexio_online.html)이 뜹니다.
//      게임 안의 ONLINE_SERVER_URL 은 wss://<앱이름>.onrender.com 으로 맞춰주세요.
// -----------------------------------------------------------------------------

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

// 이 서버와 같은 폴더에 게임 html 파일을 함께 배포해두면, 주소로 바로 접속했을 때
// 아래 후보 이름들 중 있는 파일을 찾아 그대로 보여준다. (index.html을 최우선으로 찾는다)
// -> Render에 올릴 때 이 server.js와 게임 html 파일(예: index.html)을 같은 저장소/폴더에 넣어주세요.
const HTML_CANDIDATES = ["lexio_online.html", "index.html", "lexio-online.html", "lexio.html", "game.html"];
let gameHtml = null;
let gameHtmlName = null;
for (const name of HTML_CANDIDATES) {
  const p = path.join(__dirname, name);
  if (fs.existsSync(p)) {
    gameHtml = fs.readFileSync(p);
    gameHtmlName = name;
    break;
  }
}
if (gameHtml) {
  console.log(`게임 화면 파일을 찾았습니다: ${gameHtmlName} (루트 주소에서 바로 보여줍니다)`);
} else {
  console.warn(
    "게임 html 파일을 찾지 못했습니다. server.js와 같은 폴더에 index.html(또는 mighty-online2-5.html)을 함께 배포하면 " +
      "주소로 바로 접속했을 때 게임 화면이 뜹니다. 지금은 웹소켓 릴레이 기능만 동작합니다."
  );
}

// room code -> { players: [{id, ws, name, host}], nextId }
const rooms = new Map();

function makeRoomCode() {
  // 숫자로만 구성된 6자리 방 코드 (예: 482913)
  let code;
  do {
    code = String(Math.floor(100000 + Math.random() * 900000));
  } while (rooms.has(code));
  return code;
}

// 플레이어별 재접속 토큰 (다른 화면에 잠깐 나갔다 와도 같은 자리로 돌아올 수 있게 해줌)
function makeToken() {
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
}

// 연결이 끊긴 뒤 완전히 방을 나간 것으로 처리하기까지 기다려주는 유예 시간
// (예전에는 45초 동안 AI가 대신 진행하다가 자리를 대체했지만, 지금은 짧게 10초만
//  기다려주고 그 안에 돌아오지 않으면 AI로 계속 진행하는 대신 게임 자체를 종료한다.)
const RECONNECT_GRACE_MS = 10000;

function publicPlayers(room) {
  return room.players.map((p) => ({ id: p.id, name: p.name, host: p.host, connected: p.connected !== false }));
}

function broadcastLobby(room) {
  const payload = JSON.stringify({ type: "lobby", players: publicPlayers(room) });
  for (const p of room.players) {
    if (p.ws.readyState === p.ws.OPEN) p.ws.send(payload);
  }
}

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  // 웹소켓 업그레이드가 아닌 일반 GET 요청이면 게임 화면(html)을 그대로 보여준다.
  if (gameHtml && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(gameHtml);
    return;
  }
  // 게임 html을 못 찾았을 때(또는 헬스체크)는 기존처럼 안내 텍스트만 응답한다.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("lexio online relay server is running");
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws) => {
  let joined = null; // {roomCode, playerId}

  ws.on("message", (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    if (m.type === "join") {
      const nickname = String(m.nickname || "플레이어").slice(0, 12) || "플레이어";
      // 방 코드는 숫자만 허용한다 (참여자가 잘못된 문자를 섞어 입력해도 무시)
      let roomCode = String(m.room || "").replace(/\D/g, "").trim();
      const token = typeof m.token === "string" && m.token ? m.token : null;

      // 재접속: 토큰을 가지고 있고, 그 방에 같은 토큰을 쓰던(연결이 끊겼던) 자리가 남아있으면
      // 새 자리를 만들지 않고 원래 자리를 그대로 이어받는다.
      if (roomCode && token && rooms.has(roomCode)) {
        const existingRoom = rooms.get(roomCode);
        const existingPlayer = existingRoom.players.find((p) => p.token === token);
        if (existingPlayer) {
          if (existingPlayer.leaveTimer) {
            clearTimeout(existingPlayer.leaveTimer);
            existingPlayer.leaveTimer = null;
          }
          existingPlayer.ws = ws;
          existingPlayer.connected = true;
          if (nickname) existingPlayer.name = nickname;
          joined = { roomCode, playerId: existingPlayer.id };

          send(ws, {
            type: "welcome",
            playerId: existingPlayer.id,
            host: existingPlayer.host,
            room: roomCode,
            token: existingPlayer.token,
            players: publicPlayers(existingRoom),
            resumed: true,
          });
          broadcastLobby(existingRoom);
          for (const p of existingRoom.players) {
            if (p.id !== existingPlayer.id) send(p.ws, { type: "peer_back", name: existingPlayer.name });
          }
          return;
        }
        // 토큰과 일치하는 자리가 없다면(유예 시간이 지나 이미 완전히 나간 상태) 아래의 일반 입장 로직으로 진행한다.
      }

      let room;
      if (roomCode) {
        // 참여자가 방 코드를 입력한 경우: 반드시 이미 존재하는 방이어야만 입장할 수 있다.
        // (예전에는 존재하지 않는 코드를 입력하면 그 코드로 새 방을 만들어버렸는데,
        //  그러면 오타를 낸 참여자가 엉뚱한 유령 방의 "방장"이 되어버리고, 자신은 게임에
        //  못 들어갔다고 착각하거나 다른 방과 뒤섞일 위험이 있었다. 존재하지 않는 코드는
        //  그냥 에러로 돌려주고, 기존 방(원래 방장이 만든 방)에는 아무 영향도 주지 않는다.)
        if (!rooms.has(roomCode)) {
          send(ws, { type: "error", message: "존재하지 않는 방 코드입니다. 코드를 다시 확인해주세요." });
          return;
        }
        room = rooms.get(roomCode);
        const activeCount = room.players.filter((p) => p.connected !== false).length;
        if (activeCount >= 4) {
          send(ws, { type: "error", message: "방이 가득 찼습니다 (최대 4명)." });
          return;
        }
      } else {
        // 방 코드를 비워두면 새 방을 만든다 (이때만 방장이 된다)
        roomCode = makeRoomCode();
        room = { code: roomCode, players: [], nextId: 0 };
        rooms.set(roomCode, room);
      }

      const isHost = room.players.length === 0;
      const playerId = room.nextId++;
      const playerObj = {
        id: playerId,
        ws,
        name: nickname,
        host: isHost,
        token: token || makeToken(),
        connected: true,
        leaveTimer: null,
      };
      room.players.push(playerObj);
      joined = { roomCode, playerId };

      send(ws, {
        type: "welcome",
        playerId,
        host: isHost,
        room: roomCode,
        token: playerObj.token,
        players: publicPlayers(room),
      });
      broadcastLobby(room);
      return;
    }

    if (!joined) return; // join 이전에는 아무 것도 처리하지 않음
    const room = rooms.get(joined.roomCode);
    if (!room) return;

    if (m.type === "action") {
      // guest -> host 로만 전달 (host의 ws에게)
      const host = room.players.find((p) => p.host);
      if (host) send(host.ws, { type: "action", action: m.action, playerId: joined.playerId });
      return;
    }

    if (m.type === "state" || m.type === "timer") {
      // host -> 방의 모든 사람에게 그대로 전달 (host 자신 제외)
      const sender = room.players.find((p) => p.id === joined.playerId);
      if (!sender || !sender.host) return; // host만 상태를 뿌릴 수 있다
      for (const p of room.players) {
        if (p.id !== joined.playerId) send(p.ws, m);
      }
      return;
    }

    if (m.type === "chat") {
      // 채팅: 보낸 사람을 제외한 같은 방의 모두에게 전달한다.
      // 이름은 클라이언트가 보낸 값을 믿지 않고 서버가 들고 있는 실제 닉네임을 사용한다.
      const sender = room.players.find((p) => p.id === joined.playerId);
      if (!sender) return;
      const text = String(m.text || "").slice(0, 300).trim();
      if (!text) return;
      for (const p of room.players) {
        if (p.id !== joined.playerId) send(p.ws, { type: "chat", name: sender.name, text });
      }
      return;
    }
  });

  ws.on("close", () => {
    if (!joined) return;
    const room = rooms.get(joined.roomCode);
    if (!room) return;

    const player = room.players.find((p) => p.id === joined.playerId);
    if (!player) return;
    // 이미 같은 자리에 재접속(새 ws)이 붙어있다면, 지금 닫히는 건 옛날 연결이므로 무시한다.
    if (player.ws !== ws) return;

    player.connected = false;
    broadcastLobby(room);
    for (const p of room.players) {
      if (p.id !== player.id) send(p.ws, { type: "peer_away", name: player.name });
    }

    // 유예 시간(10초) 안에 재접속하지 않으면, 그 자리를 AI로 대체해서 계속 진행하는 대신
    // 게임 자체를 종료한다. 남아있는 모든 사람에게 종료 사실을 알리고 방을 정리한다.
    player.leaveTimer = setTimeout(() => {
      const idx = room.players.findIndex((p) => p.id === player.id);
      if (idx === -1) return;
      const leaving = room.players[idx];
      if (leaving.connected) return; // 그 사이에 재접속했다면 아무것도 하지 않는다

      for (const p of room.players) {
        if (p.leaveTimer) {
          clearTimeout(p.leaveTimer);
          p.leaveTimer = null;
        }
        if (p.id !== leaving.id) {
          send(p.ws, { type: "room_closed", reason: "disconnect_timeout", name: leaving.name });
        }
      }
      rooms.delete(joined.roomCode);
    }, RECONNECT_GRACE_MS);
  });
});

server.listen(PORT, () => {
  console.log(`lexio online relay server listening on :${PORT}`);
});
