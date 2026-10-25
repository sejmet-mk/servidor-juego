// ORBIT 3033 · servidor de salas y reenvío (Socket.io)
// - "quick": emparejamiento automático, sin códigos (entra en una sala con hueco o crea una)
// - "host"/"join": salas con código (amigos)
// - "m": reenvía mensajes anfitrión <-> invitados (el anfitrión manda en la partida)
const http = require("http");
const { Server } = require("socket.io");

const MAX_PLAYERS = 6;               // anfitrión + 5 invitados
const rooms = new Map();             // code -> { code, host, ready, pub, members:Set, waiting:[], t }
const roomOf = new Map();            // socket.id -> code

const httpServer = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ORBIT 3033 server OK · salas: " + rooms.size);
    return;
  }
  res.writeHead(404); res.end();
});

const io = new Server(httpServer, {
  cors: { origin: "*" },
  pingInterval: 10000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e6,
  perMessageDeflate: false           // sin compresión: menos CPU y menos retraso
});

// canal "sin garantia": si el destino va atrasado (cola de envio llena) se descarta en vez de acumular retraso
function congested(s, chan) {
  if (chan !== "u") return false;
  try { const w = s.conn.transport.ws; return !!(w && w.bufferedAmount > 20000); } catch (e) { return false; }
}

const CODE_CH = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
function newCode() {
  for (let k = 0; k < 50; k++) {
    let s = ""; for (let i = 0; i < 5; i++) s += CODE_CH[Math.floor(Math.random() * CODE_CH.length)];
    if (!rooms.has(s)) return s;
  }
  return null;
}
const clean = c => String(c || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 8);

function closeRoom(room, why) {
  for (const id of room.members) {
    const s = io.sockets.sockets.get(id);
    if (s) s.emit("hostclosed", why || "");
    roomOf.delete(id);
  }
  for (const w of room.waiting) { try { w.cb({ ok: false, err: "NO_ROOM" }); } catch (e) {} }
  roomOf.delete(room.host);
  rooms.delete(room.code);
}

function leave(socket) {
  const code = roomOf.get(socket.id);
  if (!code) return;
  const room = rooms.get(code);
  roomOf.delete(socket.id);
  if (!room) return;
  if (room.host === socket.id) { closeRoom(room, "host"); return; }
  room.members.delete(socket.id);
  room.waiting = room.waiting.filter(w => w.id !== socket.id);
  const h = io.sockets.sockets.get(room.host);
  if (h) h.emit("peerclose", socket.id);
}

function admit(room, socket, cb) {
  room.members.add(socket.id);
  roomOf.set(socket.id, room.code);
  const h = io.sockets.sockets.get(room.host);
  if (h) h.emit("peer", socket.id);
  cb({ ok: true });
}

io.on("connection", (socket) => {
  // emparejamiento rápido: devuelve { code, host:true|false }
  socket.on("quick", (_d, cb) => {
    if (typeof cb !== "function") return;
    leave(socket);
    let best = null;
    for (const r of rooms.values()) {
      if (r.pub && 1 + r.members.size < MAX_PLAYERS && (!best || r.members.size > best.members.size)) best = r;
    }
    if (best) return cb({ ok: true, host: false, code: best.code });
    const code = newCode();
    if (!code) return cb({ ok: false, err: "FULL" });
    rooms.set(code, { code, host: socket.id, ready: false, pub: true, members: new Set(), waiting: [], t: Date.now() });
    roomOf.set(socket.id, code);
    cb({ ok: true, host: true, code });
  });

  // anfitrión confirma su sala (o crea una con código para amigos)
  socket.on("host", (d, cb) => {
    if (typeof cb !== "function") return;
    const code = clean(d && d.code);
    if (code.length < 4) return cb({ ok: false, err: "CODE" });
    let room = rooms.get(code);
    if (room && room.host !== socket.id) return cb({ ok: false, err: "CODE_TAKEN" });
    if (!room) {
      leave(socket);
      room = { code, host: socket.id, ready: false, pub: false, members: new Set(), waiting: [], t: Date.now() };
      rooms.set(code, room); roomOf.set(socket.id, code);
    }
    room.ready = true;
    cb({ ok: true });
    for (const w of room.waiting) {
      const s = io.sockets.sockets.get(w.id);
      if (s) admit(room, s, w.cb);
    }
    room.waiting = [];
  });

  socket.on("join", (d, cb) => {
    if (typeof cb !== "function") return;
    const room = rooms.get(clean(d && d.code));
    if (!room || room.host === socket.id) return cb({ ok: false, err: "NO_ROOM" });
    if (1 + room.members.size >= MAX_PLAYERS) return cb({ ok: false, err: "FULL" });
    leave(socket);
    if (room.ready) return admit(room, socket, cb);
    // el anfitrión aún está cargando: espera (máx. 15 s)
    const w = { id: socket.id, cb };
    room.waiting.push(w);
    setTimeout(() => {
      const i = room.waiting.indexOf(w);
      if (i >= 0) { room.waiting.splice(i, 1); try { cb({ ok: false, err: "TIMEOUT" }); } catch (e) {} }
    }, 15000);
  });

  // reenvío: invitado -> anfitrión ; anfitrión -> invitado concreto
  socket.on("m", (p) => {
    if (!p) return;
    if (process.env.DBG) { const k=(socket.id===(rooms.get(roomOf.get(socket.id))||{}).host?"H":"G")+(p.c)+":"+(p.d&&p.d.t); DBGC[k]=(DBGC[k]||0)+1; }
    const room = rooms.get(roomOf.get(socket.id));
    if (!room || !room.ready) return;
    if (socket.id === room.host) {
      if (!room.members.has(p.to)) return;
      const s = io.sockets.sockets.get(p.to);
      if (s && !congested(s, p.c)) s.emit("m", { c: p.c, d: p.d });
    } else {
      const h = io.sockets.sockets.get(room.host);
      if (h && !congested(h, p.c)) h.emit("m", { from: socket.id, c: p.c, d: p.d });
    }
  });

  socket.on("leave", () => leave(socket));
  socket.on("disconnect", () => leave(socket));
});

// limpieza de salas huérfanas
setInterval(() => {
  const now = Date.now();
  for (const r of rooms.values()) {
    if (!io.sockets.sockets.get(r.host) || (!r.ready && now - r.t > 60000)) closeRoom(r, "stale");
  }
}, 30000);

const DBGC={};if(process.env.DBG)setInterval(()=>console.log(JSON.stringify(DBGC)),4000);
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log("¡Servidor listo y escuchando en el puerto " + PORT + "!"));
