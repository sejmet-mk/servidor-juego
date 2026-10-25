const io = require("socket.io")(process.env.PORT || 3000, {
    cors: { origin: "*" }
});

console.log("¡Servidor listo y escuchando!");

io.on("connection", (socket) => {
    console.log("¡Un nuevo jugador ha entrado! ID:", socket.id);

    socket.on("disconnect", () => {
        console.log("Un jugador ha salido.");
    });
});