import zmq

# Dicionário para registrar "seguidores" (para eventuais funcionalidades)
followers = {}  # Exemplo: { "1": {"2", "3"}, ... }

def server_loop():
    context = zmq.Context()
    # REP socket para receber comandos enviados pelos clientes via broker
    rep_socket = context.socket(zmq.REP)
    rep_socket.connect("tcp://localhost:5556")
    
    # PUB socket para transmitir notificações a todos os clientes inscritos
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind("tcp://*:5560")
    
    print("Server iniciado: aguardando comandos...")
    
    while True:
        try:
            request = rep_socket.recv_string()
            # Espera receber comandos no formato: COMMAND|<client_id>|<args...>
            parts = request.split("|")
            command = parts[0].upper()
            
            if command == "PUB":
                if len(parts) < 3:
                    rep_socket.send_string("Formato incorreto para publicação.")
                    continue
                publisher_id = parts[1]
                message = parts[2]
                # Formata a notificação com um tópico para que clientes possam filtrar.
                # O tópico será: "Cliente <publisher_id>:" seguido da mensagem.
                notification = f"Cliente {publisher_id}: {message}"
                pub_socket.send_string(notification)  # Envia a notificação
                rep_socket.send_string("Publicação processada.")
            
            elif command == "SEGUIR":
                if len(parts) < 3:
                    rep_socket.send_string("Formato incorreto para seguir comando.")
                    continue
                client_id = parts[1]
                target_id = parts[2]
                if target_id not in followers:
                    followers[target_id] = set()
                followers[target_id].add(client_id)
                rep_socket.send_string(f"O cliente {client_id} agora segue o {target_id}.")
            
            elif command == "STATUS":
                rep_socket.send_string(str(followers))
            
            else:
                rep_socket.send_string("Comando desconhecido.")
                
        except Exception as e:
            rep_socket.send_string("Erro: " + str(e))
            break

    rep_socket.close()
    pub_socket.close()
    context.term()

if __name__ == '__main__':
    server_loop()
