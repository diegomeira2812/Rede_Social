import zmq
from datetime import datetime
import threading
import sys

followers = {}

def replication_listener(context, server_id):
    """Recebe atualizações de outros servidores."""
    rep_sub_socket = context.socket(zmq.SUB)
    rep_sub_socket.connect("tcp://localhost:5570")
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "REPL|")
    
    print(f"Servidor {server_id} escutando atualizações de replicação.")
    
    while True:
        try:
            msg = rep_sub_socket.recv_string()
            parts = msg.split("|")
            origin_server_id = parts[1]
            if origin_server_id == server_id:
                continue
        except zmq.ZMQError:
            break
    rep_sub_socket.close()

def server_loop(server_id):
    context = zmq.Context()
    
    rep_socket = context.socket(zmq.REP)
    rep_socket.connect("tcp://localhost:5556")

    notif_push_socket = context.socket(zmq.PUSH)
    notif_push_socket.connect("tcp://localhost:5581")

    rep_push_socket = context.socket(zmq.PUSH)
    rep_push_socket.connect("tcp://localhost:5571")
    
    print(f"Servidor {server_id} iniciado e aguardando comandos...")
    
    replication_thread = threading.Thread(target=replication_listener, args=(context, server_id), daemon=True)
    replication_thread.start()
    
    while True:
        try:
            request = rep_socket.recv_string()
            parts = request.split("|")
            command = parts[0].upper()
            
            if command == "PUB":
                if len(parts) < 3:
                    rep_socket.send_string("Erro")
                    continue
                publisher_id = parts[1]
                message = parts[2]
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                final_notification = f"Cliente {publisher_id}: {timestamp} - {message}"
                notif_push_socket.send_string(final_notification)
                rep_socket.send_string("Publicação enviada.")
                replication_msg = f"REPL|{server_id}|PUB|{publisher_id}|{timestamp}|{message}"
                rep_push_socket.send_string(replication_msg)
            
            elif command == "SEGUIR":
                if len(parts) < 3:
                    rep_socket.send_string("Erro")
                    continue
                client_id = parts[1]
                target_id = parts[2]
                if target_id not in followers:
                    followers[target_id] = set()
                followers[target_id].add(client_id)
                rep_socket.send_string(f"O cliente {client_id} agora segue o {target_id}.")
                replication_msg = f"REPL|{server_id}|SEGUIR|{client_id}|{target_id}"
                rep_push_socket.send_string(replication_msg)
            
            elif command == "PRIV":
                if len(parts) < 4:
                    rep_socket.send_string("Erro")
                    continue
                sender_id = parts[1]
                target_id = parts[2]
                priv_message = parts[3]
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                final_notification = f"PVT|{target_id}|Private from Cliente {sender_id} at {timestamp}: {priv_message}"
                notif_push_socket.send_string(final_notification)
                rep_socket.send_string("Mensagem privada enviada.")
                replication_msg = f"REPL|{server_id}|PRIV|{sender_id}|{target_id}|{timestamp}|{priv_message}"
                rep_push_socket.send_string(replication_msg)
            
            elif command == "STATUS":
                rep_socket.send_string(str(followers))
            
            else:
                rep_socket.send_string("Comando desconhecido.")
                
        except Exception:
            break

    rep_socket.close()
    notif_push_socket.close()
    rep_push_socket.close()
    context.term()

if __name__ == '__main__':
    server_id = sys.argv[1] if len(sys.argv) > 1 else "server1"
    server_loop(server_id)
