import zmq, threading, time, sys, random, logging
from datetime import datetime

# Configuração do logging para o servidor
logging.basicConfig(
    filename='server.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Variáveis globais para eleição e sincronização
coordinator_id = None
election_ok_received = False
sync_replies = {}
last_coordinator_sync = None
sync_timeout = 40  # se o coordenador não "pulsar" em 40s, considera-se falho

election_lock = threading.Lock()
sync_lock = threading.Lock()

# Variável para simular o relógio interno (offset em segundos)
local_offset = 0

# Dicionario de seguidores
followers = {}

def get_local_clock():
    """Retorna a hora local simulada (tempo real + offset)."""
    return time.time() + local_offset

def adjust_clock(offset):
    """Ajusta o offset do relógio local."""
    global local_offset
    local_offset += offset
    logging.info(f"Ajuste de relógio: offset modificado em {offset} segundos. Novo offset: {local_offset}")

# Socket global para enviar mensagens de eleição e sincronização
global_rep_push_socket = None

def replication_listener(context, server_id):
    """
    Escuta mensagens enviadas para o canal de replicação, eleição e sincronização.
    Essas mensagens são aquelas enviadas por outros servidores.
    """
    global last_coordinator_sync, coordinator_id, election_ok_received, sync_replies, last_coordinator_sync
    rep_sub_socket = context.socket(zmq.SUB)
    rep_sub_socket.connect("tcp://localhost:5570")
    # Inscrever-se para os três tipos de tópicos:
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "REPL|")
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "ELEC|")
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "SYNC|")
    
    logging.info(f"Servidor {server_id} escutando mensagens de replicação, eleição e sincronização.")
    
    while True:
        try:
            msg = rep_sub_socket.recv_string()
            logging.info(f"Mensagem recebida no canal de replicação: {msg}")
            parts = msg.split("|")
            topic = parts[0]
            if topic == "REPL":
                # Processamento de mensagens de replicação (pode incluir log, se necessário)
                pass
            elif topic == "ELEC":
                sender_id = parts[1]
                msg_type = parts[2]
                try:
                    my_id_num = int(server_id.replace("server", ""))
                    sender_id_num = int(sender_id.replace("server", ""))
                except:
                    my_id_num = server_id
                    sender_id_num = sender_id

                if msg_type == "ELECTION":
                    if my_id_num > sender_id_num:
                        global_rep_push_socket.send_string(f"ELEC|{server_id}|OK")
                        logging.info(f"Servidor {server_id} enviando OK para a eleição em resposta a {sender_id}.")
                elif msg_type == "OK":
                    with election_lock:
                        election_ok_received = True
                        logging.info(f"Servidor {server_id} recebeu OK na eleição de {sender_id}.")
                elif msg_type == "COORDINATOR":
                    coordinator_id = sender_id
                    last_coordinator_sync = time.time()
                    logging.info(f"Servidor {server_id} atualiza coordenador para {sender_id}.")
            elif topic == "SYNC":
                msg_type = parts[1]
                if msg_type == "REPLY":
                    sender = parts[2]
                    sender_time = float(parts[3])
                    with sync_lock:
                        sync_replies[sender] = sender_time
                    logging.info(f"Sincronização: recebida resposta de {sender} com tempo {sender_time}.")
                elif msg_type == "ADJUST":
                    target = parts[2]
                    offset = float(parts[3])
                    if target == server_id or target == "ALL":
                        adjust_clock(offset)
                        logging.info(f"Sincronização: ajuste aplicado para {target} com offset {offset}.")
                elif msg_type == "REQUEST":
                    if coordinator_id != server_id:
                        global_rep_push_socket.send_string(f"SYNC|REPLY|{server_id}|{get_local_clock()}")
                        logging.info(f"Servidor {server_id} enviou SYNC|REPLY com seu horário.")
        except zmq.ZMQError:
            break
    rep_sub_socket.close()

def election_and_sync_manager(server_id):
    global coordinator_id, election_ok_received, sync_replies, last_coordinator_sync
    while True:
        # Verifica se o coordenador atual está com timeout
        if coordinator_id is not None and coordinator_id != server_id:
        #if coordinator_id is None and not election_ok_received:
            if last_coordinator_sync is not None and (time.time() - last_coordinator_sync > sync_timeout):
                print(f"[DEBUG] {server_id} detectou timeout do coordenador {coordinator_id}. Reiniciando eleição.")
                coordinator_id = None
        
        if coordinator_id is None:
            delay = random.uniform(0.5, 2.0)
            time.sleep(delay)
            with election_lock:
                election_ok_received = False
            global_rep_push_socket.send_string(f"ELEC|{server_id}|ELECTION")
            print(f"[DEBUG] {server_id} iniciou uma eleição (delay de {delay:.2f} segundos).")
            time.sleep(7)  # Aguarda as respostas

            with election_lock:
                if not election_ok_received:
                    coordinator_id = server_id
                    global_rep_push_socket.send_string(f"ELEC|{server_id}|COORDINATOR")
                    last_coordinator_sync = time.time()
                    print(f"[DEBUG] {server_id} se declarou COORDENADOR!")
                else:
                    print(f"[DEBUG] {server_id} recebeu resposta OK, logo não se declara coordenador.")
                    time.sleep(3)

        else:
            # Se este servidor é o coordenador, inicia a sincronização de relógios
            if coordinator_id == server_id:
                with sync_lock:
                    sync_replies = {}
                global_rep_push_socket.send_string(f"SYNC|REQUEST|{server_id}|{get_local_clock()}")
                print(f"[DEBUG] {server_id} (COORDENADOR) enviou pedido de sincronização.")
                time.sleep(5)  # Tempo para aguardar respostas

                my_clock = get_local_clock()
                with sync_lock:
                    total_time = my_clock + sum(sync_replies.values())
                    count = 1 + len(sync_replies)
                avg_time = total_time / count
                own_offset = avg_time - my_clock
                adjust_clock(own_offset)
                global_rep_push_socket.send_string(f"SYNC|ADJUST|ALL|{own_offset}")
                print(f"[DEBUG] {server_id} (COORDENADOR) sincronizou relógios com offset {own_offset:.2f}.")
                last_coordinator_sync = time.time()
            time.sleep(30)


def server_loop(server_id):
    context = zmq.Context()
    
    # Socket REP para receber comandos dos clientes via broker
    rep_socket = context.socket(zmq.REP)
    rep_socket.connect("tcp://localhost:5556")
    
    # Socket PUSH para enviar notificações para os clientes (via broker)
    notif_push_socket = context.socket(zmq.PUSH)
    notif_push_socket.connect("tcp://localhost:5581")
    
    # Socket PUSH para enviar mensagens de replicação, eleição e sincronização
    rep_push_socket = context.socket(zmq.PUSH)
    rep_push_socket.connect("tcp://localhost:5571")
    
    global global_rep_push_socket
    global_rep_push_socket = rep_push_socket
    
    logging.info(f"Servidor {server_id} iniciado e aguardando comandos...")
    print(f"Servidor {server_id} iniciado e aguardando comandos...")
    
    rep_listener_thread = threading.Thread(target=replication_listener, args=(context, server_id), daemon=True)
    rep_listener_thread.start()
    
    election_thread = threading.Thread(target=election_and_sync_manager, args=(server_id,), daemon=True)
    election_thread.start()
    
    while True:
        try:
            request = rep_socket.recv_string()
            logging.info(f"Servidor {server_id} recebeu comando: {request}")
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
                logging.info(f"Publicação de {publisher_id} encaminhada: {message}")
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
                logging.info(f"O cliente {client_id} começou a seguir {target_id}.")
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
                logging.info(f"Mensagem privada enviada de {sender_id} para {target_id}: {priv_message}")
        except Exception as e:
            logging.error(f"Exceção no server_loop: {str(e)}")
            break

    rep_socket.close()
    notif_push_socket.close()
    rep_push_socket.close()
    context.term()

if __name__ == '__main__':
    server_id = sys.argv[1] if len(sys.argv) > 1 else "server1"
    server_loop(server_id)
