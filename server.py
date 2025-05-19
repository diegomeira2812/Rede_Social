import zmq, threading, time, sys, random
from datetime import datetime

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

def get_local_clock():
    """Retorna a hora local simulada (tempo real + offset)."""
    return time.time() + local_offset

def adjust_clock(offset):
    """Ajusta o offset do relógio local."""
    global local_offset
    local_offset += offset

# Socket global para enviar mensagens de eleição e sincronização
global_rep_push_socket = None

def replication_listener(context, server_id):
    """
    Escuta mensagens enviadas para o canal de replicação, eleição e sincronização.
    Essas mensagens são aquelas enviadas por outros servidores.
    """
    global last_coordinator_sync
    rep_sub_socket = context.socket(zmq.SUB)
    rep_sub_socket.connect("tcp://localhost:5570")
    # Inscrever-se para os três tipos de tópicos:
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "REPL|")
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "ELEC|")
    rep_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "SYNC|")
    
    print(f"Servidor {server_id} escutando mensagens de replicação, eleição e sincronização.")
    
    while True:
        try:
            msg = rep_sub_socket.recv_string()
            parts = msg.split("|")
            topic = parts[0]
            if topic == "REPL":
                # Processamento de mensagens de replicação (podem ser ignoradas para eleição/sync)
                pass
            elif topic == "ELEC":
                # Formato: ELEC|<sender_id>|<tipo>
                sender_id = parts[1]
                msg_type = parts[2]
                try:
                    my_id_num = int(server_id.replace("server", ""))
                    sender_id_num = int(sender_id.replace("server", ""))
                except:
                    my_id_num = server_id
                    sender_id_num = sender_id

                if msg_type == "ELECTION":
                    # Se meu ID for maior, respondo com OK
                    if my_id_num > sender_id_num:
                        global_rep_push_socket.send_string(f"ELEC|{server_id}|OK")
                elif msg_type == "OK":
                    with election_lock:
                        election_ok_received = True
                elif msg_type == "COORDINATOR":
                    # Atualiza o coordenador e o tempo de último pulso recebido
                    coordinator_id = sender_id
                    last_coordinator_sync = time.time()
            elif topic == "SYNC":
                msg_type = parts[1]
                if msg_type == "REPLY":
                    # Formato: SYNC|REPLY|<server_id>|<local_time>
                    sender = parts[2]
                    sender_time = float(parts[3])
                    with sync_lock:
                        sync_replies[sender] = sender_time
                elif msg_type == "ADJUST":
                    # Formato: SYNC|ADJUST|<target_id>|<offset>
                    target = parts[2]
                    offset = float(parts[3])
                    if target == server_id or target == "ALL":
                        adjust_clock(offset)
                elif msg_type == "REQUEST":
                    # Se não for o coordenador, responde com seu horário local
                    if coordinator_id != server_id:
                        global_rep_push_socket.send_string(f"SYNC|REPLY|{server_id}|{get_local_clock()}")
        except zmq.ZMQError:
            break
    rep_sub_socket.close()

def election_and_sync_manager(server_id):
    """
    Gerencia a eleição (usando o algoritmo de Bully) e, se for o coordenador,
    a sincronização de relógios via algoritmo de Berkeley.
    
    1. Se não houver coordenador (ou se o coordenador falhar devido a timeout), inicia a eleição:
       - Aguarda um atraso aleatório antes de enviar "ELEC|<server_id>|ELECTION".
       - Espera 5 segundos para respostas; se não receber OK, declara-se coordenador.
    2. Se este servidor for o coordenador, periodicamente (a cada 30s) inicia uma rodada de sincronização:
       - Envia um "SYNC|REQUEST" e aguarda respostas.
       - Calcula a média dos tempos e envia um ajuste via "SYNC|ADJUST".
    """
    global coordinator_id, election_ok_received, sync_replies, last_coordinator_sync
    while True:
        # Se houver um coordenador (diferente de mim), verifique se ele está "pulando"
        if coordinator_id is not None and coordinator_id != server_id:
            if last_coordinator_sync is not None and (time.time() - last_coordinator_sync > sync_timeout):
                print(f"[{server_id}] Timeout do coordenador {coordinator_id}. Reiniciando eleição.")
                coordinator_id = None
        
        if coordinator_id is None:
            # Espera um atraso aleatório para evitar eleições simultâneas
            delay = random.uniform(0.5, 2.0)
            time.sleep(delay)
            with election_lock:
                election_ok_received = False
            global_rep_push_socket.send_string(f"ELEC|{server_id}|ELECTION")
            # Aguarda 5 segundos para respostas
            time.sleep(5)
            with election_lock:
                if not election_ok_received:
                    coordinator_id = server_id
                    global_rep_push_socket.send_string(f"ELEC|{server_id}|COORDINATOR")
                    print(f"[{server_id}] Declarado como coordenador.")
                    last_coordinator_sync = time.time()
                else:
                    time.sleep(3)
        else:
            if coordinator_id == server_id:
                with sync_lock:
                    sync_replies = {}
                # Inicia a sincronização: envia um pedido de horário
                global_rep_push_socket.send_string(f"SYNC|REQUEST|{server_id}|{get_local_clock()}")
                time.sleep(5)  # tempo para aguardar as respostas
                my_clock = get_local_clock()
                with sync_lock:
                    total_time = my_clock + sum(sync_replies.values())
                    count = 1 + len(sync_replies)
                avg_time = total_time / count
                own_offset = avg_time - my_clock
                adjust_clock(own_offset)
                # Envia ajuste para todos os servidores (usando "ALL" como receptor)
                global_rep_push_socket.send_string(f"SYNC|ADJUST|ALL|{own_offset}")
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
    global_rep_push_socket = rep_push_socket  # Tornando-o acessível para as outras threads
    
    print(f"Servidor {server_id} iniciado e aguardando comandos...")
    
    # Inicia a thread que escuta mensagens de replicação, eleição e sincronização
    rep_listener_thread = threading.Thread(target=replication_listener, args=(context, server_id), daemon=True)
    rep_listener_thread.start()
    
    # Inicia a thread que gerencia eleição e sincronização
    election_thread = threading.Thread(target=election_and_sync_manager, args=(server_id,), daemon=True)
    election_thread.start()
    
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
                rep_socket.send_string("Estado de seguidores: " + str({}))
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
