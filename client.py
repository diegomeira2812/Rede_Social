import zmq
import threading
import sys

# Variável global para o socket SUB, que usaremos para atualizar assinaturas dinamicamente
sub_socket_global = None

def subscribe_notifications(context):
    global sub_socket_global
    sub_socket_global = context.socket(zmq.SUB)
    # Conecta-se ao PUB do servidor (porta 5560)
    sub_socket_global.connect("tcp://localhost:5560")
    # Inicialmente, sem subscrição para não receber todas as mensagens
    # (as assinaturas serão configuradas conforme o comando "seguir")
    while True:
        try:
            message = sub_socket_global.recv_string()
            print("\n[Notificação]:", message)
        except zmq.ZMQError:
            break
    sub_socket_global.close()

def subscribe_target(target_id):
    global sub_socket_global
    if sub_socket_global is not None:
        topic = f"Cliente {target_id}:"
        sub_socket_global.setsockopt_string(zmq.SUBSCRIBE, topic)
        print(f"[DEBUG] Agora você está inscrito no tópico: '{topic}'")

def main():
    client_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    context = zmq.Context()
    
    # REQ socket para enviar comandos ao servidor via broker.
    req_socket = context.socket(zmq.REQ)
    req_socket.connect("tcp://localhost:5555")
    
    # Inicia thread para receber notificações (PUB/SUB) diretamente do servidor.
    sub_thread = threading.Thread(target=subscribe_notifications, args=(context,), daemon=True)
    sub_thread.start()
    
    print(f"Cliente {client_id} iniciado.")
    print("Opções disponíveis:")
    print("  pub    - publicar uma mensagem")
    print("  seguir - seguir um cliente")
    print("  sair   - encerrar o programa")
    
    while True:
        try:
            option = input("Digite uma opção: ").strip().lower()
            if option == "sair":
                break
            elif option == "pub":
                publication = input("Digite sua publicação: ")
                # Cria o comando no formato: PUB|<client_id>|<mensagem>
                cmd = f"PUB|{client_id}|{publication}"
                req_socket.send_string(cmd)
                reply = req_socket.recv_string()
                print("[Resposta do servidor]:", reply)
            elif option == "seguir":
                target_id = input("Digite o id do cliente para seguir: ").strip()
                cmd = f"SEGUIR|{client_id}|{target_id}"
                req_socket.send_string(cmd)
                reply = req_socket.recv_string()
                print("[Resposta do servidor]:", reply)
                # Atualiza a assinatura do socket SUB para receber notificações do target seguido.
                subscribe_target(target_id)
            else:
                print("Opção inválida. Tente: pub, seguir ou sair.")
        except KeyboardInterrupt:
            break

    req_socket.close()
    context.term()

if __name__ == '__main__':
    main()
