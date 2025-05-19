import zmq
import threading
import sys

sub_socket_global = None

def subscribe_notifications(context, client_id):
    global sub_socket_global
    sub_socket_global = context.socket(zmq.SUB)
    sub_socket_global.connect("tcp://localhost:5560")
    sub_socket_global.setsockopt_string(zmq.SUBSCRIBE, "")
    
    while True:
        try:
            message = sub_socket_global.recv_string()
            if f"PVT|{client_id}|" in message:
                print("\n[Mensagem Privada]:", message)
            else:
                print("\n[Notificação Pública]:", message)
        except zmq.ZMQError:
            break
    sub_socket_global.close()

def main():
    client_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    context = zmq.Context()
    
    req_socket = context.socket(zmq.REQ)
    req_socket.connect("tcp://localhost:5555")
    
    sub_thread = threading.Thread(target=subscribe_notifications, args=(context, client_id), daemon=True)
    sub_thread.start()
    
    print(f"Cliente {client_id} iniciado.")
    print("\nOpções disponíveis:")
    print("  pub    - Publicar uma mensagem pública")
    print("  priv   - Enviar uma mensagem privada")
    print("  seguir - Seguir um cliente para receber publicações")
    print("  sair   - Encerrar o programa")
    
    while True:
        try:
            option = input("\nDigite uma opção: ").strip().lower()
            if option == "sair":
                break
            elif option == "pub":
                publication = input("Digite sua publicação pública: ")
                cmd = f"PUB|{client_id}|{publication}"
                req_socket.send_string(cmd)
                reply = req_socket.recv_string()
                print("\n[Servidor]:", reply)
            elif option == "priv":
                target_id = input("Digite o id do destinatário: ").strip()
                priv_message = input("Digite sua mensagem privada: ")
                cmd = f"PRIV|{client_id}|{target_id}|{priv_message}"
                req_socket.send_string(cmd)
                reply = req_socket.recv_string()
                print("\n[Servidor]:", reply)
            elif option == "seguir":
                target_id = input("Digite o id do cliente para seguir: ").strip()
                cmd = f"SEGUIR|{client_id}|{target_id}"
                req_socket.send_string(cmd)
                reply = req_socket.recv_string()
                print("\n[Servidor]:", reply)
            else:
                print("\nOpção inválida. Tente: pub, priv, seguir ou sair.")
        except KeyboardInterrupt:
            break

    req_socket.close()
    context.term()

if __name__ == '__main__':
    main()
