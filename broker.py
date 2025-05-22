import zmq
import threading

def replication_loop(context):
    # replicação entre servidores
    rep_pull_socket = context.socket(zmq.PULL)
    rep_pull_socket.bind("tcp://*:5571")
    
    rep_pub_socket = context.socket(zmq.PUB)
    rep_pub_socket.bind("tcp://*:5570")
    
    print("Broker de replicação iniciado.")
    
    while True:
        try:
            msg = rep_pull_socket.recv_string()
            rep_pub_socket.send_string(msg)
        except zmq.ZMQError:
            break
    rep_pull_socket.close()
    rep_pub_socket.close()

def notification_loop(context):
    # notificacoes para clientes
    notif_pull = context.socket(zmq.PULL)
    notif_pull.bind("tcp://*:5581")
    
    notif_pub = context.socket(zmq.PUB)
    notif_pub.bind("tcp://*:5560")
    
    print("Broker de notificações iniciado.")
    
    while True:
        try:
            msg = notif_pull.recv_string()
            notif_pub.send_string(msg)
        except zmq.ZMQError:
            break
    notif_pull.close()
    notif_pub.close()

def main():
    context = zmq.Context()

    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")

    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")

    print("Broker iniciado e aguardando mensagens...")
    
    replication_thread = threading.Thread(target=replication_loop, args=(context,), daemon=True)
    replication_thread.start()
    
    notification_thread = threading.Thread(target=notification_loop, args=(context,), daemon=True)
    notification_thread.start()
    
    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        print("Broker interrompido.")

    frontend.close()
    backend.close()
    context.term()

if __name__ == '__main__':
    main()
