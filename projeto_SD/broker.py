import zmq

def main():
    context = zmq.Context()

    # Socket para os clientes: ROUTER permite múltiplas conexões e roteia as mensagens
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")  # Usuários irão se conectar aqui

    # Socket para os servidores: DEALER permite balanceamento de carga de forma assíncrona
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")   # Servidores se conectam aqui

    print("Broker iniciado e aguardando mensagens...")

    try:
        zmq.proxy(frontend, backend)
    except KeyboardInterrupt:
        print("Broker interrompido.")

    frontend.close()
    backend.close()
    context.term()

if __name__ == '__main__':
    main()
