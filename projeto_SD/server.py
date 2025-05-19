import zmq
import time

def main():
    context = zmq.Context()
    # Usamos um socket REP para seguir o padrão REQ/REP
    socket = context.socket(zmq.REP)
    socket.connect("tcp://localhost:5556")  # Conecta ao backend do broker

    print("Servidor iniciado, aguardando requisições...")

    while True:
        try:
            # Recebe a mensagem do cliente (através do broker)
            message = socket.recv_string()
            print("Servidor recebeu:", message)

            # Simula processamento (por exemplo, consulta ou lógica qualquer)
            time.sleep(1)

            reply = f"Resposta para '{message}'"
            socket.send_string(reply)
        except KeyboardInterrupt:
            print("Servidor interrompido.")
            break

    socket.close()
    context.term()

if __name__ == '__main__':
    main()
