import zmq
import time

def main():
    context = zmq.Context()
    # Socket REQ para enviar requisições e receber respostas
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")  # Conecta ao frontend do broker

    for i in range(5):
        try:
            request = f"Olá, requisição {i}"
            print("Cliente enviando:", request)
            socket.send_string(request)

            reply = socket.recv_string()
            print("Cliente recebeu:", reply)
            time.sleep(1)
        except KeyboardInterrupt:
            print("Cliente interrompido.")
            break

    socket.close()
    context.term()

if __name__ == '__main__':
    main()
