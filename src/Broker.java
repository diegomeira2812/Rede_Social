import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Broker {

    // Loop de replicação: recebe mensagens do canal PULL (porta 5571) e as retransmite via PUB (porta 5570)
    static class ReplicationLoop implements Runnable {
        private ZContext context;

        public ReplicationLoop(ZContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            // Cria o socket PULL e o socket PUB para replicação
            ZMQ.Socket repPullSocket = context.createSocket(ZMQ.PULL);
            repPullSocket.bind("tcp://*:5571");
            ZMQ.Socket repPubSocket = context.createSocket(ZMQ.PUB);
            repPubSocket.bind("tcp://*:5570");
            
            System.out.println("Broker de replicação iniciado.");
            
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // Aguarda a mensagem (recvStr retornará null se o socket for fechado)
                    String msg = repPullSocket.recvStr();
                    if (msg == null) {
                        break;
                    }
                    repPubSocket.send(msg);
                }
            } catch (Exception e) {
                // Se ocorrer alguma exceção, saia do loop
            } finally {
                repPullSocket.close();
                repPubSocket.close();
            }
        }
    }

    // Loop de notificações: recebe as notificações via PULL (porta 5581) e as retransmite via PUB (porta 5560)
    static class NotificationLoop implements Runnable {
        private ZContext context;

        public NotificationLoop(ZContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            // Cria o socket PULL e o socket PUB para notificações
            ZMQ.Socket notifPull = context.createSocket(ZMQ.PULL);
            notifPull.bind("tcp://*:5581");
            ZMQ.Socket notifPub = context.createSocket(ZMQ.PUB);
            notifPub.bind("tcp://*:5560");
            
            System.out.println("Broker de notificações iniciado.");
            
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String msg = notifPull.recvStr();
                    if (msg == null) {
                        break;
                    }
                    notifPub.send(msg);
                }
            } catch (Exception e) {
                // Controle de exceção
            } finally {
                notifPull.close();
                notifPub.close();
            }
        }
    }

    public static void main(String[] args) {
        // Cria o contexto ZeroMQ que será compartilhado entre os sockets
        try (ZContext context = new ZContext()) {

            // Cria o socket frontend (ROUTER) para clientes e o backend (DEALER) para servidores
            ZMQ.Socket frontend = context.createSocket(ZMQ.ROUTER);
            frontend.bind("tcp://*:5555");

            ZMQ.Socket backend = context.createSocket(ZMQ.DEALER);
            backend.bind("tcp://*:5556");

            System.out.println("Broker iniciado e aguardando mensagens...");

            // Inicia as threads de replicação e notificações como daemon threads
            Thread replicationThread = new Thread(new ReplicationLoop(context));
            replicationThread.setDaemon(true);
            replicationThread.start();

            Thread notificationThread = new Thread(new NotificationLoop(context));
            notificationThread.setDaemon(true);
            notificationThread.start();

            // Usa o proxy para encaminhar as mensagens entre frontend e backend
            try {
                ZMQ.proxy(frontend, backend, null);
            } catch (Exception e) {
                System.out.println("Broker interrompido.");
            } finally {
                frontend.close();
                backend.close();
            }
        }
    }
}
