import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.util.logging.*;
import java.io.IOException;

public class Broker {

    private static final Logger logger = Logger.getLogger("BrokerLog");
    static {
        try {
            FileHandler fh = new FileHandler("log/broker.log", true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
            logger.setLevel(Level.INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Loop de replicação: recebe mensagens do socket PULL (porta 5571) e as retransmite via PUB (porta 5570)
    static class ReplicationLoop implements Runnable {
        private ZContext context;

        public ReplicationLoop(ZContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            // Cria o socket PULL para receber mensagens de replicação
            ZMQ.Socket repPullSocket = context.createSocket(ZMQ.PULL);
            repPullSocket.bind("tcp://*:5571");
            logger.info("Replication PULL socket bound to tcp://*:5571");

            // Cria o socket PUB para retransmitir as mensagens
            ZMQ.Socket repPubSocket = context.createSocket(ZMQ.PUB);
            repPubSocket.bind("tcp://*:5570");
            logger.info("Replication PUB socket bound to tcp://*:5570");

            logger.info("Broker de replicação iniciado.");

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String msg = repPullSocket.recvStr();
                    if (msg == null) {
                        break;
                    }
                    repPubSocket.send(msg);
                    logger.info("Replication: mensagem encaminhada: " + msg);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Erro no ReplicationLoop: " + e.getMessage(), e);
            } finally {
                repPullSocket.close();
                repPubSocket.close();
                logger.info("Replication sockets fechados.");
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
            // Cria o socket PULL para receber notificações
            ZMQ.Socket notifPull = context.createSocket(ZMQ.PULL);
            notifPull.bind("tcp://*:5581");
            logger.info("Notification PULL socket bound to tcp://*:5581");

            // Cria o socket PUB para retransmitir as notificações para os clientes
            ZMQ.Socket notifPub = context.createSocket(ZMQ.PUB);
            notifPub.bind("tcp://*:5560");
            logger.info("Notification PUB socket bound to tcp://*:5560");

            logger.info("Broker de notificações iniciado.");

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String msg = notifPull.recvStr();
                    if (msg == null) {
                        break;
                    }
                    notifPub.send(msg);
                    logger.info("Notificação encaminhada: " + msg);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Erro no NotificationLoop: " + e.getMessage(), e);
            } finally {
                notifPull.close();
                notifPub.close();
                logger.info("Notification sockets fechados.");
            }
        }
    }

    public static void main(String[] args) {
        logger.info("Iniciando Broker...");
        try (ZContext context = new ZContext()) {

            // Cria o socket frontend (ROUTER) para clientes
            ZMQ.Socket frontend = context.createSocket(ZMQ.ROUTER);
            frontend.bind("tcp://*:5555");
            logger.info("Frontend (ROUTER) ligado a tcp://*:5555");

            // Cria o socket backend (DEALER) para servidores
            ZMQ.Socket backend = context.createSocket(ZMQ.DEALER);
            backend.bind("tcp://*:5556");
            logger.info("Backend (DEALER) ligado a tcp://*:5556");

            logger.info("Broker iniciado e aguardando mensagens...");

            // Inicia as threads de replicação e notificações como daemon threads
            Thread replicationThread = new Thread(new ReplicationLoop(context));
            replicationThread.setDaemon(true);
            replicationThread.start();
            logger.info("Thread de replicação iniciada.");

            Thread notificationThread = new Thread(new NotificationLoop(context));
            notificationThread.setDaemon(true);
            notificationThread.start();
            logger.info("Thread de notificações iniciada.");

            // Encaminha as mensagens entre frontend e backend usando o proxy
            try {
                ZMQ.proxy(frontend, backend, null);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Broker interrompido: " + e.getMessage(), e);
            } finally {
                frontend.close();
                backend.close();
                logger.info("Frontend e backend fechados.");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Erro no Broker principal: " + e.getMessage(), e);
        }
        logger.info("Broker encerrado.");
    }
}
