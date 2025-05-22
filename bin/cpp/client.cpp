#include <zmq.hpp>
#include <thread>
#include <iostream>
#include <string>
#include <mutex>
#include <fstream>
#include <sstream>
#include <ctime>

// Função simples para obter timestamp em string
std::string current_timestamp() {
    std::time_t now = std::time(nullptr);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    return std::string(buf);
}

// Logger global para o cliente
std::ofstream logFile;

// Função de logging
void logEvent(const std::string &event) {
    std::lock_guard<std::mutex> lock(std::mutex());
    logFile << current_timestamp() << " " << event << std::endl;
}

// Variáveis globais para o socket SUB e controle de assinaturas
zmq::socket_t *sub_socket_global = nullptr;
std::mutex sub_mutex;

void subscribe_notifications(zmq::context_t &context, const std::string &client_id) {
    zmq::socket_t sub_socket(context, ZMQ_SUB);
    sub_socket.connect("tcp://localhost:5560");

    std::string sub_filter = "PVT|" + client_id + "|";
    sub_socket.setsockopt(ZMQ_SUBSCRIBE, sub_filter.c_str(), sub_filter.size());

    {
        std::lock_guard<std::mutex> lock(sub_mutex);
        sub_socket_global = &sub_socket;
    }
    
    logEvent("Iniciando thread de notificações no cliente " + client_id);
    
    while (true) {
        zmq::message_t message;
        sub_socket.recv(&message);
        std::string msg(static_cast<char*>(message.data()), message.size());

        // Filtra mensagens privadas
        if (msg.find(sub_filter) == 0) {
            std::cout << "\n[Nova Mensagem Privada]: " << msg << std::endl;
            logEvent("Mensagem Privada recebida: " + msg);
        } else {
            std::cout << "\n[Nova publicacao]: " << msg << std::endl;
            logEvent("Notificacao Publica recebida: " + msg);
        }
    }
}

void subscribe_target(const std::string &target_id) {
    std::lock_guard<std::mutex> lock(sub_mutex);
    if (sub_socket_global != nullptr) {
        std::string topic = "Cliente " + target_id + ":";
        sub_socket_global->setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.size());
        logEvent("Inscrito no tópico: " + topic);
    }
}

int main(int argc, char *argv[]) {
    std::string client_id = (argc > 1) ? argv[1] : "1";
    
    // Abre o arquivo log para o cliente (ex.: client1.log)
    logFile.open("log/client" + client_id + ".log", std::ios_base::app);
    logEvent("Cliente " + client_id + " iniciado.");
    
    zmq::context_t context(1);
    zmq::socket_t req_socket(context, ZMQ_REQ);
    req_socket.connect("tcp://localhost:5555");
    
    std::thread sub_thread(subscribe_notifications, std::ref(context), client_id);
    sub_thread.detach();
    
    std::cout << "Cliente " << client_id << " iniciado." << std::endl;
    std::cout << "\nOpcoes disponiveis:" << std::endl;
    std::cout << "  pub    - Fazer uma publicacao" << std::endl;
    std::cout << "  priv   - Enviar uma mensagem privada" << std::endl;
    std::cout << "  seguir - Seguir um usuario para receber publicacoes" << std::endl;
    std::cout << "  sair   - Encerrar o programa" << std::endl;
    
    std::string option;
    while (true) {
        std::cout << "\nDigite uma opcao: ";
        std::getline(std::cin, option);
        for (auto &c : option) c = tolower(c);
        
        if (option == "sair")
            break;
        
        std::string input;
        std::string cmd;
        
        if (option == "pub") {
            std::cout << "Digite sua publicacao: ";
            std::getline(std::cin, input);
            cmd = "PUB|" + client_id + "|" + input;
            logEvent("Publicacao enviada: " + input);
        } else if (option == "priv") {
            std::string target;
            std::cout << "Digite o id do destinatario: ";
            std::getline(std::cin, target);
            std::cout << "Digite sua mensagem privada: ";
            std::getline(std::cin, input);
            cmd = "PRIV|" + client_id + "|" + target + "|" + input;
            logEvent("Mensagem privada enviada para " + target + ": " + input);
        } else if (option == "seguir") {
            std::string target;
            std::cout << "Digite o id do cliente para seguir: ";
            std::getline(std::cin, target);
            cmd = "SEGUIR|" + client_id + "|" + target;
            subscribe_target(target);
            logEvent("Seguindo cliente: " + target);
        } else {
            std::cout << "\nOpcao invalida. Tente: pub, priv, seguir ou sair." << std::endl;
            continue;
        }
        
        req_socket.send(cmd.c_str(), cmd.size());
        zmq::message_t reply;
        req_socket.recv(&reply);
        std::string replyStr(static_cast<char*>(reply.data()), reply.size());
        std::cout << "\n[Servidor]: " << replyStr << std::endl;
        logEvent("Resposta do servidor: " + replyStr);
    }
    
    logEvent("Cliente " + client_id + " encerrado.");
    logFile.close();
    return 0;
}
