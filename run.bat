@echo off

echo Compilando o Broker.java...
javac -cp lib\jeromq-0.5.1.jar -d bin src\Broker.java

REM Verifica se houve erro na compilação. Se sim, interrompe a execução.
if errorlevel 1 (
    echo Erro na compilacao do Broker.java.
    pause
    exit /b 1
)

echo Compilacao concluida com sucesso!

REM Inicia o broker Java em uma nova janela de comando
start cmd /k "java -cp bin;lib\jeromq-0.5.1.jar Broker"

REM Inicia 2 instancias do servidor Python em novas janelas
start cmd /k "python server.py" 1
start cmd /k "python server.py" 2

REM Inicia 3 instancias do cliente Python em novas janelas
start cmd /k "python client.py" 1
start cmd /k "python client.py" 2
start cmd /k "python client.py" 3
