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
start cmd /k "python server.py" 3


REM Inicia 3 instancias do cliente C++ (client.exe) em novas janelas
REM cl /EHsc client.cpp /I"C:\vcpkg\installed\x64-windows\include" /link /LIBPATH:"C:\vcpkg\installed\x64-windows\lib" libzmq.lib
start cmd /k "client.exe 1"
start cmd /k "client.exe 2"
start cmd /k "client.exe 3"
start cmd /k "client.exe 4"
start cmd /k "client.exe 5"
