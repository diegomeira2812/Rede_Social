@echo off

echo Compilando o Broker.java...
javac -cp lib\jeromq-0.5.1.jar -d bin src\java\Broker.java

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
start cmd /k "start cmd /k "python src/python/server.py" 1



REM Inicia instancias do cliente C++ (client.exe) em novas janelas
REM ir para a pasta src\cpp\
REM cl /EHsc client.cpp /I"C:\vcpkg\installed\x64-windows\include" /link /LIBPATH:"C:\vcpkg\installed\x64-windows\lib" libzmq.lib
start cmd /k "src\cpp\client.exe 1"
start cmd /k "src\cpp\client.exe 2"

