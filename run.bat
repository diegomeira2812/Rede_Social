@echo off
start cmd /k "python broker.py"
start cmd /k "python server.py"
start cmd /k "python client.py" 1
start cmd /k "python client.py" 2


