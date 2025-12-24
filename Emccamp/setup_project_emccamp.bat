@echo off
chcp 65001
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "LOG_FILE=data\logs\execucao_emccamp.log"
if not exist data\logs mkdir data\logs >nul

echo =================================
echo  CONFIGURANDO AMBIENTE EMCCAMP
echo =================================
echo [%date% %time%] INICIO Setup ambiente >> "%LOG_FILE%"

python --version
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Python nao encontrado.
    echo Instale Python 3.10 ou superior em https://www.python.org/downloads/
    echo [%date% %time%] ERRO Python ausente >> "%LOG_FILE%"
    pause
    exit /b 1
)
echo [%date% %time%] [OK] Python >> "%LOG_FILE%"

echo.
echo 1. Criando ambiente virtual (venv)...
python -m venv venv
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ERRO Criacao venv >> "%LOG_FILE%"
    pause
    exit /b 1
)
echo [%date% %time%] [OK] Criacao venv >> "%LOG_FILE%"

echo.
echo 2. Ativando ambiente virtual...
call venv\Scripts\activate.bat
if %ERRORLEVEL% NEQ 0 (
    echo [AVISO] Falha ao ativar venv pelo caminho relativo. Tentando via python...
    python -m venv venv
    call venv\Scripts\activate.bat
)

set "REQ_PATH=%SCRIPT_DIR%requirements.txt"

echo.
echo 3. Instalando dependencias...
if exist "%REQ_PATH%" (
    "%SCRIPT_DIR%venv\Scripts\python.exe" -m pip install --upgrade pip >nul 2>nul
    "%SCRIPT_DIR%venv\Scripts\python.exe" -m pip install -r "%REQ_PATH%"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERRO] Falha ao instalar dependencias.
        echo [%date% %time%] ERRO Dependencias >> "%LOG_FILE%"
        pause
        exit /b 1
    )
    echo [%date% %time%] [OK] Dependencias >> "%LOG_FILE%"
) else (
    echo [ERRO] Arquivo requirements.txt nao encontrado em %SCRIPT_DIR%
    echo O projeto EMCCAMP precisa de seu arquivo requirements.txt.
    echo [%date% %time%] ERRO Requirements ausente >> "%LOG_FILE%"
    pause
    exit /b 1
)

echo.
echo =================================
echo  AMBIENTE EMCCAMP CONFIGURADO!
echo =================================
echo [%date% %time%] FIM Setup concluido >> "%LOG_FILE%"
pause
