@echo off
chcp 65001
setlocal enabledelayedexpansion
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "LOG_FILE=data\logs\execucao_emccamp.log"
if not exist data\logs mkdir data\logs >nul

echo ===============================================
echo  PIPELINE EMCCAMP - EXECUCAO COMPLETA
echo  Extracao ^> Tratamento ^> Batimento ^> Baixa ^> Devolucao ^> Enriquecimento
echo ===============================================
echo Iniciando em %date% %time%

echo [%date% %time%] INICIO Pipeline EMCCAMP > "%LOG_FILE%"

echo.
echo [1/7] Verificando Python...
python --version
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Python nao encontrado.
    echo [%date% %time%] ERRO Python >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Python >> "%LOG_FILE%"

echo [2/7] Preparando ambiente virtual...
if not exist venv\Scripts\python.exe (
    echo Criando ambiente virtual dedicado ao EMCCAMP...
    echo [%date% %time%] INFO - Criando venv >> "%LOG_FILE%"
    python -m venv venv
)
call venv\Scripts\activate.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha ao ativar venv.
    echo [%date% %time%] ERRO - Ativacao venv >> "%LOG_FILE%"
    exit /b 1
)

set "REQ_PATH=%SCRIPT_DIR%requirements.txt"
if not exist "%REQ_PATH%" (
    set "REQ_PATH=%SCRIPT_DIR%..\requirements.txt"
)

if exist "%REQ_PATH%" (
    echo Atualizando dependencias...
    "%SCRIPT_DIR%venv\Scripts\python.exe" -m pip install --upgrade pip >nul 2>nul
    "%SCRIPT_DIR%venv\Scripts\python.exe" -m pip install -r "%REQ_PATH%"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERRO] Falha ao instalar dependencias.
        echo [%date% %time%] ERRO Dependencias >> "%LOG_FILE%"
        exit /b 1
    )
)
echo [%date% %time%] [OK] Ambiente >> "%LOG_FILE%"

echo.
echo [3/8] Extraindo bases (EMCCAMP, MAX, Judicial, Baixas, Acordos)...
python main.py extract all
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha na extracao das bases.
    echo [%date% %time%] ERRO Extracao >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Extracao >> "%LOG_FILE%"

echo.
echo [4/8] Tratando EMCCAMP e MAX...
python main.py treat emccamp
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha no tratamento EMCCAMP.
    echo [%date% %time%] ERRO Tratamento EMCCAMP >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Tratamento EMCCAMP >> "%LOG_FILE%"

python main.py treat max
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha no tratamento MAX.
    echo [%date% %time%] ERRO Tratamento MAX >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Tratamento MAX >> "%LOG_FILE%"

echo.
echo [5/8] Executando Batimento...
python main.py batimento
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha no batimento.
    echo [%date% %time%] ERRO Batimento >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Batimento >> "%LOG_FILE%"

echo.
echo [6/8] Executando Baixa...
python main.py baixa
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha na Baixa.
    echo [%date% %time%] ERRO Baixa >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Baixa >> "%LOG_FILE%"

echo.
echo [7/8] Executando Devolucao...
python main.py devolucao
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha na Devolucao.
    echo [%date% %time%] ERRO Devolucao >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Devolucao >> "%LOG_FILE%"

echo.
echo [8/8] Executando Enriquecimento de Contato...
python main.py enriquecimento
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Falha no Enriquecimento.
    echo [%date% %time%] ERRO Enriquecimento >> "%LOG_FILE%"
    exit /b 1
)
echo [%date% %time%] [OK] Enriquecimento >> "%LOG_FILE%"

echo.
echo ===============================================
echo    PIPELINE EMCCAMP FINALIZADO COM SUCESSO
echo ===============================================
echo [%date% %time%] FIM Pipeline concluido >> "%LOG_FILE%"

echo Arquivos gerados em data\output\emccamp_tratada, max_tratada, batimento, baixa, devolucao, enriquecimento_contato_emccamp
echo Logs: %LOG_FILE%

echo.
pause

endlocal
exit /b 0
