@echo off
chcp 65001
setlocal enabledelayedexpansion
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "LOG_FILE=data\logs\execucao_emccamp.log"
if not exist data\logs mkdir data\logs >nul

echo ===============================================
echo    PIPELINE EMCCAMP - MENU INTERATIVO
echo ===============================================
echo.

if not exist venv\Scripts\python.exe (
    echo ERRO: Ambiente virtual nao encontrado em %SCRIPT_DIR%venv
    echo Execute primeiro: setup_project_emccamp.bat
    echo.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
if %ERRORLEVEL% NEQ 0 (
    echo ERRO: Falha ao ativar ambiente virtual.
    pause
    exit /b 1
)

set "REQ_PATH=%SCRIPT_DIR%requirements.txt"
if exist "%REQ_PATH%" (
    echo Atualizando dependencias...
    "%SCRIPT_DIR%venv\Scripts\python.exe" -m pip install --upgrade pip >nul 2>nul
    "%SCRIPT_DIR%venv\Scripts\python.exe" -m pip install -r "%REQ_PATH%"
    if %ERRORLEVEL% NEQ 0 (
        echo ERRO: Falha ao instalar dependencias do projeto isolado.
        pause
        exit /b 1
    )
) else (
    echo AVISO: requirements.txt nao encontrado; dependencias nao foram atualizadas.
)

REM ===============================
REM MODO NAO-INTERATIVO (RUN-ONCE)
REM Se um argumento numerico (1-8) for passado, executa e sai
set "ONCE="
if not "%~1"=="" (
    set "OPT=%~1"
    set "ONCE=1"
    goto SELECT
)

:MENU
echo ===============================================
echo Selecione uma opcao:
echo ===============================================
echo 1. Pipeline completo (extrair ^> tratar ^> batimento ^> baixa ^> devolucao ^> enriquecimento)
echo 2. Extrair TODAS as bases (EMCCAMP, MAX, Judicial, Baixas, Acordos)
echo 3. Pipeline SEM EXTRACAO (tratar ^> batimento ^> baixa ^> devolucao ^> enriquecimento)
echo 4. Tratamento completo (EMCCAMP + MAX)
echo 5. Executar somente Batimento
echo 6. Executar somente Baixa
echo 7. Executar somente Devolucao
echo 8. Executar somente Enriquecimento
echo 9. Sair
echo.
set /p OPT=Digite sua escolha (1-9): 
goto SELECT

:SELECT
if "%OPT%"=="1" goto FULL
if "%OPT%"=="2" goto EXTRACT_ALL
if "%OPT%"=="3" goto FULL_NO_EXTRACT
if "%OPT%"=="4" goto TREAT_ALL
if "%OPT%"=="5" goto ONLY_BAT
if "%OPT%"=="6" goto ONLY_BAIXA
if "%OPT%"=="7" goto ONLY_DEVOLUCAO
if "%OPT%"=="8" goto ONLY_ENRICH
if "%OPT%"=="9" goto EXIT
echo.
echo Opcao invalida.
echo.
if defined ONCE goto EXIT
goto MENU

:EXTRACT_ALL
echo.
echo Extraindo TODAS as bases (EMCCAMP, MAX, Judicial, Baixas, Acordos)...
echo [%date% %time%] Extracao iniciada >> "%LOG_FILE%"
python main.py extract all
if errorlevel 1 (
    echo [%date% %time%] ERRO Extracao >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Extracao >> "%LOG_FILE%"
echo.
goto RETURN

:TREAT_ALL
echo.
echo Tratando bases EMCCAMP e MAX...
echo [%date% %time%] Tratamento EMCCAMP iniciado >> "%LOG_FILE%"
python main.py treat emccamp
if errorlevel 1 (
    echo [%date% %time%] ERRO Tratamento EMCCAMP >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Tratamento EMCCAMP >> "%LOG_FILE%"
echo.
echo [%date% %time%] Tratamento MAX iniciado >> "%LOG_FILE%"
python main.py treat max
if errorlevel 1 (
    echo [%date% %time%] ERRO Tratamento MAX >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Tratamento MAX >> "%LOG_FILE%"
echo.
goto RETURN

:FULL
echo.
echo ===============================================
echo    EXECUTANDO PIPELINE COMPLETO
echo ===============================================
echo [%date% %time%] INICIO Pipeline completo >> "%LOG_FILE%"
echo.
echo [1/6] Extraindo bases (EMCCAMP, MAX, Judicial, Baixas, Acordos)...
python main.py extract all
if errorlevel 1 (
    echo [%date% %time%] ERRO Extracao >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Extracao >> "%LOG_FILE%"
echo.
echo [2/6] Tratando EMCCAMP...
python main.py treat emccamp
if errorlevel 1 (
    echo [%date% %time%] ERRO Tratamento EMCCAMP >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Tratamento EMCCAMP >> "%LOG_FILE%"
echo.
echo [3/6] Tratando MAX...
python main.py treat max
if errorlevel 1 (
    echo [%date% %time%] ERRO Tratamento MAX >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Tratamento MAX >> "%LOG_FILE%"
echo.
echo [4/7] Executando Batimento...
python main.py batimento
if errorlevel 1 (
    echo [%date% %time%] ERRO Batimento >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Batimento >> "%LOG_FILE%"
echo.
echo [5/7] Executando Baixa...
python main.py baixa
if errorlevel 1 (
    echo [%date% %time%] ERRO Baixa >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Baixa >> "%LOG_FILE%"
echo.
echo [6/7] Executando Devolucao...
python main.py devolucao
if errorlevel 1 (
    echo [%date% %time%] ERRO Devolucao >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Devolucao >> "%LOG_FILE%"
echo.
echo [7/7] Executando Enriquecimento de Contato...
python main.py enriquecimento
if errorlevel 1 (
    echo [%date% %time%] ERRO Enriquecimento >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Enriquecimento >> "%LOG_FILE%"
echo.
echo ===============================================
echo    PIPELINE COMPLETO CONCLUIDO COM SUCESSO!
echo ===============================================
echo [%date% %time%] FIM Pipeline completo >> "%LOG_FILE%"
goto RETURN

:FULL_NO_EXTRACT
echo.
echo ===============================================
echo  EXECUTANDO PIPELINE SEM EXTRACAO
echo  (usa bases ja extraidas)
echo ===============================================
echo [%date% %time%] INICIO Pipeline sem extracao >> "%LOG_FILE%"
echo.
echo [1/5] Tratando EMCCAMP...
python main.py treat emccamp
if errorlevel 1 (
    echo [%date% %time%] ERRO Tratamento EMCCAMP >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Tratamento EMCCAMP >> "%LOG_FILE%"
echo.
echo [2/5] Tratando MAX...
python main.py treat max
if errorlevel 1 (
    echo [%date% %time%] ERRO Tratamento MAX >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Tratamento MAX >> "%LOG_FILE%"
echo.
echo [3/6] Executando Batimento...
python main.py batimento
if errorlevel 1 (
    echo [%date% %time%] ERRO Batimento >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Batimento >> "%LOG_FILE%"
echo.
echo [4/6] Executando Baixa...
python main.py baixa
if errorlevel 1 (
    echo [%date% %time%] ERRO Baixa >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Baixa >> "%LOG_FILE%"
echo.
echo [5/6] Executando Devolucao...
python main.py devolucao
if errorlevel 1 (
    echo [%date% %time%] ERRO Devolucao >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Devolucao >> "%LOG_FILE%"
echo.
echo [6/6] Executando Enriquecimento de Contato...
python main.py enriquecimento
if errorlevel 1 (
    echo [%date% %time%] ERRO Enriquecimento >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Enriquecimento >> "%LOG_FILE%"
echo.
echo ===============================================
echo  PIPELINE (SEM EXTRACAO) CONCLUIDO COM SUCESSO!
echo ===============================================
echo [%date% %time%] FIM Pipeline sem extracao >> "%LOG_FILE%"
goto RETURN

:ONLY_BAT
echo.
echo Executando batimento (usa arquivos tratados mais recentes)...
echo [%date% %time%] Batimento iniciado >> "%LOG_FILE%"
python main.py batimento
if errorlevel 1 (
    echo [%date% %time%] ERRO Batimento >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Batimento >> "%LOG_FILE%"
echo.
goto RETURN

:ONLY_BAIXA
echo.
echo Executando Baixa (usa arquivos tratados mais recentes)...
echo [%date% %time%] Baixa iniciada >> "%LOG_FILE%"
python main.py baixa
if errorlevel 1 (
    echo [%date% %time%] ERRO Baixa >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Baixa >> "%LOG_FILE%"
echo.
goto RETURN

:ONLY_DEVOLUCAO
echo.
echo Executando Devolucao (usa arquivos tratados mais recentes)...
echo [%date% %time%] Devolucao iniciada >> "%LOG_FILE%"
python main.py devolucao
if errorlevel 1 (
    echo [%date% %time%] ERRO Devolucao >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Devolucao >> "%LOG_FILE%"
echo.
goto RETURN

:ONLY_ENRICH
echo.
echo Executando Enriquecimento de Contato...
echo [%date% %time%] Enriquecimento iniciado >> "%LOG_FILE%"
python main.py enriquecimento
if errorlevel 1 (
    echo [%date% %time%] ERRO Enriquecimento >> "%LOG_FILE%"
    goto ERROR
)
echo [%date% %time%] [OK] Enriquecimento >> "%LOG_FILE%"
echo.
goto RETURN

:ERROR
echo.
echo [ERRO] A execucao falhou. Verifique as mensagens acima.
echo.
goto RETURN

:EXIT
echo.
echo Encerrando pipeline EMCCAMP.
echo.
endlocal
exit /b 0

:RETURN
REM Caso esteja em modo nao-interativo, sair apos a execucao
if defined ONCE goto EXIT
echo.
pause
goto MENU
