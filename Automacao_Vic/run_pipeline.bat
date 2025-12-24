@echo off
chcp 65001 >nul 2>&1
setlocal
set "ROOT=%~dp0"
cd /d "%ROOT%"

if not exist "venv\Scripts\python.exe" (
    echo [AUTO] Ambiente virtual nao encontrado. Executando setup_project.bat...
    call setup_project.bat
)

if not exist "venv\Scripts\python.exe" (
    echo ERRO: Ambiente virtual nao encontrado.
    echo Execute primeiro o setup_project.bat.
    echo.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERRO: Falha ao ativar o ambiente virtual.
    pause
    exit /b 1
)

:MENU
echo ===============================================
echo    PIPELINE VIC/MAX - PROCESSAMENTO DE DADOS
echo ===============================================
echo.
echo 1. Pipeline Completo (com extracao)
echo 2. Pipeline Completo (sem extracao)
echo 3. Apenas Extracao das Bases
echo 4. Apenas Tratamento (MAX + VIC)
echo 5. Inclusao (Batimento + Enriquecimento)
echo 6. Baixa (VIC baixado x MAX em aberto)
echo 7. Devolucao (MAX - VIC)
echo 8. Ajuda
echo 9. Sair
echo.
set /p opcao=Digite sua escolha (1-9):

echo.
if "%opcao%"=="1" goto FULL_WITH_EXTRACT
if "%opcao%"=="2" goto PIPELINE
if "%opcao%"=="3" goto EXTRACT
if "%opcao%"=="4" goto TREATMENT
if "%opcao%"=="5" goto INCLUSAO
if "%opcao%"=="6" goto BAIXA
if "%opcao%"=="7" goto DEVOLUCAO
if "%opcao%"=="8" goto HELP
if "%opcao%"=="9" goto EXIT

echo Opcao invalida.
echo.
goto MENU

:FULL_WITH_EXTRACT
echo [1/2] Extracao das bases...
python main.py --extrair-bases
if errorlevel 1 goto ERROR
echo.
echo [2/2] Executando pipeline completo...
python main.py --pipeline-completo
if errorlevel 1 goto ERROR
goto END

:PIPELINE
echo Executando pipeline completo (sem extracao)...
python main.py --pipeline-completo
if errorlevel 1 goto ERROR
goto END

:EXTRACT
echo Executando extracao das bases...
python main.py --extrair-bases
if errorlevel 1 goto ERROR
goto END

:TREATMENT
echo Executando tratamento MAX...
python main.py --max
if errorlevel 1 goto ERROR
echo.
echo Executando tratamento VIC...
python main.py --vic
if errorlevel 1 goto ERROR
goto END

:INCLUSAO
call :FindLatest "data\output\vic_tratada" "vic_base_limpa_*.zip" VIC_FILE
if not defined VIC_FILE goto ERR_NO_VIC
call :FindLatest "data\output\max_tratada" "max_tratada_*.zip" MAX_FILE
if not defined MAX_FILE goto ERR_NO_MAX

echo Executando batimento...
python main.py --batimento "%VIC_FILE%" "%MAX_FILE%"
if errorlevel 1 goto ERROR
call :FindLatest "data\output\batimento" "vic_batimento_*.zip" BAT_FILE
if not defined BAT_FILE (
    echo ERRO: Arquivo de batimento nao encontrado apos processamento.
    goto ERROR
)
for %%I in ("%VIC_FILE%") do set "VIC_ABS=%%~fI"
for %%I in ("%BAT_FILE%") do set "BAT_ABS=%%~fI"

echo Executando enriquecimento...
python -c "from pathlib import Path; from main import PipelineOrchestrator; PipelineOrchestrator().processar_enriquecimento(Path(r'%VIC_ABS%'), Path(r'%BAT_ABS%'))"
if errorlevel 1 goto ERROR
goto END

:BAIXA
call :FindLatest "data\output\vic_tratada" "vic_base_limpa_*.zip" VIC_FILE
if not defined VIC_FILE goto ERR_NO_VIC
call :FindLatest "data\output\max_tratada" "max_tratada_*.zip" MAX_FILE
if not defined MAX_FILE goto ERR_NO_MAX
for %%I in ("%VIC_FILE%") do set "VIC_ABS=%%~fI"
for %%I in ("%MAX_FILE%") do set "MAX_ABS=%%~fI"

echo Executando baixa...
python -c "from pathlib import Path; from main import PipelineOrchestrator; PipelineOrchestrator().processar_baixa(Path(r'%VIC_ABS%'), Path(r'%MAX_ABS%'))"
if errorlevel 1 goto ERROR
goto END

:DEVOLUCAO
call :FindLatest "data\output\vic_tratada" "vic_base_limpa_*.zip" VIC_FILE
if not defined VIC_FILE goto ERR_NO_VIC
call :FindLatest "data\output\max_tratada" "max_tratada_*.zip" MAX_FILE
if not defined MAX_FILE goto ERR_NO_MAX
for %%I in ("%VIC_FILE%") do set "VIC_ABS=%%~fI"
for %%I in ("%MAX_FILE%") do set "MAX_ABS=%%~fI"

echo Executando devolucao...
python -c "from pathlib import Path; from main import PipelineOrchestrator; PipelineOrchestrator().processar_devolucao(Path(r'%VIC_ABS%'), Path(r'%MAX_ABS%'))"
if errorlevel 1 goto ERROR
goto END

:HELP
echo ===============================================
echo    AJUDA - PIPELINE VIC/MAX
echo ===============================================
echo.
echo Entrada: data/input (vic, max, judicial)
echo Saida:   data/output/{max_tratada,vic_tratada,baixa,devolucao,batimento,enriquecimento}
echo Logs:    data/logs
echo.
echo Comandos diretos: venv\Scripts\python.exe main.py --help
echo.
pause
goto MENU

:ERR_NO_VIC
echo ERRO: Arquivo VIC tratado nao encontrado.
goto ERROR

:ERR_NO_MAX
echo ERRO: Arquivo MAX tratado nao encontrado.
goto ERROR

:END
echo.
echo Operacao concluida.
echo.
pause
goto MENU

:ERROR
echo.
echo ERRO: Falha na execucao! Verifique mensagens acima.
echo.
pause
exit /b 1

:EXIT
echo.
echo Obrigado por usar o Pipeline VIC/MAX!
echo.
exit /b 0

:FindLatest
setlocal
set "DIR=%~1"
set "WILD=%~2"
set "RESULT="
for /f "delims=" %%f in ('dir /b /a-d /o-d "%DIR%\%WILD%" 2^>nul') do (
    set "RESULT=%DIR%\%%f"
    goto FOUND
)
:FOUND
endlocal & set "%~3=%RESULT%"
goto :EOF
