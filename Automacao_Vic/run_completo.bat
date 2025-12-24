@echo off
chcp 65001 >nul 2>&1
setlocal
set "ROOT=%~dp0"
cd /d "%ROOT%"

echo ===============================================
echo    PIPELINE VIC/MAX - EXECUCAO COMPLETA
echo ===============================================
echo.

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

set "LOG_FILE=data\logs\execucao_completa.log"
if not exist data\logs mkdir data\logs >nul 2>&1
echo [%date% %time%] INICIO - Execucao completa iniciada > "%LOG_FILE%"

echo [1/3] Executando pipeline completo...
python main.py --pipeline-completo
if errorlevel 1 (
    echo [ERRO] Falha na execucao do pipeline. Veja o log em %LOG_FILE%.
    pause
    exit /b 1
)

echo [2/3] Pipeline executado com sucesso. Registrando arquivos gerados...
call :ListarUltimosArquivos >> "%LOG_FILE%"

echo [3/3] Fluxo concluido.
echo.
echo Logs disponiveis em: %LOG_FILE%
echo.
pause
exit /b 0

:ListarUltimosArquivos
echo ================== RELATORIO FINAL ==================
for %%G in (
    "data\output\max_tratada\max_tratada_*.zip"
    "data\output\vic_tratada\vic_base_limpa_*.zip"
    "data\output\devolucao\vic_devolucao_*.zip"
    "data\output\batimento\vic_batimento_*.zip"
    "data\output\enriquecimento\enriquecimento_vic_*.zip"
    "data\output\baixa\vic_baixa_*.csv"
) do (
    for /f "delims=" %%F in ('dir /b /a-d /o-d %%~G 2^>nul') do (
        echo %%~F
        goto CONTINUAR
    )
    echo Nenhum arquivo encontrado para %%~G
:CONTINUAR
)
echo =====================================================
goto :EOF
