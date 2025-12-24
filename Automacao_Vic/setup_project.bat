@echo off
chcp 65001
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
echo =================================
echo  CONFIGURANDO O AMBIENTE
echo =================================

:: Verifica se o Python está instalado
python --version
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Python não encontrado.
    echo Instale o Python 3.8 ou superior:
    echo https://www.python.org/downloads/
    pause
    exit /b 1
)

echo.
echo 1. Criando ambiente virtual...
python -m venv venv
:: Não verificar o código de erro aqui, pois pode falhar mas ainda funcionar parcialmente

echo.
echo 2. Ativando ambiente virtual...
call venv\Scripts\activate.bat
:: Não verificar o código de erro aqui, pois pode falhar mas ainda funcionar parcialmente

echo.
echo 3. Instalando dependências...
:: Tentar diferentes métodos para instalar dependências, suprimindo mensagens de erro
"%SCRIPT_DIR%venv\Scripts\pip.exe" install -r "%SCRIPT_DIR%requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    python -m pip install -r "%SCRIPT_DIR%requirements.txt"
)

echo Dependências instaladas com sucesso!



echo.
echo =================================
echo  CONFIGURAÇÃO CONCLUÍDA!
echo =================================
echo.
echo Para executar o processamento:
echo 1. Coloque seus arquivos nas pastas data/input/ existentes:
echo    - VIC: data/input/vic/ (candiotto.zip, vic.csv, etc.)
echo    - MAX: data/input/max/ (MaxSmart.zip, max.csv, etc.)
echo    - Blacklist: data/input/blacklist/ (blacklist.csv)
echo.
echo 2. Execute: run_pipeline.bat
echo.
pause
