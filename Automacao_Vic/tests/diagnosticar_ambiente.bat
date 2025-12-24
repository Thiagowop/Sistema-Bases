@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

echo ===============================================
echo    DIAGNOSTICO DO AMBIENTE
echo ===============================================
echo.
echo Este script verifica se o ambiente esta configurado
echo corretamente para executar o Pipeline VIC/MAX.
echo.

set "ERROS=0"
set "AVISOS=0"

:: ===============================================
:: 1. VERIFICAR LOCALIZACAO E ACESSO
:: ===============================================
echo [1/9] Verificando localizacao do projeto...
echo Local: %SCRIPT_DIR%
echo Drive: %~d0

:: Verificar se e drive de rede
set "DRIVE=%~d0"
if "%DRIVE:~0,2%"=="\\" (
    echo ⚠️  AVISO: Projeto esta em um caminho UNC de rede
    echo    Recomendacao: Copie para um drive mapeado ou local
    set /a AVISOS+=1
)

:: Verificar acesso de escrita
echo. > "%SCRIPT_DIR%test_write.tmp" 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ❌ ERRO: Sem permissao de escrita no diretorio
    set /a ERROS+=1
) else (
    del "%SCRIPT_DIR%test_write.tmp" 2>nul
    echo ✅ OK: Permissoes de escrita: OK
)

echo ✅ OK: Local de execucao verificado
echo.

:: ===============================================
:: 2. VERIFICAR PYTHON
:: ===============================================
echo [2/9] Verificando Python...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ❌ ERRO: Python nao encontrado
    echo.
    echo Solucao:
    echo 1. Instale Python 3.8+: https://www.python.org/downloads/
    echo 2. Durante instalacao, marque: "Add Python to PATH"
    echo 3. Reinicie o computador
    set /a ERROS+=1
) else (
    for /f "tokens=2" %%v in ('python --version 2^>^&1') do set "PY_VERSION=%%v"
    echo ✅ OK: Python !PY_VERSION! encontrado
    
    :: Verificar versão mínima
    for /f "tokens=1,2 delims=." %%a in ("!PY_VERSION!") do (
        set "PY_MAJOR=%%a"
        set "PY_MINOR=%%b"
    )
    if !PY_MAJOR! LSS 3 (
        echo ❌ ERRO: Python muito antigo. Requer Python 3.8+
        set /a ERROS+=1
    ) else if !PY_MAJOR! EQU 3 if !PY_MINOR! LSS 8 (
        echo ⚠️  AVISO: Python !PY_VERSION! pode ter problemas
        echo    Recomendacao: Atualize para Python 3.8+
        set /a AVISOS+=1
    )
)
echo.

:: ===============================================
:: 3. VERIFICAR PIP
:: ===============================================
echo [3/9] Verificando pip...
python -m pip --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ❌ ERRO: pip nao encontrado
    echo.
    echo Solucao:
    echo python -m ensurepip --default-pip
    set /a ERROS+=1
) else (
    for /f "tokens=2" %%v in ('python -m pip --version 2^>^&1') do set "PIP_VERSION=%%v"
    echo ✅ OK: pip !PIP_VERSION! encontrado
)
echo.

:: ===============================================
:: 4. VERIFICAR AMBIENTE VIRTUAL
:: ===============================================
echo [4/9] Verificando ambiente virtual...
if exist "venv\Scripts\python.exe" (
    echo ✅ OK: Ambiente virtual existe
    
    :: Verificar se esta funcionando
    venv\Scripts\python.exe --version >nul 2>&1
    if %ERRORLEVEL% NEQ 0 (
        echo ❌ ERRO: Ambiente virtual corrompido
        echo.
        echo Solucao:
        echo 1. Delete a pasta: venv\
        echo 2. Execute: setup_project.bat
        set /a ERROS+=1
    ) else (
        echo ✅ OK: Ambiente virtual funcionando
    )
) else (
    echo ⚠️  AVISO: Ambiente virtual nao encontrado
    echo.
    echo Solucao:
    echo Execute: setup_project.bat
    set /a AVISOS+=1
)
echo.

:: ===============================================
:: 5. VERIFICAR ESTRUTURA DE DIRETORIOS
:: ===============================================
echo [5/9] Verificando estrutura de diretorios...
set "DIRS_REQUERIDOS=data data\input data\output data\logs src scripts tests"
set "DIRS_OK=1"
for %%d in (%DIRS_REQUERIDOS%) do (
    if exist "%%d\" (
        echo ✅ OK: %%d
    ) else (
        echo ❌ ERRO: %%d nao encontrado
        set "DIRS_OK=0"
        set /a ERROS+=1
    )
)
echo.

:: ===============================================
:: 6. VERIFICAR ARQUIVOS ESSENCIAIS
:: ===============================================
echo [6/9] Verificando arquivos essenciais...
set "ARQUIVOS=config.yaml requirements.txt main.py setup_project.bat run_pipeline.bat"
for %%f in (%ARQUIVOS%) do (
    if exist "%%f" (
        echo ✅ OK: %%f
    ) else (
        echo ❌ ERRO: %%f nao encontrado
        set /a ERROS+=1
    )
)
echo.

:: ===============================================
:: 7. VERIFICAR DEPENDENCIAS (SE VENV EXISTE)
:: ===============================================
echo [7/9] Verificando dependencias Python...
if exist "venv\Scripts\python.exe" (
    venv\Scripts\python.exe -c "import pandas, pyodbc, yaml, dotenv, openpyxl" 2>nul
    if %ERRORLEVEL% NEQ 0 (
        echo ⚠️  AVISO: Algumas dependencias nao instaladas
        echo.
        echo Solucao:
        echo venv\Scripts\python.exe -m pip install -r requirements.txt
        set /a AVISOS+=1
    ) else (
        echo ✅ OK: Todas as dependencias instaladas
    )
) else (
    echo ⚠️  Pulando: Ambiente virtual nao existe
)
echo.

:: ===============================================
:: 8. VERIFICAR CONEXAO COM INTERNET
:: ===============================================
echo [8/9] Verificando conexao com internet...
ping -n 1 8.8.8.8 >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ⚠️  AVISO: Sem conexao com internet
    echo    Necessaria apenas para instalacao inicial
    set /a AVISOS+=1
) else (
    echo ✅ OK: Conexao com internet disponivel
)
echo.

:: ===============================================
:: 9. VERIFICAR ARQUIVO .ENV
:: ===============================================
echo [9/9] Verificando arquivo de configuracao...
if exist ".env" (
    echo ✅ OK: Arquivo .env encontrado
) else (
    echo ⚠️  AVISO: Arquivo .env nao encontrado
    echo    Crie um arquivo .env se precisar de credenciais
    echo    Copie de: env.example
    set /a AVISOS+=1
)
echo.

:: ===============================================
:: RESUMO FINAL
:: ===============================================
echo ===============================================
echo    RESUMO DO DIAGNOSTICO
echo ===============================================
echo.
echo Total de erros: %ERROS%
echo Total de avisos: %AVISOS%
echo.

if %ERROS% EQU 0 (
    if %AVISOS% EQU 0 (
        echo ✅ PERFEITO: Sistema pronto para uso!
        echo.
        echo Proximos passos:
        echo 1. Execute: run_pipeline.bat
        echo 2. Selecione a opcao desejada
    ) else (
        echo ⚠️  ATENCAO: Sistema funcional mas com avisos
        echo.
        echo O sistema pode funcionar, mas recomendamos
        echo corrigir os avisos acima para melhor desempenho.
    )
) else (
    echo ❌ ERRO: Sistema nao esta pronto
    echo.
    echo Corrija os %ERROS% erro(s) acima antes de continuar.
    echo.
    echo Comandos uteis:
    echo - setup_project.bat  : Configura o ambiente
    echo - INSTALACAO.md      : Guia completo de instalacao
)
echo.
echo ===============================================
echo.
echo Pressione qualquer tecla para sair...
pause >nul

exit /b 0
