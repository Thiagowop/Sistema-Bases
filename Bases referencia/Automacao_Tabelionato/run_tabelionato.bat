@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "VENV_DIR=%SCRIPT_DIR%\.venv"
set "VENV_PYTHON=%VENV_DIR%\Scripts\python.exe"
set "VENV_ACTIVATE=%VENV_DIR%\Scripts\activate.bat"
set "VENV_DEACTIVATE=%VENV_DIR%\Scripts\deactivate.bat"
set "VENV_PIP=%VENV_DIR%\Scripts\pip.exe"
set "REQUIREMENTS=%SCRIPT_DIR%requirements.txt"

set "choice=%~1"
set "SCRIPT_MODE=INTERACTIVE"
if not "%choice%"=="" set "SCRIPT_MODE=NOPAUSE"
if "%choice%"=="" goto MENU
goto PROCESS_CHOICE

:MENU
cls
echo ===============================================
echo    PIPELINE TABELIONATO - MENU PRINCIPAL
echo ===============================================
echo.
echo 1. Instalar dependencias
echo 2. Executar fluxo completo (extracao ^> tratamento ^> batimento ^> baixa)
echo 3. Extrair base MAX do banco
echo 4. Extrair base a partir do e-mail
echo 5. Processar apenas MAX (tratamento)
echo 6. Processar apenas Tabelionato (tratamento)
echo 7. Processar apenas Batimento
echo 8. Processar apenas Baixa
echo 9. Limpar arquivos de saida
echo 0. Sair
echo.
set /p choice=Digite sua escolha (0-9):

:PROCESS_CHOICE
set "EXIT_CODE=0"
set "HANDLED="

if "%choice%"=="1" (
    call :SETUP_ENVIRONMENT %SCRIPT_MODE%
    set "EXIT_CODE=%ERRORLEVEL%"
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="2" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%SCRIPT_DIR%fluxo_completo.bat"
        set "EXIT_CODE=%ERRORLEVEL%"
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="3" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%VENV_PYTHON%" extracao_base_max_tabelionato.py
        set "EXIT_CODE=%ERRORLEVEL%"
        if not "%EXIT_CODE%"=="0" (
            echo ERRO: Falha na extracao MAX.
        ) else (
            echo Extracao MAX concluida com sucesso!
        )
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="4" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%VENV_PYTHON%" extrair_base_tabelionato.py
        set "EXIT_CODE=%ERRORLEVEL%"
        if not "%EXIT_CODE%"=="0" (
            echo ERRO: Falha na extracao da base de e-mail.
        ) else (
            echo Extracao da base de e-mail concluida com sucesso!
        )
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="5" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%VENV_PYTHON%" tratamento_max.py
        set "EXIT_CODE=%ERRORLEVEL%"
        if not "%EXIT_CODE%"=="0" (
            echo ERRO: Falha no tratamento MAX.
        ) else (
            echo Tratamento MAX concluido com sucesso!
        )
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="6" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%VENV_PYTHON%" tratamento_tabelionato.py
        set "EXIT_CODE=%ERRORLEVEL%"
        if not "%EXIT_CODE%"=="0" (
            echo ERRO: Falha no tratamento Tabelionato.
        ) else (
            echo Tratamento Tabelionato concluido com sucesso!
        )
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="7" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%VENV_PYTHON%" batimento_tabelionato.py
        set "EXIT_CODE=%ERRORLEVEL%"
        if not "%EXIT_CODE%"=="0" (
            echo ERRO: Falha no batimento.
        ) else (
            echo Batimento concluido com sucesso!
        )
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="8" (
    call :REQUIRE_ENV
    if errorlevel 1 (
        set "EXIT_CODE=%ERRORLEVEL%"
    ) else (
        call "%VENV_PYTHON%" baixa_tabelionato.py
        set "EXIT_CODE=%ERRORLEVEL%"
        if not "%EXIT_CODE%"=="0" (
            echo ERRO: Falha na baixa.
        ) else (
            echo Baixa concluida com sucesso!
        )
    )
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="9" (
    call :CLEAN_OUTPUT
    set "HANDLED=1"
    goto ACTION_DONE
)

if "%choice%"=="0" (
    set "EXIT_CODE=0"
    goto FINISH
)

goto INVALID_OPTION

:ACTION_DONE
if "%HANDLED%"=="1" (
    if "%SCRIPT_MODE%"=="INTERACTIVE" (
        echo.
        pause
        set "choice="
        goto MENU
    )
    goto FINISH
)

:INVALID_OPTION
if defined choice (
    echo ERRO: Opcao "%choice%" invalida. Tente novamente.
) else (
    echo Opcao nao informada.
)
timeout /t 2 >nul
set "choice="
if "%SCRIPT_MODE%"=="INTERACTIVE" goto MENU
set "EXIT_CODE=1"
goto FINISH

:CLEAN_OUTPUT
echo Removendo arquivos de saida...
if exist "data\output\max_tratada\*.zip" del /q "data\output\max_tratada\*.zip"
if exist "data\output\tabelionato_tratada\*.zip" del /q "data\output\tabelionato_tratada\*.zip"
if exist "data\output\batimento\*.zip" del /q "data\output\batimento\*.zip"
if exist "data\output\inconsistencias\*.zip" del /q "data\output\inconsistencias\*.zip"
if exist "data\output\baixa\*.zip" del /q "data\output\baixa\*.zip"
if exist "data\logs\*.log" del /q "data\logs\*.log"
echo Limpeza concluida!
set "EXIT_CODE=0"
exit /b 0

:REQUIRE_ENV
if exist "%VENV_PYTHON%" exit /b 0
echo Ambiente virtual nao encontrado. Preparando automaticamente...
call :SETUP_ENVIRONMENT NOPAUSE
if exist "%VENV_PYTHON%" exit /b 0
echo ERRO: Ambiente virtual nao encontrado.
echo Execute a opcao "1. Instalar dependencias" antes de continuar.
exit /b 201

:SETUP_ENVIRONMENT
set "SETUP_ERR=0"
echo =================================
echo  CONFIGURANDO O AMBIENTE
echo =================================

python --version
if errorlevel 1 (
    echo [ERRO] Python nao encontrado.
    echo Instale o Python 3.8 ou superior:
    echo https://www.python.org/downloads/
    set "SETUP_ERR=1"
    goto SETUP_FINISH
)

echo.
echo 1. Criando ambiente virtual...
python -m venv "%VENV_DIR%"

echo.
echo 2. Ativando ambiente virtual...
if exist "%VENV_ACTIVATE%" (
    call "%VENV_ACTIVATE%" >nul
) else (
    echo [ERRO] Script de ativacao nao encontrado em "%VENV_ACTIVATE%".
    set "SETUP_ERR=1"
    goto SETUP_FINISH
)

echo.
echo 3. Instalando dependencias...
if exist "%VENV_PIP%" (
    "%VENV_PIP%" install -r "%REQUIREMENTS%"
    if errorlevel 1 (
        python -m pip install -r "%REQUIREMENTS%"
    )
) else (
    python -m pip install -r "%REQUIREMENTS%"
)
if errorlevel 1 (
    echo [ERRO] Falha na instalacao das dependencias.
    echo Verifique a conexao com a internet e tente novamente.
    set "SETUP_ERR=1"
) else (
    echo Dependencias instaladas com sucesso!
)

:SETUP_FINISH
if exist "%VENV_DEACTIVATE%" call "%VENV_DEACTIVATE%" >nul 2>&1
if "%~1"=="INTERACTIVE" (
    echo.
    pause
)
exit /b %SETUP_ERR%

:FINISH
endlocal & exit /b %EXIT_CODE%
