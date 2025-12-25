@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "PROJECT_ROOT=%SCRIPT_DIR%"
set "VENV_DIR=%PROJECT_ROOT%\.venv"
set "VENV_PYTHON=%VENV_DIR%\Scripts\python.exe"
set "VENV_ACTIVATE=%VENV_DIR%\Scripts\activate.bat"
set "VENV_DEACTIVATE=%VENV_DIR%\Scripts\deactivate.bat"
set "VENV_PIP=%VENV_DIR%\Scripts\pip.exe"
set "REQUIREMENTS=%PROJECT_ROOT%\requirements.txt"
set "PYTHONPATH=%PROJECT_ROOT%"

if not exist "%VENV_PYTHON%" (
    call :SETUP_ENVIRONMENT NOPAUSE
)

if not exist "%VENV_PYTHON%" (
    echo ERRO: Ambiente virtual nao encontrado.
    echo Execute "run_tabelionato.bat 1" para preparar o ambiente antes de executar o fluxo completo.
    set "EXIT_CODE=201"
    goto END
)

call "%VENV_ACTIVATE%" >nul
call :PREPARAR_FERRAMENTAS
python "%SCRIPT_DIR%scripts\fluxo_completo.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
if exist "%VENV_DEACTIVATE%" call "%VENV_DEACTIVATE%" >nul 2>&1
goto END

:SETUP_ENVIRONMENT
echo =================================
echo  CONFIGURANDO O AMBIENTE
echo =================================

python --version
if errorlevel 1 (
    echo [ERRO] Python nao encontrado.
    echo Instale o Python 3.8 ou superior:
    echo https://www.python.org/downloads/
    set "SETUP_ERR=1"
    goto SETUP_END
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
    goto SETUP_END
)

echo.
echo 3. Instalando dependencias...
set "SETUP_ERR=0"
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

:SETUP_PREPARE_BIN
echo.
echo 4. Preparando ferramentas (7-Zip)...
set "BIN_DIR=%PROJECT_ROOT%\bin"
set "SEVEN_ZIP_MAIN=%BIN_DIR%\7z.exe"
set "SEVEN_ZIP_ALT1=%BIN_DIR%\7_zip_rar\7z.exe"
set "SEVEN_ZIP_ALT2=%BIN_DIR%\7_zip_rar\7-Zip\7z.exe"
if exist "%SEVEN_ZIP_MAIN%" (
    echo 7-Zip presente: "%SEVEN_ZIP_MAIN%"
    echo sim
) else (
    if exist "%SEVEN_ZIP_ALT1%" (
        echo 7-Zip presente: "%SEVEN_ZIP_ALT1%"
    ) else (
        if exist "%SEVEN_ZIP_ALT2%" (
            echo 7-Zip presente: "%SEVEN_ZIP_ALT2%"
        ) else (
            if exist "%BIN_DIR%\7_zip_rar.zip" (
                echo Descompactando pacote: "%BIN_DIR%\7_zip_rar.zip"
                powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Force -Path '%BIN_DIR%\7_zip_rar.zip' -DestinationPath '%BIN_DIR%'"
                if errorlevel 1 (
                    echo [ERRO] Falha ao descompactar 7_zip_rar.zip
                    set "SETUP_ERR=1"
                ) else (
                    echo 7-Zip descompactado em: "%BIN_DIR%\7_zip_rar"
                )
            ) else (
                echo [AVISO] 7-Zip ausente em "%BIN_DIR%" e nenhum "7_zip_rar.zip" encontrado.
                echo Instale o 7-Zip no sistema ou disponibilize "bin\7_zip_rar.zip".
            )
        )
    )
)

echo.
echo 5. Preparando ferramentas (Python portatil)...
set "PY_BIN_DIR=%BIN_DIR%\python"
set "PY_ZIP=%BIN_DIR%\python_portable.zip"
if not exist "%PY_BIN_DIR%\python.exe" (
    if exist "%PY_ZIP%" (
        echo Descompactando pacote: %PY_ZIP%
        powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Force -Path '%PY_ZIP%' -DestinationPath '%BIN_DIR%'"
        if errorlevel 1 (
            echo [AVISO] Falha ao descompactar python_portable.zip
        ) else (
            echo Python portatil descompactado em: %PY_BIN_DIR%
        )
    ) else (
        echo [AVISO] Python portatil ausente e nenhum 'python_portable.zip' encontrado.
        echo O projeto usa o Python do ambiente virtual .venv. Esta etapa e opcional.
    )
) else (
    echo Python portatil presente: %PY_BIN_DIR%\python.exe
)

:SETUP_END
if exist "%VENV_DEACTIVATE%" call "%VENV_DEACTIVATE%" >nul 2>&1
if "%~1"=="INTERACTIVE" (
    echo.
    pause
)
exit /b %SETUP_ERR%

:PREPARAR_FERRAMENTAS
echo.
echo Preparando ferramentas (7-Zip)...
set "BIN_DIR=%PROJECT_ROOT%\bin"
set "SEVEN_ZIP_MAIN=%BIN_DIR%\7z.exe"
set "SEVEN_ZIP_ALT1=%BIN_DIR%\7_zip_rar\7z.exe"
set "SEVEN_ZIP_ALT2=%BIN_DIR%\7_zip_rar\7-Zip\7z.exe"
if exist "%SEVEN_ZIP_MAIN%" (
    echo 7-Zip presente: "%SEVEN_ZIP_MAIN%"
) else (
    if exist "%SEVEN_ZIP_ALT1%" (
        echo 7-Zip presente: "%SEVEN_ZIP_ALT1%"
    ) else (
        if exist "%SEVEN_ZIP_ALT2%" (
            echo 7-Zip presente: "%SEVEN_ZIP_ALT2%"
        ) else (
            if exist "%BIN_DIR%\7_zip_rar.zip" (
                echo Descompactando pacote: "%BIN_DIR%\7_zip_rar.zip"
                powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Force -Path '%BIN_DIR%\7_zip_rar.zip' -DestinationPath '%BIN_DIR%'"
                if errorlevel 1 (
                    echo [ERRO] Falha ao descompactar 7_zip_rar.zip
                ) else (
                    echo 7-Zip descompactado em: "%BIN_DIR%\7_zip_rar"
                )
            ) else (
                echo [AVISO] 7-Zip ausente em "%BIN_DIR%" e nenhum "7_zip_rar.zip" encontrado.
            )
        )
    )
)
exit /b 0

:END
endlocal & exit /b %EXIT_CODE%
