# ✅ Checklist de Portabilidade do Projeto

Este documento garante que o projeto funcione em **qualquer máquina e ambiente** sem caminhos fixos ou explícitos.

## 🎯 Princípios de Portabilidade

### ✅ Caminhos Relativos
- **Todos os scripts** usam caminhos relativos ao diretório do projeto
- **Nenhum caminho absoluto** (ex: `C:\Users\Thiago\...`) está hardcoded
- **Variável `SCRIPT_DIR`** é usada nos arquivos `.bat` para garantir execução de qualquer local

### ✅ Scripts de Batch (`.bat`)

#### `run_completo.bat`
```bat
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
```
- Define o diretório do script automaticamente
- Navega para o diretório do projeto
- Usa caminhos relativos: `data\logs\`, `venv\Scripts\`, etc.

#### `run_pipeline.bat`
```bat
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
```
- Define o diretório do script automaticamente
- Navega para o diretório do projeto
- Usa caminhos relativos para todas as operações

#### `setup_project.bat`
```bat
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
```
- Define o diretório do script automaticamente
- Cria ambiente virtual no diretório local (`venv\`)

### ✅ Configurações (`config.yaml`)

Todos os caminhos são relativos ao diretório raiz do projeto:
- `data/input/vic`
- `data/input/max`
- `data/input/judicial`
- `data/output`
- `data/logs`

### ✅ Código Python

- Usa `Path` do módulo `pathlib` para caminhos multiplataforma
- Não contém caminhos absolutos hardcoded
- Lê configurações do `config.yaml` com caminhos relativos

## 📋 Como Testar a Portabilidade

### 1. Teste em Diferentes Locais
```powershell
# Copie o projeto para diferentes diretórios
Copy-Item -Recurse "C:\Projeto" "D:\Teste"
cd D:\Teste
.\setup_project.bat
.\run_pipeline.bat
```

### 2. Teste em Diferentes Máquinas
- Copie a pasta completa do projeto
- Execute `setup_project.bat` para configurar o ambiente
- Execute `run_pipeline.bat` ou `run_completo.bat`

### 3. Teste com Diferentes Usuários
- O projeto não depende do nome do usuário
- O projeto não depende de variáveis de ambiente específicas
- Apenas requer Python 3.8+ instalado no sistema

## 🔍 Verificações de Segurança

### ❌ NÃO FAZER:
```python
# NÃO usar caminhos absolutos
arquivo = "C:\\Users\\Thiago\\Desktop\\arquivo.csv"

# NÃO usar caminhos com usuário hardcoded
caminho = Path("C:/Users/Thiago/projeto")
```

### ✅ FAZER:
```python
# Usar caminhos relativos
arquivo = Path("data/input/arquivo.csv")

# Usar Path para compatibilidade multiplataforma
caminho = Path(__file__).parent / "data" / "input"
```

## 🛠️ Estrutura de Diretórios Portável

```
Trabalho-3/
├── venv/                    # Ambiente virtual (criado localmente)
├── data/                    # Dados (caminhos relativos)
│   ├── input/
│   ├── output/
│   └── logs/
├── src/                     # Código fonte
├── scripts/                 # Scripts auxiliares
├── tests/                   # Testes
├── config.yaml              # Configurações (caminhos relativos)
├── requirements.txt         # Dependências Python
├── setup_project.bat        # Setup com SCRIPT_DIR
├── run_pipeline.bat         # Execução com SCRIPT_DIR
└── run_completo.bat         # Execução completa com SCRIPT_DIR
```

## 📝 Comandos para Instalação em Nova Máquina

```powershell
# 1. Clone ou copie o projeto
git clone <repo> ou Copy-Item <origem> <destino>

# 2. Entre no diretório
cd Trabalho-3

# 3. Configure o ambiente
.\setup_project.bat

# 4. Execute o pipeline
.\run_pipeline.bat
```

## ✅ Status de Portabilidade

- [x] Scripts `.bat` usam `SCRIPT_DIR`
- [x] `config.yaml` usa caminhos relativos
- [x] Código Python usa `Path` e caminhos relativos
- [x] Nenhum caminho absoluto hardcoded
- [x] Nenhum nome de usuário hardcoded
- [x] Compatível com Windows (PowerShell e CMD)
- [x] Instalação automática de dependências
- [x] Criação automática de diretórios necessários

## 🎉 Resultado

O projeto está **100% portável** e pode ser executado em:
- ✅ Qualquer máquina Windows
- ✅ Qualquer diretório (local ou rede)
- ✅ Qualquer usuário
- ✅ Múltiplos ambientes simultaneamente
