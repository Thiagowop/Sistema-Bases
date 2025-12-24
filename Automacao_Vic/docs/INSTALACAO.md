# 📦 Guia de Instalação - Pipeline VIC/MAX

## 🎯 Requisitos Mínimos

### Sistema Operacional
- ✅ Windows 10 ou superior
- ✅ Windows Server 2016 ou superior

### Software Necessário
- ✅ **Python 3.8 ou superior** - [Download aqui](https://www.python.org/downloads/)
  - ⚠️ Durante a instalação, marque: **"Add Python to PATH"**
- ✅ Mínimo 500 MB de espaço livre em disco
- ✅ Conexão com a internet (apenas para instalação inicial)

### Permissões
- ✅ Permissões de leitura/escrita no diretório de instalação
- ✅ Permissões para executar scripts (.bat)

---

## 📥 Opções de Instalação

### Opção 1: Download do Repositório GitHub (Recomendado)

#### Se você tem Git instalado:
```bash
# Clone o repositório
git clone https://github.com/Thiagowop/Trabalho.git

# Entre no diretório
cd Trabalho
```

#### Se NÃO tem Git instalado:
1. Acesse: https://github.com/Thiagowop/Trabalho
2. Clique no botão verde **"Code"**
3. Selecione **"Download ZIP"**
4. Extraia o arquivo ZIP para o local desejado
   - ✅ Pode ser: `C:\Projetos\`, `D:\Trabalho\`, drive de rede, etc.
   - ⚠️ Evite caminhos muito longos (>200 caracteres)

### Opção 2: Cópia Manual

Se você recebeu uma cópia do projeto:
1. Copie a pasta completa para o local desejado
2. Certifique-se de copiar **TODA** a estrutura de pastas

---

## 🚀 Instalação e Configuração

### Passo 1: Verificar Python

Abra o **Prompt de Comando** (CMD) ou **PowerShell** e execute:

```powershell
python --version
```

**Resultado esperado:**
```
Python 3.8.x (ou superior)
```

**Se aparecer erro "Python não reconhecido":**
1. Instale o Python: https://www.python.org/downloads/
2. Durante instalação, marque: **"Add Python to PATH"**
3. Reinicie o computador
4. Tente novamente

### Passo 2: Executar Setup do Projeto

#### No Windows Explorer:
1. Navegue até a pasta do projeto
2. Clique duplo em: **`setup_project.bat`**

#### Ou no terminal:
```powershell
cd caminho\para\o\projeto
.\setup_project.bat
```

**O que o setup faz:**
- ✅ Verifica a instalação do Python
- ✅ Cria ambiente virtual isolado (`venv/`)
- ✅ Instala todas as dependências necessárias
- ✅ Cria estrutura de diretórios

**Tempo estimado:** 2-5 minutos

---

## ▶️ Como Usar

### Opção 1: Interface de Menu (Recomendado)

1. Clique duplo em: **`run_pipeline.bat`**
2. Selecione uma opção do menu:

```
===============================================
   SELECIONE UMA OPCAO:
===============================================

1. Executar Pipeline Completo (MAX > VIC > Devolucao > Batimento)
4. Processar apenas MAX (tratamento)
5. Processar apenas VIC (tratamento)
6. Processar apenas Devolucao
7. Processar apenas Batimento
8. Extrair Bases (VIC email, MAX DB, Judicial DB)
9. Ajuda
0. Sair
```

### Opção 2: Execução Automática Completa

Para executar todas as etapas automaticamente:

1. Clique duplo em: **`run_completo.bat`**

**O que faz:**
1. ✅ Configura ambiente (se necessário)
2. ✅ Extrai bases de dados
3. ✅ Executa pipeline completo
4. ✅ Gera relatórios e logs

**Ideal para:** Agendamento no Windows Task Scheduler

---

## 📁 Estrutura de Arquivos

```
Trabalho/
├── 📂 venv/                    # Ambiente virtual (criado automaticamente)
├── 📂 data/                    # Dados de entrada e saída
│   ├── 📂 input/               # Arquivos de entrada
│   │   ├── 📂 vic/             # Dados VIC
│   │   ├── 📂 max/             # Dados MAX
│   │   ├── 📂 judicial/        # Dados Judiciais
│   │   └── 📂 blacklist/       # Lista de exclusão
│   ├── 📂 output/              # Arquivos gerados
│   │   ├── 📂 vic_tratada/     # VIC processado
│   │   ├── 📂 max_tratada/     # MAX processado
│   │   ├── 📂 devolucao/       # Devolução
│   │   └── 📂 batimento/       # Batimento
│   └── 📂 logs/                # Logs de execução
├── 📂 src/                     # Código fonte
├── 📂 scripts/                 # Scripts auxiliares
├── 📂 tests/                   # Testes automatizados
├── 📄 config.yaml              # Configurações
├── 📄 requirements.txt         # Dependências Python
├── 📄 requirements-dev.txt     # Dependências de testes
├── 🚀 setup_project.bat        # Instalação inicial
├── ▶️  run_pipeline.bat        # Execução com menu
└── ▶️  run_completo.bat        # Execução automática
```

---

## 🔧 Configuração

### Arquivo: `config.yaml`

Edite este arquivo para ajustar:

#### Email (Extração VIC)
```yaml
email:
  imap_server: imap.gmail.com
  imap_folder: INBOX
  email_sender: seuemail@exemplo.com
```

#### Banco de Dados (Extração MAX/Judicial)
```yaml
database:
  server: servidor.exemplo.com
  database: nome_banco
  # Credenciais em .env
```

#### Credenciais Sensíveis: `.env`

Crie um arquivo `.env` na raiz do projeto:

```env
# Email
EMAIL_ADDRESS=seu_email@exemplo.com
EMAIL_PASSWORD=sua_senha_app

# Banco de Dados
DB_USER=usuario
DB_PASSWORD=senha
```

⚠️ **IMPORTANTE:** Nunca compartilhe o arquivo `.env`!

---

## 🎬 Primeiros Passos

### 1. Instalação Completa
```powershell
# 1. Extrair/copiar projeto para pasta desejada
# 2. Executar setup
.\setup_project.bat

# 3. Configurar credenciais (se necessário)
# Editar .env com suas credenciais
```

### 2. Teste Básico
```powershell
# Executar menu
.\run_pipeline.bat

# Selecionar opção 9 (Ajuda) para ver instruções
```

### 3. Primeira Execução
```powershell
# Opção A: Com dados já presentes em data/input/
.\run_pipeline.bat
# Selecione opção 1 (Pipeline Completo)

# Opção B: Extrair dados primeiro
.\run_pipeline.bat
# Selecione opção 8 (Extrair Bases)
# Depois selecione opção 1 (Pipeline Completo)
```

---

## ❓ Resolução de Problemas

### Erro: "Python não reconhecido"

**Solução:**
1. Instale Python 3.8+: https://www.python.org/downloads/
2. Marque: "Add Python to PATH" durante instalação
3. Reinicie o computador

### Erro: "Ambiente virtual não encontrado"

**Solução:**
```powershell
# Execute o setup novamente
.\setup_project.bat
```

### Erro: "Permissão negada" ou "Acesso negado"

**Solução:**
- Execute como Administrador (botão direito > "Executar como administrador")
- Ou mova o projeto para uma pasta com permissões de escrita (ex: `C:\Projetos\`)

### Erro: Drive de rede desconectado (G:\Meu)

**Solução:**
- Reconecte o drive de rede
- Ou copie o projeto para um drive local (C:, D:)

### Erro: "Falha na instalação de dependências"

**Solução:**
```powershell
# Instalar manualmente
cd caminho\para\o\projeto
venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 📊 Arquivos de Saída

Após a execução, os arquivos estarão em:

### `data/output/`
- **`vic_tratada/`** - Dados VIC processados (ZIP)
- **`max_tratada/`** - Dados MAX processados (ZIP)
- **`devolucao/`** - Lista de devolução (ZIP)
- **`batimento/`** - Resultado do batimento (ZIP)
  - `judicial/` - Casos judiciais
  - `extrajudicial/` - Casos extrajudiciais

### `data/logs/`
- **`pipeline.log`** - Log detalhado da execução
- **`execucao_completa.log`** - Log da execução automática

---

## 🔄 Atualizações

### Se você clonou com Git:
```bash
git pull origin main
.\setup_project.bat  # Atualizar dependências
```

### Se você baixou ZIP:
1. Baixe a versão mais recente
2. Substitua apenas os arquivos do código fonte
3. **NÃO substitua:** `venv/`, `data/`, `.env`
4. Execute `.\setup_project.bat` novamente

---

## 📞 Suporte

### Documentação Adicional
- `PORTABILIDADE.md` - Informações sobre portabilidade
- `README.md` - Visão geral do projeto
- `docs/` - Documentação técnica detalhada

### Logs para Diagnóstico
Ao reportar problemas, inclua:
- Arquivo: `data/logs/pipeline.log`
- Sistema operacional e versão
- Versão do Python (`python --version`)
- Mensagem de erro completa

---

## ✅ Checklist de Instalação

- [ ] Python 3.8+ instalado
- [ ] Python adicionado ao PATH
- [ ] Projeto extraído/copiado para local desejado
- [ ] `setup_project.bat` executado com sucesso
- [ ] Arquivo `.env` configurado (se necessário)
- [ ] Teste básico realizado com `run_pipeline.bat`
- [ ] Primeira execução bem-sucedida

---

## 🎉 Pronto para Usar!

Após completar a instalação, você pode:
- ✅ Executar o pipeline completo
- ✅ Processar dados VIC e MAX
- ✅ Gerar relatórios de devolução e batimento
- ✅ Agendar execuções automáticas

**Boa sorte! 🚀**
