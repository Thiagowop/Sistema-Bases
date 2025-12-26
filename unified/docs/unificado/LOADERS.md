# Sistema Unificado - Loaders

## Visão Geral

O sistema unificado utiliza **Loaders** para extrair dados de diferentes fontes. Cada loader implementa a mesma interface mas com lógica específica para cada tipo de fonte.

---

## 1. EmailLoader

### Lógica de Busca

```
┌─────────────────────────────────────────────────────────────┐
│  1. CONECTAR ao servidor IMAP                               │
│     └── server: imap.gmail.com, port: 993 (SSL)            │
├─────────────────────────────────────────────────────────────┤
│  2. BUSCAR emails com critérios:                            │
│     ├── FROM: remetente específico                          │
│     ├── SUBJECT: palavra-chave no assunto                   │
│     └── SINCE: data mínima (N dias atrás)                   │
├─────────────────────────────────────────────────────────────┤
│  3. ORDENAR resultados por ID (decrescente)                 │
│     └── IDs maiores = emails mais recentes                  │
├─────────────────────────────────────────────────────────────┤
│  4. SELECIONAR o email MAIS RECENTE (último ID)             │
│     └── Garante sempre a base mais atualizada               │
├─────────────────────────────────────────────────────────────┤
│  5. EXTRAIR anexo que corresponde ao padrão                 │
│     └── Ex: "candiotto.zip", "*.zip"                        │
├─────────────────────────────────────────────────────────────┤
│  6. DESCOMPACTAR e CARREGAR dados                           │
│     └── Suporta: ZIP, CSV, Excel                            │
└─────────────────────────────────────────────────────────────┘
```

### Por que sempre o mais recente?

O sistema **sempre busca o email mais recente** porque:
1. Emails são enviados periodicamente (diário/semanal/mensal)
2. A base mais recente é a mais completa e atualizada
3. Processar bases antigas pode resultar em dados inconsistentes

### Código do EmailLoader

```python
# Buscar emails
status, message_ids = mail.search(None, search_criteria)

# Pegar o mais recente (está no final da lista)
ids = message_ids[0].split()
latest_id = ids[-1]  # <-- SEMPRE O ÚLTIMO = MAIS RECENTE

# Baixar o email
status, msg_data = mail.fetch(latest_id, "(RFC822)")
```

### Parâmetros de Configuração

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `server` | Servidor IMAP | `imap.gmail.com` |
| `email` | Endereço de email | `user@empresa.com` |
| `password` | Senha de app | `xxxx xxxx xxxx` |
| `sender_filter` | Filtro por remetente | `noreply@exemplo.com` |
| `subject_filter` | Filtro por assunto | `Candiotto` |
| `attachment_pattern` | Padrão do anexo | `candiotto.zip` |
| `days_back` | Dias para buscar | `7` |

---

## 2. SQLLoader

### Lógica de Funcionamento

```
┌─────────────────────────────────────────────────────────────┐
│  1. CONECTAR ao SQL Server                                   │
│     └── Tenta pyodbc, depois pymssql                        │
├─────────────────────────────────────────────────────────────┤
│  2. EXECUTAR query SQL                                       │
│     └── Query completa definida na configuração             │
├─────────────────────────────────────────────────────────────┤
│  3. CARREGAR resultados em DataFrame                         │
│     └── Todos os registros retornados                       │
├─────────────────────────────────────────────────────────────┤
│  4. NORMALIZAR colunas                                       │
│     └── Nomes em MAIÚSCULO, sem espaços                     │
└─────────────────────────────────────────────────────────────┘
```

### Parâmetros de Configuração

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `server` | Servidor SQL | `192.168.1.8\STD2016` |
| `database` | Nome do banco | `Candiotto_std` |
| `username` | Usuário | `Rodrigo` |
| `password` | Senha | `****` |
| `query` | Consulta SQL | `SELECT * FROM ...` |

---

## 3. FileLoader

### Lógica de Funcionamento

```
┌─────────────────────────────────────────────────────────────┐
│  1. VERIFICAR tipo de arquivo                                │
│     └── .csv, .zip, .xlsx, .xls                             │
├─────────────────────────────────────────────────────────────┤
│  2. SE ZIP:                                                  │
│     ├── Verificar se tem senha                               │
│     ├── Tentar extrair com pyzipper/7-zip/unzip             │
│     └── Carregar CSV/Excel interno                          │
├─────────────────────────────────────────────────────────────┤
│  3. CARREGAR dados em DataFrame                              │
│     └── Respeita encoding e separator configurados          │
└─────────────────────────────────────────────────────────────┘
```

### Parâmetros de Configuração

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `path` | Caminho do arquivo | `data/input/base.zip` |
| `password` | Senha do ZIP | `Mf4tab@` |
| `encoding` | Codificação | `utf-8-sig` |
| `separator` | Separador CSV | `;` |

---

## 4. APILoader (EMCCAMP)

### Lógica de Funcionamento

```
┌─────────────────────────────────────────────────────────────┐
│  1. AUTENTICAR na API TOTVS                                  │
│     └── Basic Auth: user/password                           │
├─────────────────────────────────────────────────────────────┤
│  2. ENVIAR request com parâmetros                            │
│     └── Datas de vencimento inicial/final                   │
├─────────────────────────────────────────────────────────────┤
│  3. PROCESSAR resposta JSON                                  │
│     └── Converter para DataFrame                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Resumo: Fluxo de Extração

```
                    ┌─────────────┐
                    │   .env      │
                    │ (credenciais)│
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  Email   │ │   SQL    │ │   File   │
        │  Loader  │ │  Loader  │ │  Loader  │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │            │            │
             ▼            ▼            ▼
        ┌─────────────────────────────────┐
        │         DataFrame               │
        │   (dados normalizados)          │
        └─────────────────────────────────┘
```
