# VIC - Fase 1: Extração

## Objetivo
Extrair dados brutos das fontes e salvar em `data/input/`.

> ⚠️ **IMPORTANTE**: Extração = apenas extrair. Sem filtros, sem validações, sem tratamentos.

---

## Fontes de Dados

| Fonte | Tipo | Script Referência | Output |
|-------|------|-------------------|--------|
| **VIC Base** | Email IMAP | `extrair_email.py` | `data/input/vic/VicCandiotto.zip` |
| **MAX** | SQL Server | `extrair_basemax.py` | `data/input/max/MaxSmart.zip` |
| **Judicial** | SQL Server | `extrair_judicial.py` | `data/input/judicial/ClientesJudiciais.zip` |

---

## 1. Extração Email (VIC Base)

### Lógica
1. Conectar ao Gmail via IMAP
2. Buscar emails com critérios:
   - Remetente: `noreply@fcleal.com.br`
   - Assunto contém: `Candiotto`
3. Pegar o email **mais recente**
4. Baixar anexo `candiotto.zip`
5. Salvar em `data/input/vic/VicCandiotto.zip`

### Por que o mais recente?
- Emails são enviados periodicamente
- A base mais recente é a mais atualizada

### Variáveis (.env)
```
EMAIL_USER=thiago.vitorio@mcsarc.com.br
EMAIL_APP_PASSWORD=****
IMAP_SERVER=imap.gmail.com
VIC_EMAIL_SENDER=noreply@fcleal.com.br
VIC_EMAIL_SUBJECT=Candiotto
VIC_ATTACHMENT_FILENAME=candiotto.zip
```

---

## 2. Extração SQL (MAX)

### Lógica
1. Conectar ao SQL Server
2. Executar query (retorna todos os dados)
3. Salvar em `data/input/max/MaxSmart.zip`

### Query SQL
```sql
SELECT DISTINCT
    dbo.RetornaNomeCampanha(MoCampanhasID,1) AS 'CAMPANHA',
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS 'CPFCNPJ_CLIENTE',
    MoContrato AS 'NUMERO_CONTRATO',
    MoNumeroDocumento AS 'PARCELA',
    MoDataVencimento AS 'VENCIMENTO',
    MoValorDocumento AS 'VALOR',
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS 'STATUS_TITULO'
FROM Movimentacoes
WHERE MoClientesID = 232  -- VIC Engenharia
  AND MoOrigemMovimentacao in ('C', 'I')
```

### Variáveis (.env)
```
MSSQL_SERVER_STD=192.168.1.8\STD2016
MSSQL_DATABASE_STD=Candiotto_std
MSSQL_USER_STD=Rodrigo
MSSQL_PASSWORD_STD=****
```

---

## 3. Extração SQL (Judicial)

### Lógica
1. Conectar ao SQL Server (Candiotto)
2. Executar queries Autojur + MaxSmart
3. Salvar em `data/input/judicial/ClientesJudiciais.zip`

---

## ⚠️ O que NÃO é feito na extração:
- ❌ Filtrar por status (Aberto/Baixado)
- ❌ Calcular aging
- ❌ Gerar CHAVE
- ❌ Validar CPF
- ❌ Remover inconsistências

**Isso é feito na FASE 2 (Tratamento).**
