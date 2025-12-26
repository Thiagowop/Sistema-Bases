# EMCCAMP - Fase 1: Extração

## Objetivo
Extrair dados brutos das fontes EMCCAMP e salvar em `data/input/`.

---

## Fontes de Dados

| Fonte | Tipo | Output |
|-------|------|--------|
| **EMCCAMP Base** | API TOTVS | `data/input/emccamp/Emccamp.zip` |
| **EMCCAMP Baixas** | API TOTVS | `data/input/baixas/baixa_emccamp.zip` |
| **MAX** | SQL Server | `data/input/base_max/MaxSmart.zip` |
| **Judicial** | SQL Server | `data/input/judicial/*.zip` |

---

## 1. Extração EMCCAMP Base (API TOTVS)

### Lógica
1. Conectar na API TOTVS (autenticação básica)
2. Enviar parâmetros de data de vencimento
3. Receber JSON com dados
4. Salvar em `data/input/emccamp/Emccamp.zip`

### Variáveis (.env)
```
EMCCAMP_API_URL=https://totvs.emccamp.com.br:8051/api/.../CANDIOTTO.001/0/X
EMCCAMP_API_USER=****
EMCCAMP_API_PASSWORD=****
EMCCAMP_DATA_VENCIMENTO_INICIAL=2020-01-01
EMCCAMP_DATA_VENCIMENTO_FINAL=AUTO
```

### Parâmetros
- `DATA_VENCIMENTO_INICIAL`: Data inicial para busca
- `DATA_VENCIMENTO_FINAL`: AUTO = hoje - 6 dias

---

## 2. Extração EMCCAMP Baixas (API TOTVS)

### Lógica
1. Conectar na API TOTVS (endpoint diferente: CANDIOTTO.002)
2. Baixar dados de pagamentos
3. Filtrar: HONORARIO_BAIXADO != 0
4. Gerar CHAVE = NUM_VENDA + "-" + ID_PARCELA
5. Salvar em `data/input/baixas/baixa_emccamp.zip`

### Variáveis (.env)
```
TOTVS_BASE_URL=https://totvs.emccamp.com.br:8051
TOTVS_USER=****
TOTVS_PASS=****
```

---

## 3. Extração MAX (SQL Server)

### Lógica
Similar ao VIC MAX - extrai dados do banco SQL Server.

---

## ⚠️ O que NÃO é feito na extração:
- ❌ Filtros de status/aging
- ❌ Validações de CPF
- ❌ Geração de CHAVE (exceto baixas que já vem pronta)

**Isso é feito na FASE 2 (Tratamento).**
