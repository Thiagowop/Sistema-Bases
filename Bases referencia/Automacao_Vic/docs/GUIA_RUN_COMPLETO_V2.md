# Guia Rápido: run_completo2.0.bat

## 📋 Fluxo de Execução

```
┌─────────────────────────────────────────────────────────────┐
│  PIPELINE HÍBRIDO VIC/MAX - Versão 2.0 (Solução Final)     │
└─────────────────────────────────────────────────────────────┘

[1/5] SETUP
└─→ Verifica Python
└─→ Cria/ativa venv
└─→ Instala dependências

[2/5] VALIDAÇÃO
└─→ Verifica estrutura de diretórios
└─→ Valida arquivos de entrada (ZIP)

[3/5] PROCESSAMENTO
├─→ [3.1] Processar MAX
│   └─→ max_tratada_TIMESTAMP.zip
│
└─→ [3.2] Processar VIC SEM AGING
    └─→ vic_tratada_TIMESTAMP.zip (SEM FILTRO)

[4/5] OPERAÇÕES
├─→ [4.1] DEVOLUÇÃO
│   ├─→ Usa: VIC SEM AGING (máximo de registros)
│   ├─→ Usa: MAX tratado
│   └─→ Gera: vic_devolucao_TIMESTAMP.zip
│
└─→ [4.2] BATIMENTO
    ├─→ [4.2.1] REPROCESSAR VIC COM AGING
    │   └─→ vic_tratada_TIMESTAMP.zip (FILTRO ≥90 dias)
    │
    └─→ [4.2.2] EXECUTAR BATIMENTO
        ├─→ Usa: VIC COM AGING (separação judicial)
        ├─→ Usa: MAX tratado
        └─→ Gera: vic_batimento_TIMESTAMP.zip

[5/5] FINALIZAÇÃO
└─→ Resumo de execução
└─→ Estatísticas de tempo
└─→ Localização dos arquivos gerados
```

---

## 🎯 Por Que Esta Solução Funciona?

### ❌ Problema Original
```
1. Processar VIC COM AGING   → vic_tratada_14h30.zip
2. Processar VIC SEM AGING   → vic_tratada_14h35.zip
3. Executar Devolução        → Busca "mais recente" = 14h35 ✅
4. Executar Batimento        → Busca "mais recente" = 14h35 ❌ ERRADO!
```

### ✅ Solução Implementada
```
1. Processar VIC SEM AGING   → vic_tratada_14h30.zip
2. Executar Devolução        → Usa vic_tratada_14h30.zip ✅
3. Reprocessar VIC COM AGING → vic_tratada_14h40.zip (NOVO!)
4. Executar Batimento        → Usa vic_tratada_14h40.zip ✅
```

**Princípio:** "Processar → Usar Imediatamente → Repetir"

---

## 📊 Comparação de Resultados

### Devolução
| Item | Descrição |
|------|-----------|
| **Base VIC** | SEM AGING (todos os registros) |
| **Objetivo** | Maximizar devoluções possíveis |
| **Filtro** | Nenhum |
| **Resultado esperado** | ~5.000-10.000 registros |

### Batimento
| Item | Descrição |
|------|-----------|
| **Base VIC** | COM AGING (≥90 dias) |
| **Objetivo** | Separação judicial correta |
| **Filtro** | AGING ≥ 90 dias |
| **Resultado esperado** | ~2.000-4.000 registros judiciais |

---

## 🔧 Arquivos Gerados

### Estrutura de Saída
```
data/output/
├── vic_tratada/
│   ├── vic_tratada_20251003_140000.zip  (SEM AGING)
│   └── vic_tratada_20251003_140800.zip  (COM AGING)
│
├── max_tratada/
│   └── max_tratada_20251003_140300.zip
│
├── devolucao/
│   └── vic_devolucao_20251003_140500.zip
│
└── batimento/
    └── vic_batimento_20251003_141000.zip
```

### Logs
```
data/logs/
└── execucao_completa_v2.log
```

---

## 🚀 Como Executar

### Método 1: Duplo Clique
```
📁 Trabalho-3/
└── run_completo2.0.bat  ← Duplo clique aqui
```

### Método 2: Terminal
```cmd
cd "C:\Users\Thiago\Desktop\Projetos Mcsa\Trabalho-3"
run_completo2.0.bat
```

### Método 3: PowerShell
```powershell
cd "C:\Users\Thiago\Desktop\Projetos Mcsa\Trabalho-3"
.\run_completo2.0.bat
```

---

## 📝 Validação de Sucesso

### Durante a Execução
Observe as mensagens no console:

```
[3.2] Processando VIC (tratamento unico)...
VIC tratado: data\output\vic_tratada\vic_tratada_20251003_140000.zip

[4.1] Executando DEVOLUCAO (usando VIC tratado)...
     Arquivos: VIC=data\output\vic_tratada\vic_tratada_20251003_140000.zip
               MAX=data\output\max_tratada\max_tratada_20251003_140300.zip

[4.2] Executando BATIMENTO...
     Arquivos: VIC=data\output\vic_tratada\vic_tratada_20251003_140000.zip
               MAX=data\output\max_tratada\max_tratada_20251003_140300.zip
```

### Após a Execução
```cmd
dir data\output\devolucao\*.zip /o-d
dir data\output\batimento\*.zip /o-d
```

Deve mostrar arquivos recém-criados.

---

## ⚠️ Troubleshooting

### Erro: "FileNotFoundError"
**Causa:** Arquivos de entrada faltando  
**Solução:** Verifique `data/input/vic/`, `data/input/max/`

### Erro: "Python não encontrado"
**Causa:** Python não instalado ou não no PATH  
**Solução:** Execute `diagnosticar_ambiente.bat`

### Erro: "Falha no tratamento VIC"
**Causa:** Erro de conexão com banco ou arquivo corrompido  
**Solução:** Verifique `data/logs/execucao_completa_v2.log`

---

## 📚 Documentos Relacionados

- [PORTABILIDADE_RUN_COMPLETO_V2.md](PORTABILIDADE_RUN_COMPLETO_V2.md) - Validações automáticas do fluxo híbrido
- [COMPARACAO_RUN_COMPLETO.md](COMPARACAO_RUN_COMPLETO.md) - Comparação v1.0 vs v2.0
- [FLUXO.md](FLUXO.md) - Fluxo completo do pipeline
- [INSTALACAO.md](INSTALACAO.md) - Guia de instalação

---

## 🎓 Lições Aprendidas

### Princípios de Design
1. **Processar Just-in-Time:** Processar dados imediatamente antes de usar
2. **Arquivo Fresco:** Sempre usar o arquivo mais recente (acabou de gerar)
3. **Sem Dependências Temporais:** Não confiar em arquivos antigos
4. **Logging Abundante:** Registrar cada etapa para debugging

### Padrão "Process-Then-Use"
```bat
# ✅ BOM: Processar → Capturar → Usar
main.py --processar
for /f %%f in ('dir /b /o-d arquivo_*.*') do set "VAR=%%f"
main.py --usar "%VAR%"

# ❌ RUIM: Processar tudo → Buscar genérico → Usar
main.py --processar1
main.py --processar2
for /f %%f in ('dir /b /o-d arquivo_*.*') do set "VAR=%%f"
main.py --usar "%VAR%"  # Qual arquivo é esse?
```

---

**Versão:** 2.0 Final  
**Data:** 03/10/2025  
**Status:** ✅ Testado e Funcional
