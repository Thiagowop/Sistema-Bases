# 📚 Documentação do Projeto - Índice Enxuto

Este índice reúne apenas os artefatos essenciais para operar, manter e evoluir o pipeline VIC/MAX.

## 📖 Para Usuários Finais

### [INSTALACAO.md](INSTALACAO.md)
Guia completo de instalação e configuração inicial do ambiente.

### [GUIA_RUN_COMPLETO_V2.md](GUIA_RUN_COMPLETO_V2.md)
Passo a passo visual do fluxo híbrido recomendado (v2.0).

### [COMPARACAO_RUN_COMPLETO.md](COMPARACAO_RUN_COMPLETO.md)
Resumo das diferenças entre as execuções v1.0 e v2.0 e quando utilizar cada uma.

### [RESUMO_EXECUTIVO.md](RESUMO_EXECUTIVO.md)
Visão executiva com principais métricas, ganhos de performance e destaques de entrega.

---

## 🔧 Para Desenvolvedores e TI

### [PORTABILIDADE.md](PORTABILIDADE.md)
Checklist técnico para garantir execução consistente em qualquer ambiente.

### [PORTABILIDADE_RUN_COMPLETO_V2.md](PORTABILIDADE_RUN_COMPLETO_V2.md)
Validação detalhada da portabilidade específica do fluxo híbrido v2.0.

### [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
Visão macro da arquitetura, integrações e responsabilidades de cada módulo.

### [FLUXO.md](FLUXO.md)
Descrição detalhada das etapas do pipeline e dependências entre processadores.

### [LOGS_SPECIFICATION.md](LOGS_SPECIFICATION.md)
Guia rápido para interpretar logs e diagnosticar execuções.

---

## 🧠 Processadores

- [VIC_PROCESSOR.md](VIC_PROCESSOR.md) — Regras e tratamento da base VIC.
- [MAX_PROCESSOR.md](MAX_PROCESSOR.md) — Limpeza e validações da base MAX.
- [DEVOLUCAO_PROCESSOR.md](DEVOLUCAO_PROCESSOR.md) — Lógica da etapa de devolução.
- [BATIMENTO_PROCESSOR.md](BATIMENTO_PROCESSOR.md) — Alinhamento final VIC × MAX.

---

## 📎 Recursos Complementares

- [INDICE_DOCUMENTACAO.md](INDICE_DOCUMENTACAO.md) — Navegação estendida com filtros por perfil.
- [visualizar_pr_localmente.md](visualizar_pr_localmente.md) — Como testar visualmente Pull Requests.

---

## 🗂️ Estrutura Atual da Documentação

```
docs/
├── 📖 INSTALACAO.md
├── 🚀 GUIA_RUN_COMPLETO_V2.md
├── 📊 COMPARACAO_RUN_COMPLETO.md
├── 📌 RESUMO_EXECUTIVO.md
├── 🔧 PORTABILIDADE.md
├── 🔒 PORTABILIDADE_RUN_COMPLETO_V2.md
├── 🏗️ ARCHITECTURE_OVERVIEW.md
├── 🔄 FLUXO.md
├── 🧠 Processadores
│   ├── VIC_PROCESSOR.md
│   ├── MAX_PROCESSOR.md
│   ├── DEVOLUCAO_PROCESSOR.md
│   └── BATIMENTO_PROCESSOR.md
├── 🧾 LOGS_SPECIFICATION.md
├── 📚 INDICE_DOCUMENTACAO.md
└── 🛠️ visualizar_pr_localmente.md
```

---

## 🚀 Início Rápido

1. **Instalar e configurar?** → [INSTALACAO.md](INSTALACAO.md)
2. **Executar o fluxo recomendado?** → [GUIA_RUN_COMPLETO_V2.md](GUIA_RUN_COMPLETO_V2.md)
3. **Entender diferenças entre versões?** → [COMPARACAO_RUN_COMPLETO.md](COMPARACAO_RUN_COMPLETO.md)
4. **Precisa de arquitetura ou logs?** → [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md) e [LOGS_SPECIFICATION.md](LOGS_SPECIFICATION.md)

---

## 📞 Onde Encontrar Informações

| Preciso de... | Consulte... |
|---------------|-------------|
| Instalar o sistema | [INSTALACAO.md](INSTALACAO.md) |
| Operar o pipeline híbrido | [GUIA_RUN_COMPLETO_V2.md](GUIA_RUN_COMPLETO_V2.md) |
| Comparar versões 1.0 vs 2.0 | [COMPARACAO_RUN_COMPLETO.md](COMPARACAO_RUN_COMPLETO.md) |
| Garantir portabilidade | [PORTABILIDADE.md](PORTABILIDADE.md) |
| Verificar portabilidade do fluxo v2.0 | [PORTABILIDADE_RUN_COMPLETO_V2.md](PORTABILIDADE_RUN_COMPLETO_V2.md) |
| Entender arquitetura e dependências | [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md) |
| Investigar processadores específicos | Arquivos `*_PROCESSOR.md` |
| Interpretar logs | [LOGS_SPECIFICATION.md](LOGS_SPECIFICATION.md) |

---

**Última atualização:** Outubro 2025  
**Versão da documentação:** 2.0
