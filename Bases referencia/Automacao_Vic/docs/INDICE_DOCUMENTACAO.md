# 📚 Índice Completo da Documentação - Pipeline VIC/MAX v2.0

> **Versão:** 2.0 | **Última Atualização:** 03/10/2025

---

## 🎯 Início Rápido

| Documento | Descrição | Público |
|-----------|-----------|---------|
| **[README.md](../README.md)** | 📖 Documentação principal do projeto | Todos |
| **[INSTALACAO.md](INSTALACAO.md)** | 🔧 Guia completo de instalação | Usuário final |
| **[GUIA_RUN_COMPLETO_V2.md](GUIA_RUN_COMPLETO_V2.md)** | 🚀 Guia visual do fluxo híbrido v2.0 | Operação |
| **[RESUMO_EXECUTIVO.md](RESUMO_EXECUTIVO.md)** | 🧭 Destaques do projeto e ganhos de entrega | Liderança |

---

## 📋 Documentação de Usuário

### Guias de Instalação e Configuração

| Documento | Conteúdo |
|-----------|----------|
| [INSTALACAO.md](INSTALACAO.md) | Setup inicial, Python, venv, dependências, credenciais |
| [PORTABILIDADE.md](PORTABILIDADE.md) | Checklist de portabilidade e boas práticas de distribuição |

### Guias de Uso

| Documento | Conteúdo |
|-----------|----------|
| [GUIA_RUN_COMPLETO_V2.md](GUIA_RUN_COMPLETO_V2.md) | Fluxo híbrido completo com exemplos visuais |
| [COMPARACAO_RUN_COMPLETO.md](COMPARACAO_RUN_COMPLETO.md) | Diferenças entre v1.0 e v2.0, quando usar cada um |

---

## 🏗️ Documentação Técnica

### Arquitetura e Fluxo

| Documento | Conteúdo |
|-----------|----------|
| [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md) | Visão geral da arquitetura, componentes, integrações |
| [FLUXO.md](FLUXO.md) | Fluxo completo do pipeline, dependências entre etapas |
| [LOGS_SPECIFICATION.md](LOGS_SPECIFICATION.md) | Especificação de logs, formato, estrutura |

### Processadores

| Documento | Conteúdo |
|-----------|----------|
| [VIC_PROCESSOR.md](VIC_PROCESSOR.md) | Extração, filtros, validações e normalização da base VIC |
| [MAX_PROCESSOR.md](MAX_PROCESSOR.md) | Tratamento da base MAX, validação de parcelas e chaves |
| [DEVOLUCAO_PROCESSOR.md](DEVOLUCAO_PROCESSOR.md) | Cruzamento VIC×MAX, regras de negócio |
| [BATIMENTO_PROCESSOR.md](BATIMENTO_PROCESSOR.md) | Auditoria final, divergências, relatórios |
| [VALIDACAO_TAMANHO_VIC.md](VALIDACAO_TAMANHO_VIC.md) | 🆕 Validação automática de tamanho de arquivos VIC |

### Portabilidade

| Documento | Conteúdo |
|-----------|----------|
| [PORTABILIDADE_RUN_COMPLETO_V2.md](PORTABILIDADE_RUN_COMPLETO_V2.md) | Certificado de portabilidade e validações automáticas |

---

## 📊 Por Perfil de Usuário

### 👤 Usuário Final (Operação)

1. **Primeiro uso:**
   - [INSTALACAO.md](INSTALACAO.md)
   - [README.md](../README.md) - Setup rápido (4 comandos)
2. **Uso diário:**
   - [GUIA_RUN_COMPLETO_V2.md](GUIA_RUN_COMPLETO_V2.md)
   - [COMPARACAO_RUN_COMPLETO.md](COMPARACAO_RUN_COMPLETO.md)
3. **Problemas:**
   - [PORTABILIDADE.md](PORTABILIDADE.md) - Checklist rápido
   - Executar `diagnosticar_ambiente.bat`

### 👨‍💻 Desenvolvedor

1. **Arquitetura:**
   - [ARCHITECTURE_OVERVIEW.md](ARCHITECTURE_OVERVIEW.md)
   - [FLUXO.md](FLUXO.md)
2. **Processadores:**
   - [VIC_PROCESSOR.md](VIC_PROCESSOR.md)
   - [MAX_PROCESSOR.md](MAX_PROCESSOR.md)
   - [DEVOLUCAO_PROCESSOR.md](DEVOLUCAO_PROCESSOR.md)
   - [BATIMENTO_PROCESSOR.md](BATIMENTO_PROCESSOR.md)
3. **Logs e Debug:**
   - [LOGS_SPECIFICATION.md](LOGS_SPECIFICATION.md)

### 🔧 DevOps / QA

1. **Portabilidade geral:**
   - [PORTABILIDADE.md](PORTABILIDADE.md)
2. **Validação da execução híbrida:**
   - [PORTABILIDADE_RUN_COMPLETO_V2.md](PORTABILIDADE_RUN_COMPLETO_V2.md)
3. **Monitoramento:**
   - [LOGS_SPECIFICATION.md](LOGS_SPECIFICATION.md)

---

## 🗺️ Tempo de Leitura Sugerido

| Prioridade | Tempo | Documentos |
|------------|-------|------------|
| ⭐ Alta | 30 min | README geral + INSTALACAO + GUIA_RUN_COMPLETO_V2 |
| ⭐ Média | 45 min | PORTABILIDADE + ARCHITECTURE_OVERVIEW + FLUXO |
| ⭐ Baixa | 60 min | Documentos dos processadores + LOGS_SPECIFICATION |

---

## 📦 Pacotes de Entrega

| Pacote | Conteúdo | Objetivo |
|--------|----------|----------|
| **Operação** | INSTALACAO, GUIA_RUN_COMPLETO_V2, COMPARACAO_RUN_COMPLETO | Execução diária |
| **Técnico** | ARCHITECTURE_OVERVIEW, FLUXO, LOGS_SPECIFICATION | Manutenção e evolução |
| **QA/Portabilidade** | PORTABILIDADE, PORTABILIDADE_RUN_COMPLETO_V2 | Validação cross-ambiente |

---

## 🔁 Como manter a documentação atualizada

1. Atualize sempre `docs/README.md` ao criar/alterar documentos.
2. Garanta que os arquivos principais estejam listados acima.
3. Utilize `.gitkeep` para manter a estrutura de `data/` sem comprometer dados sensíveis.

---

**Responsável:** Equipe VIC/MAX  
**Contato:** squad-vic-max@empresa.com
