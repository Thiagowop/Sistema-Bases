# Validação de Tamanho de Arquivos - Base VIC

## 📋 Visão Geral

Sistema de validação automática do tamanho de arquivos extraídos por email, projetado para detectar bases da VIC que venham com formato incorreto ou corrompidas.

## 🎯 Problema Identificado

Ocasionalmente, as bases da VIC são recebidas por email com tamanho muito abaixo do esperado, indicando:
- Formato incorreto
- Arquivo corrompido
- Dados incompletos
- Anexo errado enviado

**Exemplo real:**
- ✅ Base normal: ~14 MB (14.698.469 bytes)
- ❌ Base com erro: ~76 KB (76.835 bytes)
- 📊 Diferença: ~99.5% menor que o esperado

## ⚙️ Configuração

### config.yaml

```yaml
email:
  imap_server: imap.gmail.com
  imap_folder: INBOX
  email_sender: noreply@fcleal.com.br
  email_subject_keyword: Candiotto
  attachment_filename: candiotto.zip
  output_filename: VicCandiotto.zip
  download_dir: data/input/vic
  validation:
    min_file_size_mb: 1.0  # Tamanho mínimo em MB
```

### Parâmetros de Validação

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `validation.min_file_size_mb` | float | 0 | Tamanho mínimo esperado em MB. Se 0, validação desabilitada |

**Valor recomendado:** 1.0 MB
- Arquivos válidos (~14 MB) passam ✅
- Arquivos com erro (~0.07 MB) são bloqueados ❌

## 🔍 Funcionamento

### Fluxo de Validação

1. **Download do anexo** - Arquivo é baixado normalmente
2. **Verificação de tamanho** - Compara tamanho com mínimo configurado
3. **Se válido** - Processamento continua normalmente
4. **Se inválido** - Sistema:
   - Exibe mensagem de erro crítico
   - Registra log detalhado
   - Encerra com código de erro (exit 1)
   - Impede processamento de dados incorretos

### Mensagem de Erro

Quando detectado arquivo com tamanho inválido:

```
============================================================
[ERRO CRITICO] BASE COM INCONFORMIDADE DETECTADA
============================================================
[ERRO] O arquivo baixado possui tamanho MUITO ABAIXO do esperado!
[ERRO] Tamanho recebido: 0.07 MB
[ERRO] Tamanho minimo esperado: 1.00 MB
[ERRO] Arquivo: C:\...\data\input\vic\VicCandiotto.zip
[ERRO] 
[ERRO] POSSIVEIS CAUSAS:
[ERRO]   - Base enviada com formato incorreto
[ERRO]   - Arquivo corrompido ou incompleto
[ERRO]   - Email com anexo errado
[ERRO] 
[ERRO] ACAO NECESSARIA:
[ERRO]   - Verificar manualmente o arquivo baixado
[ERRO]   - Contatar o remetente: noreply@fcleal.com.br
[ERRO]   - Solicitar reenvio da base correta
============================================================
```

## 📝 Logs

### Arquivo de Log

**Localização:** `data/logs/extracao_email_erros.log`

**Encoding:** UTF-8 com BOM (`utf-8-sig`)  
**Nota:** Os logs usam caracteres sem acentuação para garantir compatibilidade com terminal Windows

### Formato do Log

```
================================================================================
[2024-12-15 10:30:45] ERRO CRITICO - BASE COM INCONFORMIDADE
================================================================================
Arquivo: C:\...\data\input\vic\VicCandiotto.zip
Tamanho recebido: 0.0733 MB
Tamanho minimo esperado: 1.00 MB
Diferenca: 0.9267 MB abaixo do esperado

Informacoes do e-mail:
  Remetente: noreply@fcleal.com.br
  Assunto: Candiotto - Base Diaria
  Data: Thu, 12 Dec 2024 12:38:00 -0300

Acao necessaria: Verificar arquivo e solicitar reenvio da base
================================================================================
```

## 🧪 Testes

### Teste Automático

Execute o teste de validação:

```bash
python tests/test_validacao_tamanho_vic.py
```

**Saída esperada:**
```
======================================================================
TESTE DE VALIDAÇÃO DE TAMANHO - BASE VIC
======================================================================

[TESTE] Arquivo com ERRO detectado:
        Caminho: C:\...\tests\candiotto (3).zip
        Tamanho: 75.03 KB (0.0733 MB)

[TESTE] Arquivo CORRETO de referência:
        Caminho: C:\...\data\input\vic\VicCandiotto.zip
        Tamanho: 14353.97 KB (14.02 MB)

[ANÁLISE] Comparação:
        Diferença: 13.94 MB
        Arquivo com erro representa 0.52% do tamanho esperado

[VALIDAÇÃO] Teste com tamanho mínimo de 1.0 MB:
        ✓ Arquivo com erro (0.0733 MB) REPROVADO corretamente
        ✓ Arquivo correto (14.02 MB) APROVADO corretamente
```

### Teste Manual

1. Execute a extração de email:
```bash
python scripts/extrair_email.py
```

2. Se o arquivo baixado for inválido, você verá:
   - Mensagem de erro no console
   - Registro em `data/logs/extracao_email_erros.log`
   - Sistema encerrado com código 1

## 🔧 Manutenção

### Ajuste do Limiar

Se necessário ajustar o tamanho mínimo aceito, edite `config.yaml`:

```yaml
email:
  validation:
    min_file_size_mb: 2.0  # Novo valor em MB
```

### Desabilitar Validação

Para desabilitar temporariamente:

```yaml
email:
  validation:
    min_file_size_mb: 0  # Validação desabilitada
```

## 📊 Estatísticas de Uso

### Comparação de Tamanhos

| Tipo de Arquivo | Tamanho Típico | Status |
|-----------------|----------------|--------|
| Base VIC Normal | 10-15 MB | ✅ Válido |
| Base VIC Erro | 50-100 KB | ❌ Inválido |
| Limite Configurado | 1.0 MB | 🔍 Validação |

## 🚨 Troubleshooting

### Problema: Base válida sendo rejeitada

**Solução:** Reduzir `min_file_size_mb` no config.yaml

### Problema: Base inválida não sendo detectada

**Solução:** Aumentar `min_file_size_mb` no config.yaml

### Problema: Log não está sendo gerado

**Verificar:**
1. Permissões de escrita em `data/logs/`
2. Espaço em disco disponível
3. Caminho do arquivo no código

## 📚 Arquivos Relacionados

- **Script:** `scripts/extrair_email.py`
- **Configuração:** `config.yaml`
- **Teste:** `tests/test_validacao_tamanho_vic.py`
- **Log de erros:** `data/logs/extracao_email_erros.log`
- **Exemplo erro:** `tests/candiotto (3).zip` (76 KB)
- **Exemplo válido:** `data/input/vic/VicCandiotto.zip` (14 MB)

## 🎯 Benefícios

1. ✅ **Detecção precoce** de problemas com bases
2. 🚫 **Previne processamento** de dados incorretos
3. 📝 **Rastreabilidade** completa via logs
4. 🔔 **Alertas claros** sobre ações necessárias
5. ⚡ **Automático** - sem intervenção manual necessária

## 📞 Suporte

Em caso de dúvidas sobre a validação:
1. Consultar logs em `data/logs/extracao_email_erros.log`
2. Executar teste: `python tests/test_validacao_tamanho_vic.py`
3. Verificar configuração em `config.yaml`
