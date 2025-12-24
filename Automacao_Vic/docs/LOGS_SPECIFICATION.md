# Especificação dos Logs - Pipeline VIC/MAX

## Localização
Todos os logs são armazenados em: `data/logs/`

## Arquivo Principal
**Nome**: `pipeline.log`  
**Formato**: Rotating log (máximo 10MB, 5 backups)  
**Encoding**: UTF-8

## Conteúdo dos Logs

### 1. Formato das Mensagens
```
[YYYY-MM-DD HH:MM:SS] - [MENSAGEM]
```

### 2. Tipos de Informações Registradas

#### A) Início e Fim de Processamento
- Início de cada etapa do pipeline
- Tempo de execução de cada processador
- Status final (sucesso/erro)

#### B) Estatísticas de Dados
- Quantidade de registros carregados
- Quantidade de registros processados
- Quantidade de registros filtrados/removidos
- Quantidade de inconsistências encontradas
- Taxas de batimento entre bases

#### C) Arquivos Processados
- Caminhos dos arquivos de entrada
- Caminhos dos arquivos de saída gerados
- Tamanho dos arquivos processados
- Formatos de arquivo (CSV, ZIP)

#### D) Validações e Inconsistências
- Registros com dados inválidos
- Problemas de formato (datas, CPF/CNPJ, valores)
- Chaves duplicadas ou ausentes
- Violações de regras de negócio

#### E) Erros e Exceções
- Stack traces completos
- Contexto do erro (arquivo, linha, dados)
- Ações de recuperação executadas
- Uso de fallback quando aplicável

### 3. Exemplos de Mensagens de Log

```
2025-01-05 14:30:15 - Iniciando processamento VIC
2025-01-05 14:30:16 - CSV carregado: data/input/vic/VIC_20250105.csv (45,230 registros)
2025-01-05 14:30:18 - VIC após filtro status=EM ABERTO: 42,156 registros
2025-01-05 14:30:19 - VIC após filtro aging>=90: 38,904 registros
2025-01-05 14:30:20 - Inconsistências encontradas: 127 registros
2025-01-05 14:30:21 - Arquivo exportado: data/output/vic_tratada/vic_tratada_20250105_143021.zip
2025-01-05 14:30:21 - Processamento VIC concluído em 6.42s

2025-01-05 14:30:22 - Iniciando batimento VIC-MAX
2025-01-05 14:30:23 - VIC tratado: 38,904 registros
2025-01-05 14:30:23 - MAX tratado: 41,567 registros
2025-01-05 14:30:24 - Parcelas VIC ausentes no MAX: 2,341 (taxa: 6.02%)
2025-01-05 14:30:24 - Batimento concluído em 2.18s

2025-01-05 14:30:25 - ERRO: Falha ao conectar com banco MAX
2025-01-05 14:30:25 - Ativando fallback para sistema atual
```

### 4. Níveis de Log

- **INFO**: Operações normais, estatísticas, arquivos processados
- **WARNING**: Inconsistências de dados, uso de fallback
- **ERROR**: Falhas de processamento, erros de conexão
- **DEBUG**: Detalhes técnicos (apenas em modo debug)

### 5. Rotação de Logs

- Arquivo atual: `pipeline.log`
- Backups: `pipeline.log.1`, `pipeline.log.2`, etc.
- Máximo 5 backups mantidos
- Rotação automática ao atingir 10MB

### 6. Configuração

As configurações de log estão definidas em `config.yaml`:

```yaml
logging:
  level: INFO
  format: "%(asctime)s - %(message)s"
  date_format: "%Y-%m-%d %H:%M:%S"
  file_handler:
    enabled: true
    filename: pipeline.log
```

### 7. Monitoramento

Os logs devem ser monitorados para:
- Identificar padrões de inconsistências
- Acompanhar performance do pipeline
- Detectar problemas recorrentes
- Validar integridade dos dados processados

### 8. Retenção

- Logs são mantidos por tempo indeterminado
- Backup manual recomendado para logs antigos
- Limpeza manual quando necessário