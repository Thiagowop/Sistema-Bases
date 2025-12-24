# Devolução EMCCAMP - Documentação

## 📋 Visão Geral

A funcionalidade de **Devolução** foi implementada no projeto EMCCAMP seguindo o padrão estabelecido no projeto VIC. O módulo identifica títulos presentes no MAX que não existem mais no EMCCAMP (anti-join: MAX − EMCCAMP) e gera arquivos formatados para devolução ao sistema de cobrança.

## 🎯 Objetivo

Identificar parcelas/títulos que estão no sistema de cobrança (MAX) mas que não existem mais na base do credor (EMCCAMP), gerando arquivos de devolução no layout universal.

## 📊 Lógica de Processamento

```
MAX (tratado) − EMCCAMP (tratado) = Títulos para devolver
```

### Fluxo de Execução

1. **Carregamento das bases**
   - EMCCAMP tratado (mais recente)
   - MAX tratado (mais recente)

2. **Aplicação de filtros**
   - **MAX**: Status em aberto (configurável)
   - **EMCCAMP**: Status em aberto (configurável)
   - Filtro de campanha EMCCAMP (opcional)

3. **Anti-join (PROCV)**
   - Identifica registros do MAX ausentes no EMCCAMP
   - Usa coluna `CHAVE` para o join

4. **Remoção por baixa**
   - Remove registros presentes no arquivo de baixas (opcional)
   - Evita devolver títulos já baixados

5. **Divisão de carteiras**
   - **Judicial**: CPFs presentes em `ClientesJudiciais.zip`
   - **Extrajudicial**: Demais registros

6. **Formatação do layout**
   - Layout universal compatível com VIC e Tabelionato
   - 9 colunas padronizadas

7. **Exportação**
   - Arquivo ZIP contendo CSVs separados
   - Separador: `;` (ponto e vírgula)

## 📁 Arquivos Criados/Modificados

### Arquivos Criados

1. **`src/utils/helpers.py`**
   - Funções auxiliares reutilizáveis
   - `primeiro_valor()`: Extrai primeiro valor não-nulo de uma Series
   - `normalizar_data_string()`: Normaliza datas para formato DD/MM/YYYY
   - `extrair_data_referencia()`: Extrai data base de um DataFrame

2. **`src/processors/devolucao.py`**
   - Processador principal de devolução
   - Classe `DevolucaoProcessor`
   - Método `process()` executa pipeline completo
   - ~550 linhas de código

### Arquivos Modificados

3. **`src/config/config.yaml`**
   - Seção `devolucao` adicionada
   - Configurações de filtros, chaves e exportação

4. **`src/pipeline.py`**
   - Import do módulo `devolucao`
   - Método `devolucao()` adicionado

5. **`main.py`**
   - Comando CLI `devolucao` adicionado

6. **`run_pipeline_emccamp.bat`**
   - Opção 7: Executar somente Devolução
   - Integração no pipeline completo (opção 1)
   - Integração no pipeline sem extração (opção 3)

7. **`run_completo_emccamp.bat`**
   - Passo 7/8: Devolução integrada no fluxo completo

## ⚙️ Configuração (config.yaml)

```yaml
devolucao:
  # Filtro de campanha (vazio = aceita todas)
  campanha_termo: "EMCCAMP"
  
  # Status a excluir do MAX
  status_excluir: []
  
  # Colunas chave para join
  chaves:
    emccamp: CHAVE
    max: CHAVE
  
  # Filtros para aplicar no MAX
  filtros_max:
    status_em_aberto: true
  
  # Filtros para aplicar no EMCCAMP
  filtros_emccamp:
      status_em_aberto: true
  
  # Configuração de exportação
  export:
    filename_prefix: "emccamp_devolucao"
    subdir: "devolucao"
    add_timestamp: true
    gerar_geral: true
  
  # Status fixo para devolução
  status_devolucao_fixo: "98"
  
  # Remoção por arquivo de baixa
  remover_por_baixa: true
```

## 🚀 Como Usar

### Comando Direto

```bash
python main.py devolucao
```

### Pipeline Interativo

```bash
run_pipeline_emccamp.bat
# Selecione opção 7 (Executar somente Devolução)
```

### Pipeline Completo

```bash
run_completo_emccamp.bat
# Executa: Extração > Tratamento > Batimento > Baixa > Devolução > Enriquecimento
```

### Pipeline Interativo Completo

```bash
run_pipeline_emccamp.bat
# Selecione opção 1 (Pipeline completo)
```

## 📤 Layout de Saída

### Estrutura do ZIP

```
emccamp_devolucao_YYYYMMDD_HHMMSS.zip
├── emccamp_devolucao.csv              (geral - todos os registros)
├── emccamp_devolucao_jud.csv          (apenas judicial)
└── emccamp_devolucao_extra.csv        (apenas extrajudicial)
```

### Colunas do CSV

| Coluna | Descrição | Exemplo |
|--------|-----------|---------|
| CNPJ CREDOR | CNPJ da empresa (config) | `19.403.252/0001-90` |
| CPFCNPJ CLIENTE | CPF/CNPJ do cliente | `202.745.347-46` |
| NOME / RAZAO SOCIAL | Nome do cliente | `Alex Lopes Pinheiro Junior` |
| PARCELA | Chave da parcela (MAX) | `33808-17742` |
| VENCIMENTO | Data de vencimento | `22/11/2022` |
| VALOR | Valor da parcela | `94,12` |
| TIPO PARCELA | Tipo da parcela | `ADIANTAMENTO ESCRITURA/ITBI` |
| DATA DEVOLUCAO | Data de processamento | `2025-12-16` |
| STATUS | Status fixo (98) | `98` |

### Exemplo de Registro

```csv
CNPJ CREDOR;CPFCNPJ CLIENTE;NOME / RAZAO SOCIAL;PARCELA;VENCIMENTO;VALOR;TIPO PARCELA;DATA DEVOLUCAO;STATUS
19.403.252/0001-90;202.745.347-46;Alex Lopes Pinheiro Junior;33808-17742;22/11/2022;94,12;ADIANTAMENTO ESCRITURA/ITBI;2025-12-16;98
```

## 📊 Métricas Geradas

O processador gera estatísticas detalhadas:

```python
{
    "emccamp_registros_iniciais": 15553,
    "emccamp_apos_filtros": 15553,
    "max_registros_iniciais": 36856,
    "max_apos_filtros": 17332,
    "registros_devolucao_bruto": 2232,
    "removidos_por_baixa": 0,
    "registros_devolucao": 2232,
    "judicial": 0,
    "extrajudicial": 2232,
    "arquivo_zip": "C:/path/to/emccamp_devolucao_20251216_150233.zip",
    "arquivos_no_zip": {
        "arquivo_extrajudicial": "emccamp_devolucao_extra.csv",
        "arquivo_geral": "emccamp_devolucao.csv"
    },
    "duracao": 0.35
}
```

### Output do Console

```
================================================================================
DEVOLUCAO MAX - EMCCAMP
================================================================================

EMCCAMP base recebida: 15.553

MAX base recebida: 36.856
Após filtro STATUS em aberto: 17.332

Registros identificados para devolucao (antes baixa): 2.232
Registros identificados para devolucao (apos baixa): 2.232
Taxa de devolucao: 12.88%

Divisao por carteira:
  Judicial: 0
  Extrajudicial: 2.232

Arquivo exportado: C:\...\emccamp_devolucao_20251216_150233.zip
   Conteudo: emccamp_devolucao_extra.csv, emccamp_devolucao.csv

Duracao: 0.35s
================================================================================
```

## 🔍 Validações Implementadas

1. **Colunas obrigatórias**
   - EMCCAMP: `CHAVE`
   - MAX: `CHAVE`

2. **Arquivos de entrada**
   - Valida existência dos arquivos tratados
   - Usa arquivo mais recente automaticamente

3. **Clientes judiciais**
   - Carrega `ClientesJudiciais.zip` se disponível
   - Separa carteiras corretamente

4. **Arquivo de baixa**
   - Tenta carregar se configurado
   - Continua sem erro se não encontrado

## 🎨 Diferenças vs VIC

| Aspecto | VIC | EMCCAMP |
|---------|-----|---------|
| **Chaves de join** | CHAVE (VIC) vs PARCELA (MAX) | CHAVE (ambos) |
| **Filtros EMCCAMP/VIC** | Tipos, aging, blacklist, status | Apenas status (configurável) |
| **Separador CSV** | `;` | `;` |
| **Encoding** | `utf-8-sig` | `utf-8-sig` |
| **Layout saída** | ✅ Universal | ✅ Universal (idêntico) |
| **Status fixo** | `98` | `98` |

## ⚠️ Observações Importantes

1. **Ordem de execução**: Deve ser executada APÓS tratamento de EMCCAMP e MAX
2. **Arquivo de baixa**: Opcional, mas recomendado para evitar devolver títulos baixados
3. **ClientesJudiciais.zip**: Opcional, todos serão extrajudiciais se não existir
4. **Filtros**: Status em aberto do EMCCAMP está DESABILITADO por padrão (diferente da VIC)
5. **Performance**: ~0.35s para processar 15k EMCCAMP + 36k MAX

## 🐛 Troubleshooting

### Erro: "Coluna CHAVE ausente"
- Verifique se executou o tratamento antes
- Tratamento cria a coluna CHAVE automaticamente

### Erro: "Nenhum arquivo encontrado"
- Execute `python main.py treat emccamp` e `python main.py treat max` primeiro
- Verifique se os arquivos estão em `data/output/emccamp_tratada` e `data/output/max_tratada`

### Arquivo de baixa não encontrado (Warning)
- É apenas um aviso, não impede execução
- Configure `inputs.baixa_emccamp_path` no config.yaml se necessário

### Taxa de devolução muito alta/baixa
- Ajuste filtros em `devolucao.filtros_max` e `devolucao.filtros_emccamp`
- Verifique se as bases estão atualizadas

## 📈 Próximas Melhorias

- [ ] Dashboard de métricas
- [ ] Histórico de devoluções
- [ ] Relatório de divergências
- [ ] Integração com API de cobrança
- [ ] Validação de duplicatas

## ✅ Testes Realizados

- ✅ Comando direto `python main.py devolucao`
- ✅ Pipeline interativo (opção 7)
- ✅ Pipeline completo (com extração)
- ✅ Layout de saída validado
- ✅ Encoding corrigido (Windows)
- ✅ Divisão judicial/extrajudicial
- ✅ Remoção por baixa (quando disponível)

---

**Versão**: 1.0  
**Data**: 16/12/2025  
**Autor**: Sistema de Automação EMCCAMP
