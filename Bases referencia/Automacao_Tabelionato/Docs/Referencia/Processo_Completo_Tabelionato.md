# Processo Completo — Tabelionato

Este documento descreve o processo completo de extração, tratamento, baixa e batimento dos dados do Tabelionato, incluindo detalhes técunicos, regras de negócio e estrutura do projeto.

## Estrutura do Projeto

```
Tabelionato/
├── data/
│   ├── input/
│   │   ├── max/           # Arquivos da base MAX
│   │   └── tabelionato/   # Arquivos do Tabelionato
│   └── output/
│       ├── batimento/     # Resultados do batimento
│       ├── baixa/        # Arquivos de baixa
│       ├── inconsistencias/ # Registros com problemas
│       ├── max_tratada/   # Base MAX após tratamento
│       └── tabelionato_tratada/ # Base Tabelionato após tratamento
├── Docs/
│   └── Referencia/       # Documentação técnica e regras
├── src/
│   ├── extracao_base_max_tabelionato.py
│   ├── extrair_base_tabelionato.py
│   ├── tratamento_max.py
│   ├── tratamento_tabelionato.py
│   ├── baixa_tabelionato.py
│   └── batimento_tabelionato.py
└── utils/
    └── queries_tabelionato.py  # Queries SQL
```

## Dependências

- Python 3.11 (baixado automaticamente quando `run_tabelionato.bat` for executado com internet)
- pandas>=2.3.1,<2.4.0 (manipulação de dados)
- numpy>=1.26.4,<3.0.0 (computação numérica)
- pyodbc>=4.0.39 (conexão SQL Server)
- python-dotenv>=1.0.0 (variáveis de ambiente)
- py7zr>=0.20.5 (arquivos 7z)
- pyautogui>=0.9.54 (ferramentas auxiliares em `tests/`)

### Preparação de ambiente

- Copie `.env_exemplo` para `.env` antes da primeira execução e preencha com as credenciais de e-mail/SQL utilizadas pelo cliente.
- Os diretórios `data/input/` e `data/output/` permanecem fora do controle de versão (definidos no `.gitignore`) para evitar expor bases sensíveis e resultados volumosos.
- `run_tabelionato.bat 1` cria/atualiza o ambiente virtual `.venv`; todos os .bat e o orquestrador executam seus passos com o interpretador presente nesse diretório.
- Caso o projeto seja aberto com o Python do sistema, `fluxo_completo.py` garante a criação do `.venv` e se reexecuta automaticamente dentro dele antes de iniciar as etapas.

## Fluxo de Execução

### 1. Extração MAX

**Script**: `extracao_base_max_tabelionato.py`
**Objetivo**: Extrair registros "EM ABERTO" do SQL Server MAX.

**Processo**:
1. Conecta ao SQL Server usando credenciais do `.env`
2. Executa query que extrai:
   - PARCELA (chave)
   - VENCIMENTO
   - CPFCNPJ_CLIENTE
   - STATUS
   - VALOR
3. Salva em `data/input/max/MaxSmart_Tabelionato.zip`

### 2. Extração Tabelionato

**Script**: `extrair_base_tabelionato.py`
**Objetivo**: Processar arquivo RAR do Tabelionato.

**Processo**:
1. Extrai RAR usando 7-Zip
2. Processa arquivo TXT/CSV
3. Adiciona "Valor Total Pago"
4. Mantém coluna "Intimado" sem transformações
5. Salva em `data/input/tabelionato/RecebimentoCustas_[DATA].zip`
6. Remove o CSV intermediário, mantendo apenas o ZIP final (o resto do fluxo consome apenas esse arquivo)
7. Remove RAR original

### 3. Tratamento MAX

**Script**: `tratamento_max.py`
**Objetivo**: Padronizar e validar dados do MAX.

**Processo**:
1. Lê `MaxSmart_Tabelionato.zip`
2. Padroniza:
   - PARCELA → CHAVE (limpa espaços)
   - VENCIMENTO → data (%d/%m/%Y)
   - CPFCNPJ_CLIENTE (remove espaços)
3. Valida:
   - Parcela: vazia, vírgula, data, hífen, dígito único
   - Vencimento: vazio, ano < 1900, formato inválido
4. Cria `MOTIVO_INCONSISTENCIA` apenas para registros inválidos
5. Exporta:
   - Válidos: `max_tratada.zip`
   - Inválidos: `max_inconsistencias.zip`

### 4. Tratamento Tabelionato

**Script**: `tratamento_tabelionato.py`
**Objetivo**: Padronizar dados e classificar campanhas.

**Processo**:
1. Lê arquivo ZIP do Tabelionato
2. Padroniza:
   - Separa "DtAnuencia Devedor"
   - Padroniza CPF/CNPJ
   - Protocolo → CHAVE
3. Calcula AGING e classifica campanhas:
   - ≤1800 dias → Campanha 14
   - >1800 dias → Campanha 58
4. Exporta:
   - Válidos: `tabelionato_tratado.zip`
   - Inválidos: `tabelionato_inconsistencias.zip`

### 5. Baixa

**Script**: `baixa_tabelionato.py`
**Objetivo**: Identificar registros para baixa.

**Processo**:
1. Carrega bases tratadas
2. Filtra MAX "EM ABERTO"
3. Compara CHAVEs
4. Identifica registros MAX ausentes no Tabelionato
5. Remove arquivo ZIP anterior de baixa
6. Exporta `baixa_tabelionato_[DATA].zip` e registra pendncias no log

### 6. Batimento

**Script**: `batimento_tabelionato.py`
**Objetivo**: Identificar e tratar registros para cobrança.

**Processo**:
1. Carrega bases tratadas
2. LEFT ANTI-JOIN (Tabelionato → MAX)
3. Trata duplicados:
   - Prioriza CNPJ sobre CPF
   - Escolhe CPF principal
4. Exporta:
   - `batimento_campanha14.zip`
   - `batimento_campanha58.zip`
   - `tabela_enriquecimento.zip`

## Regras de Negócio

### Chaves e Cruzamento
- MAX: CHAVE = PARCELA (limpa)
- Tabelionato: CHAVE = Protocolo
- Baixa: registros em MAX ausentes no Tabelionato
- Batimento: registros em Tabelionato ausentes no MAX

### Classificação de Campanhas
- Campanha 14: AGING ≤ 1800 dias
- Campanha 58: AGING > 1800 dias
- Se CPF/CNPJ tem parcelas em ambas, vai para Campanha 14

### Tratamento de Duplicados
1. Se tem CNPJ + CPF:
   - Prioriza CNPJ
   - CPFs vão para enriquecimento
2. Se tem só CPFs:
   - Escolhe um principal
   - Demais vão para enriquecimento

## Validações e Inconsistências

### MAX
- Parcela vazia/nula
- Parcela com vírgula/hífen
- Parcela com padrão de data
- Parcela com dígito único
- Vencimento vazio/inválido
- Vencimento anterior a 1900
- `MOTIVO_INCONSISTENCIA` criada apenas para registros inválidos

### Tabelionato
- DtAnuencia vazia/inválida
- DtAnuencia anterior a 1900
- Quebras de linha em colunas
- Protocolo ausente
- Coluna "Intimado" mantida sem transformações

## Arquivos de Saída

### Tratamento
- `max_tratada.zip`: registros MAX válidos
- `max_inconsistencias.zip`: registros MAX inválidos (com MOTIVO_INCONSISTENCIA)
- `tabelionato_tratado.zip`: registros Tabelionato válidos
- `tabelionato_inconsistencias.zip`: registros Tabelionato inválidos

### Baixa
- `baixa_tabelionato_[DATA].zip`: registros para baixa (remove ZIP anterior; pendncias ficam apenas registradas em log)

### Batimento
- `batimento_campanha14.zip`: registros Campanha 14
- `batimento_campanha58.zip`: registros Campanha 58
- `tabela_enriquecimento.zip`: CPFs adicionais

## Logs e Monitoramento

- Cada script gera logs detalhados
- Registra quantidades e estatísticas
- Inclui timestamps para rastreabilidade
- Exibe caminhos dos arquivos gerados

## Mapeamento da Saída Final

### Estrutura dos Arquivos de Saída

O processo gera diferentes tipos de arquivos de saída, cada um com estrutura específica conforme a etapa:

#### 1. Arquivo de Baixa (`baixa_tabelionato_YYYYMMDD_HHMMSS.zip`)

**Layout Final de Recebimento:**
- `NOME CLIENTE` ← `NOME_RAZAO_SOCIAL` (da base MAX tratada)
- `CPF/CNPJ CLIENTE` ← `CPFCNPJ_CLIENTE` (da base MAX tratada)
- `CNPJ CREDOR` ← `CNPJ_CREDOR` (da base MAX tratada)
- `NUMERO DOC` ← `CHAVE` (protocolo do Tabelionato)
- `VALOR DA PARCELA` ← `VALOR` (da base MAX tratada, sem formatação adicional)
- `DT. VENCIMENTO` ← `VENCIMENTO` (da base MAX tratada)
- `STATUS ACORDO` ← Valor fixo = 2
- `DT. PAGAMENTO` ← Data atual da execução
- `VALOR RECEBIDO` ← `Valor Total Pago` (da base Custas)

#### 2. Arquivos de Batimento (`batimento_campanha14.zip`, `batimento_campanha58.zip`)

**Layout de Importação do Sistema:**
- `Campanha` ← Classificação automática baseada no aging (14 ou 58 dias)
- `CPFCNPJ CLIENTE` ← `CpfCnpj` (do Tabelionato tratado)
- `NOME / RAZAO SOCIAL` ← `Devedor` (do Tabelionato tratado)
- `VALOR` ← `Custas` (valores monetários processados)
- `ID NEGOCIADOR` ← Campo em branco (para preenchimento posterior)
- `CNPJ CREDOR` ← Valor fixo "16.746.133/0001-41"
- `PARCELA` ← `Protocolo` (do Tabelionato)
- `VENCIMENTO` ← `DtAnuencia` (do Tabelionato tratado)
- `OBSERVACAO CONTRATO` ← `Credor` (do Tabelionato)
- `NUMERO CONTRATO` ← `Protocolo` (do Tabelionato)

#### 3. Tabela de Enriquecimento (`tabela_enriquecimento.zip`)

Contém registros que necessitam de enriquecimento adicional, mantendo a mesma estrutura dos arquivos de batimento.

### Fontes de Dados por Coluna

**Base MAX Tratada:**
- Informações de clientes (nome, CPF/CNPJ, CNPJ credor)
- Dados financeiros (valor, vencimento)
- Chaves de relacionamento

**Base Tabelionato Tratada:**
- Protocolos (chave de batimento)
- Dados de anuência (data, devedor)
- Informações do credor
- Classificação de campanha (baseada em aging)

**Base Custas:**
- Valores pagos efetivamente
- Dados de enriquecimento financeiro

## Considerações Técnicas

### Ambiente
- Processamento isolado em `Tabelionato/`
- Sem dependências externas
- Implementações locais de funções críticas

### Arquivos
- Estrutura de diretórios fixa
- Validação de arquivos necessários
- Remoção de arquivos temporários e anteriores

### Codificação
- UTF-8 para todos os arquivos
- Separador `;` nos CSVs
- Compressão ZIP para saídas
- Todos os arquivos de saída são compactados em formato ZIP com senha
- O sistema mantém logs detalhados de cada etapa do processamento
- Validações são aplicadas em cada etapa para garantir a integridade dos dados
- O processo é idempotente: pode ser executado múltiplas vezes com o mesmo resultado
