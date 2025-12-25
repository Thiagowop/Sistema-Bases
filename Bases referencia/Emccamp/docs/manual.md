# Manual de Operacao e Replicacao - Pipeline EMCCAMP

Este manual explica, de ponta a ponta, como entender, replicar e operar o pipeline EMCCAMP. O foco e na logica de negocio, no fluxo de dados e nos criterios de qualidade - sem depender de nomes de arquivos ou detalhes internos da implementacao.

A quem se destina: analistas, engenheiros de dados e partes interessadas que precisam compreender o processo, revisar cada etapa e opera-lo com seguranca e previsibilidade.

---

## 1. Visao geral do problema

- Objetivo: comparar e alinhar duas visoes (EMCCAMP e MAX), identificar divergencias (batimento) e preparar uma baixa formal (MAX - EMCCAMP) respeitando regras de negocio.
- Fontes de dados:
  - EMCCAMP via API TOTVS - titulos/parcelas com vencimentos e valores.
  - Baixas financeiras (EMCCAMP) via API TOTVS - pagamentos efetuados (data e valor recebidos).
  - MAX via consultas SQL - situacao atual dos titulos.
  - Bases judiciais (Autojur e MAX Smart) via SQL - identificacao de CPFs com processos.
  - Acordos em aberto (Doublecheck) via SQL - identificacao de CPFs com acordo vigente.
- Resultado: 
  - Conjunto de titulos presentes em EMCCAMP e ausentes em MAX (batimento EMCCAMP - MAX), segregado por judicial/extrajudicial.
  - Conjunto de titulos presentes em MAX e ausentes em EMCCAMP (baixa MAX - EMCCAMP), com marcacoes de recebimento e exclusoes por acordo.

---

## 2. Requisitos e preparacao de ambiente

- Software:
  - Python 3.10+ (recomendado 3.11 ou superior).
  - Acesso a SQL Server (rede/VPN e credenciais validas).
  - Acesso as APIs TOTVS (URL base, usuario/senha) do ambiente EMCCAMP.
- Dependencias (conceito):
  - Leitura/escrita de CSV/ZIP, requisicoes HTTP, acesso a SQL Server, manipulacao de dados tabulares (pandas) e logs.
- Configuracao por variaveis de ambiente (.env):
  - TOTVS - URLs e credenciais para os endpoints `CANDIOTTO.001` (titulos) e `CANDIOTTO.002` (baixas), alem dos parametros de filtro de vencimento.
  - SQL Server - hosts/instancias, bancos, usuario/senha para as bases MAX e Autojur.
  - Timeouts - conexao e comandos SQL/HTTP (evitar travamentos prolongados).
- Boas praticas de seguranca:
  - Nunca versionar credenciais.
  - Restringir o acesso do usuario das APIs/SQL ao minimo necessario.
  - Rotacionar senhas periodicamente.

Checklist inicial (antes da primeira execucao):
- [ ] Verifique rede/VPN.
- [ ] Teste credenciais das APIs TOTVS.
- [ ] Teste conexao com SQL Server (ambos ambientes: MAX/STD e CANDIOTTO/Autojur).
- [ ] Defina as datas de vencimento (inicio obrigatorio; fim opcional, pode ser `AUTO` para hoje-6) para a extracao do credor.
- [ ] Garanta espaco em disco suficiente para artefatos intermediarios.

---

## 3. Padroes de dados e contratos

- Chave canonica do titulo (CHAVE):
  - Definicao: concatenacao de "contrato" e "parcela". Ex.: CONTRATO-PARCELA.
  - Motivacao: permite anti-joins deterministicos entre visoes distintas.
  - **Formato**: `{CONTRATO}-{PARCELA}` onde ambos sao strings sem espacos
  - **Exemplos**: `12345-01`, `98765-001`, `ABC123-12`
  
- **Mapeamento de colunas (origem - destino):**

  **EMCCAMP (API TOTVS):**
  - `CODIGO_CONTRATO` - `CONTRATO`
  - `CODIGO_PARCELA` - `PARCELA`
  - `DATA_VENCTO_PARCELA` - `DATA_VENCIMENTO`
  - `VALOR_ATUALIZADO_PARCELA` - `VALOR`
  - `VALOR_ORIGINAL_PARCELA` - `VALOR_ORIGINAL`
  - `TIPO_PAGAMENTO` - `TIPO_PAGTO`
  - `NOME_EMPREENDIMENTO` - `EMPREENDIMENTO`
  - `DOCUMENTO_CLIENTE` - `CPF_CNPJ`
  - Coluna calculada: `CHAVE` = `CONTRATO + "-" + PARCELA`

  **MAX (SQL Server):**
  - `numero_contrato` - `NUMERO_CONTRATO`
  - `parcela` - `PARCELA` (usada como CHAVE)
  - `data_vencimento` - `DATA_VENCIMENTO`
  - `valor_titulo` - `VALOR`
  - `documento` - `CPF_CNPJ`
  - `campanha_nome` - `CAMPANHA`
  - `status` - `STATUS_TITULO`
  - `dt_baixa_max` - `DT_BAIXA` (quando existir)
  - Coluna calculada: `CHAVE` = `PARCELA` (ja vem no formato completo)

  **Baixas EMCCAMP (API TOTVS):**
  - `CODIGO_CONTRATO` - usado para construir CHAVE
  - `CODIGO_PARCELA` - usado para construir CHAVE
  - `DATA_PAGAMENTO` - `DATA_RECEBIMENTO`
  - `VALOR_PAGO` - `VALOR_RECEBIDO`
  - Coluna calculada: `CHAVE` = `CODIGO_CONTRATO + "-" + CODIGO_PARCELA`

- Campos obrigatorios por visao (minimo necessario):
  - EMCCAMP: CONTRATO, PARCELA, DATA_VENCIMENTO, VALOR, CPF_CNPJ
  - MAX: NUMERO_CONTRATO, PARCELA, DATA_VENCIMENTO, VALOR, CPF_CNPJ
  - Baixas: CHAVE (construida), DATA_RECEBIMENTO, VALOR_RECEBIDO
  
- Padroes de normalizacao:
  - Datas no padrao ISO (YYYY-MM-DD) durante o processamento; apresentacao pode ser ajustada (DD/MM/YYYY) quando necessario.
  - Numeros livres de formatacao local (sem separadores de milhar; ponto como separador decimal).
  - Texto sem espacos excedentes, com composicao consistente (normalizacao de acentos quando aplicavel).
- Integridade:
  - Nenhum registro com CHAVE vazia.
  - Duplicidades por CHAVE sao inconsistencias (devem ser separadas/ajustadas antes de comparacoes).
  - CPF/CNPJ sempre tratado no formato "somente digitos" para comparacoes e juncoes.

---

<a id="fluxo"></a>
## 4. Fluxo macro (end-to-end)

```mermaid
flowchart TD
  A[Extracoes (5 fontes)] --> B[Tratamentos (2 bases)]
  B --> C[Batimento (EMCCAMP - MAX)]
  C --> D[Baixa (MAX - EMCCAMP)]

  subgraph A1[Extracoes]
    A1a[Credor via API] --> A1
    A1b[Baixas via API] --> A1
    A1c[MAX via SQL]    --> A1
    A1d[Judicial via SQL] --> A1
    A1e[Acordos via SQL]  --> A1
  end

  subgraph B1[Tratamentos]
    B1a[Normalizar credor] --> B1
    B1b[Normalizar MAX]    --> B1
  end
```

- Ordem de execucao (sugerida): extracoes - tratamentos - batimento - baixa.
- Observabilidade: ao final de cada etapa, exibir contagens, filtros aplicados, duracao e proximo passo.
- Determinismo: nenhuma "gambiarra"/fallback silencioso; se faltar coluna essencial, abortar com erro claro.

---

<a id="extracoes"></a>
## 5. Extracoes (conceito e regras)

As extracoes trazem "visoes" complementares:

1) EMCCAMP - API TOTVS (consulta SQL encapsulada)
- Objetivo: obter a posicao "de origem" do credor (vendas/parcelas com vencimento e valores).
- Parametros: data de vencimento inicial (obrigatoria) e final (opcional) enviados ao endpoint.
- Autenticacao: HTTP com usuario/senha do TOTVS.
- Campos minimos esperados: contrato, parcela, data de vencimento, valor atualizado/original, tipo de pagamento, empreendimento, CPF/CNPJ do cliente.
- Validacoes imediatas:
  - Rejeitar respostas vazias ou com esquema inesperado.
  - Converter para tabela; padronizar nomes e tipos basicos ja na entrada quando possivel.

2) Baixas financeiras - API TOTVS
- Objetivo: capturar pagamentos efetivados (data e valor) para cruzamento posterior.
- Regra de filtro: considerar apenas registros com indicador de honorario baixado diferente de zero (pagamentos relevantes).
- Derivacoes imediatas:
  - Construir CHAVE (contrato-parcela) usando os campos proprios desse retorno.
  - Normalizar data de recebimento (padrao ISO) e valor recebido (numerico).

3) MAX (sistema de cobranca) - SQL Server
- Objetivo: obter a posicao atual da cobranca (situacao, valores, campanha, etc.).
- Filtro por data de vencimento: configuravel via variaveis de ambiente; inicio e obrigatorio, fim opcional.
- Colunas essenciais devem estar presentes para gerar CHAVE e validar consistencia.

4) Judicial - SQL (Autojur + MAX Smart)
- Objetivo: construir uma lista de CPFs vinculados a processos judiciais.
- Operacao: unir os dois resultados e remover duplicados por CPF (sempre tratados como "somente digitos").

5) Acordos em aberto - SQL
- Objetivo: listar CPFs com acordo vigente; servira como "bloqueio" na baixa.
- Normalizacao: garantir CPF/CNPJ consistente para juncao posterior.

**Detalhes tecnicos das extracoes:**

A) API TOTVS (EMCCAMP e Baixas)
- **Autenticacao**: HTTP Basic Auth com usuario e senha
- **EMCCAMP**: Endpoint especifico `CANDIOTTO.001`
  - Metodo: POST com body contendo query SQL e parametros de data
  - Retorno: JSON com array de registros
  - Conversao: JSON - DataFrame pandas
- **Baixas**: Endpoint `CANDIOTTO.002`
  - Similar ao EMCCAMP mas com query diferente
  - Filtro aplicado: `HONORARIO_BAIXADO <> 0`
  
B) SQL Server (MAX, Judicial, Acordos)
- **Conexao**: pyodbc com driver SQL Server
- **Timeout**: configuravel (padrao: 30s conexao, 300s comando)
- **MAX**: Query com JOIN de multiplas tabelas
  - Principais: `tb_titulo_01`, `tb_titulo_07`, `tb_pessoa`
  - Filtro: `DATA_VENCIMENTO` entre datas configuradas
- **Judicial**: UNION de duas queries
  - Autojur: `tb_processos` (campo CPF_CLIENTE)
  - MAX Smart: tabelas de processos judiciais
  - Resultado: lista unica de CPFs (sem duplicatas)
- **Acordos**: Query simples
  - Tabela: dados de acordos vigentes
  - Campo chave: `CPFCNPJ_CLIENTE`

C) Formato de saida das extracoes
- **Todas as extracoes** geram arquivos ZIP contendo CSV:
  - CSV interno com encoding `utf-8-sig` (compativel com Excel)
  - Separador: ponto-e-virgula (`;`)
  - Nome do arquivo: timestampado (ex.: `Emccamp_20251016_120530.csv`)
- **Motivo do ZIP**: compactacao reduz espaco e padroniza formato

Falhas comuns e como tratar:
- HTTP 401/403: rever credenciais ou escopo do usuario.
- Timeouts: aumentar limiares de conexao/comando dentro de limites razoaveis; verificar VPN e latencia.
- SQL sem retorno: validar filtros de data e parametros de consulta.
- Campos ausentes: o pipeline deve falhar explicitamente; corrija a consulta/fonte.

---

<a id="tratamentos"></a>
## 6. Tratamentos (normalizacao e qualidade)

Por que tratar? Cada fonte usa nomenclaturas e formatos diferentes. O tratamento alinha tudo a um "esquema canonico" para comparacoes consistentes.

6.1 EMCCAMP (normalizacao)
- Renomear colunas para o esquema canonico.
- Criar CHAVE = CONTRATO-PARCELA (sem espacos; strings tratadas) a partir das colunas CONTRATO e PARCELA.
- Validar obrigatorios (contrato, parcela, data de vencimento, valor, CPF/CNPJ).
- Separar inconsistencias: CHAVE vazia, CHAVE duplicada, CPF/CNPJ vazio/ invalido.

6.2 MAX (normalizacao)
- Renomear colunas para o esquema canonico e preservar campos uteis (ex.: data de baixa quando existir).
- Usar PARCELA como chave primaria (CHAVE = PARCELA). Se houver variacao local, a composicao e NUMERO_CONTRATO-PARCELA.
- **Validacoes de formato da PARCELA (CRITICO para qualidade):**
  - **Formato valido**: Somente numeros no padrao `XXX-YY` onde:
    - Primeiro grupo: minimo 3 digitos (ex.: `123`, `12345`)
    - Segundo grupo: minimo 2 digitos apos o hifen (ex.: `01`, `001`)
    - Exemplos validos: `12345-01`, `123-12`, `1234-001`
  - **Formatos invalidos (devem ser rejeitados):**
    - Com letras: `JM-3`, `hon-1`, `mensal-41`
    - Formato curto: `123-1` (segundo grupo com 1 digito)
    - Com caracteres especiais: `few/14`, `22/04/2022`, `76.43-15/03/2023`
    - Sem hifen: `1`, `5485`, `123854`
    - Com 3+ grupos: `123-12-3`, `1234-12-34`
  - **Regex usado**: `^[0-9]{3,}-[0-9]{2,}$`
- **Validacao de duplicatas (CRITICO):**
  - Nenhuma PARCELA pode aparecer duplicada nos dados validos
  - Todas as duplicatas (tanto em PARCELA quanto em CHAVE) devem ser removidas para inconsistencias
  - Usar `duplicated(keep=False)` para remover TODAS as ocorrencias duplicadas (nao apenas a segunda)
- Validar campos obrigatorios: NUMERO_CONTRATO, PARCELA, DATA_VENCIMENTO, VALOR, CPF_CNPJ
- Separar inconsistencias: PARCELA vazia, formato invalido, PARCELA/CHAVE duplicada, campos obrigatorios vazios

Boas praticas de tratamento:
- Converter datas e valores para tipos fortes antes de comparacoes.
- Centralizar limpeza (trim, upper/normalizacao de acentos quando necessario).
- Registrar as contagens: originais, validos e inconsistentes.

---

<a id="batimento"></a>
## 7. Batimento (EMCCAMP - MAX)

- Objetivo: identificar titulos existentes no credor e ausentes no MAX.
- **Tecnica de comparacao (CRITICO):**
  - Anti-join a esquerda (left anti-join) comparando:
    - `EMCCAMP.CHAVE` (formato: `CONTRATO-PARCELA`, ex.: `12345-01`)
    - vs
    - `MAX.PARCELA` (formato: `NUMERO-SEQUENCIA`, ex.: `12345-01`)
  - **IMPORTANTE**: No MAX, a coluna comparada e `PARCELA` (nao `CHAVE`), pois `PARCELA` ja contem o identificador completo
  - A comparacao busca registros onde `EMCCAMP.CHAVE` NAO existe em `MAX.PARCELA`
- **Pre-filtros no EMCCAMP (OBRIGATORIOS - aplicados ANTES do anti-join):**
  - **Filtro de TIPO_PAGTO** (CRITICO):
    - Remover linhas onde a coluna `TIPO_PAGTO` contenha exatamente:
      - `"TAXA JUDICIAL"` (maiusculas, sem variacoes)
      - `"HONORARIO COBRANCA"` (com acento, maiusculas)
    - Metodo: usar operador de negacao `NOT IN` ou exclusao por mascara booleana
    - **Justificativa**: Esses tipos representam taxas/honorarios administrativos que nao fazem parte da comparacao de titulos
    - **Impacto se nao aplicar**: Falsos positivos no batimento (registros que nao deveriam ser comparados aparecerao como ausentes)
- **Deduplicacao no MAX (OBRIGATORIA - aplicada ANTES do anti-join):**
  - **Quando aplicar**: Se houver duplicidade na coluna PARCELA
  - **Regra de desempate**: Ordenar por `DT_BAIXA` descendente e manter apenas o primeiro registro
  - **Justificativa**: Garante que, em caso de multiplos registros para a mesma PARCELA, o mais recente seja usado na comparacao
  - **Impacto se nao aplicar**: Erro na comparacao (chaves duplicadas causam juncoes incorretas)
- **Pos-processamento (classificacao judicial/extrajudicial):**
  - Classificar por carteira judicial vs extrajudicial usando a lista de CPFs judiciais.
- Metricas de sucesso:
  - Quantos titulos do credor foram comparados; quantos ficaram sem par no MAX; distribuicao judicial/extrajudicial.
- Erros a observar:
  - Falta de colunas de chave; divergencias de formato de data/valor; filtros de tipos muito restritivos.

---

<a id="baixa"></a>
## 8. Baixa (MAX - EMCCAMP)

- Objetivo: apontar titulos existentes no MAX e ausentes no credor, prontos para baixa formal.
- **Tecnica de comparacao (CRITICO):**
  - Anti-join a direita (right anti-join) conceitual; na pratica, comparar:
    - `MAX.PARCELA` (formato: `NUMERO-SEQUENCIA`, ex.: `12345-01`)
    - vs
    - `EMCCAMP.CHAVE` (formato: `CONTRATO-PARCELA`, ex.: `12345-01`)
  - Busca registros onde `MAX.PARCELA` NAO existe em `EMCCAMP.CHAVE`
- **Filtros de negocio sobre MAX (OBRIGATORIOS - ANTES do anti-join):**
  - **Filtro 1 - Coluna CAMPANHA** (operador: `IN`):
    - Manter apenas: `"000041 - EMCCAMP"` OU `"000041-EMCCAMP"` OU `"EMCCAMP 41"`
    - Sao 3 variacoes possiveis do mesmo identificador de campanha
    - Case-sensitive: usar exatamente como descrito (maiusculas)
    - **Justificativa**: Apenas titulos da campanha EMCCAMP devem ser devolvidos ao credor
    - **Impacto se nao aplicar**: Baixa de titulos de outras campanhas (erro critico de negocio)
  - **Filtro 2 - Coluna STATUS_TITULO** (operador: `==`):
    - Manter apenas: `"ABERTO"` (maiusculas, sem variacoes)
    - Remove titulos ja baixados/encerrados
    - **Justificativa**: Apenas titulos em aberto devem ser devolvidos (titulos baixados nao sao mais obrigacao do credor)
    - **Impacto se nao aplicar**: Baixa incorreta de titulos ja pagos/baixados
  - **Sequencia**: aplicar ambos os filtros com operador AND (ambos devem ser verdadeiros simultaneamente)
- **Exclusao por acordos (OBRIGATORIA - PRIMEIRA, APOS o anti-join):**
  - **Ordem otimizada**: feito ANTES do PROCV com baixas para reduzir processamento
  - **Objetivo**: remover CPFs com acordo vigente (nao devem ser devolvidos ao credor)
  - **Normalizacao de CPF** (CRITICO):
    - No MAX: extrair apenas digitos da coluna `CPF_CNPJ` (remover pontos, hifens, barras)
    - Nos Acordos: extrair apenas digitos da coluna `CPFCNPJ_CLIENTE`
    - Exemplo: `123.456.789-00` vira `12345678900`
  - **Tecnica**: Anti-join (excluir registros onde CPF normalizado esta na lista de acordos)
  - **Contagem**: registrar quantos foram removidos por esse filtro
  - **Justificativa**: Clientes com acordo em aberto nao devem ter titulos devolvidos ao credor (risco de cobranca em duplicidade)
  - **Impacto se nao aplicar**: Baixa indevida de titulos com acordo (erro critico de negocio e compliance)
- **Enriquecimento com Baixas via PROCV (OBRIGATORIO - SEGUNDA, APOS filtro de acordos):**
  - **Ordem otimizada**: feito DEPOIS do filtro de acordos (menos registros para processar)
  - **Objetivo**: trazer informacoes de pagamento quando existirem
  - **Tipo de join**: LEFT JOIN (mantem todos os registros da baixa, traz baixas quando houver)
  - **Chave de join**: `MAX.PARCELA` = `BAIXAS.CHAVE`
  - **Colunas trazidas**:
    - `DATA_RECEBIMENTO` (data do pagamento)
    - `VALOR_RECEBIDO` (valor pago)
  - **Regra de marcacao "com recebimento"**:
    - Verdadeiro se: `DATA_RECEBIMENTO` nao nulo/vazio E `VALOR_RECEBIDO` > 0
    - Falso: qualquer um dos campos ausente/zero
  - **Justificativa**: Separar titulos com evidencia de pagamento dos sem evidencia para analise posterior
- **Split final (OBRIGATORIO - saida em 2 arquivos):**
  - **Arquivo 1 - Com recebimento**: registros onde DATA_RECEBIMENTO e VALOR_RECEBIDO estao preenchidos
  - **Arquivo 2 - Sem recebimento**: registros onde qualquer um dos campos esta vazio/nulo
  - Ambos os arquivos devem incluir todas as colunas relevantes para analise posterior
- Metricas de sucesso:
  - Quantidade apos filtros; quantos removidos por acordo; quantos com/sem recebimento.
- Erros a observar:
  - Campanhas/status incorretos; ausencia das colunas da CHAVE nas fontes; CPFs com formatacao inconsistente.

---

<a id="runbook"></a>
## 9. Enriquecimento de Contato <a id="enriquecimento"></a>

- **Comando CLI**: `python main.py enriquecimento` (ou via `run_pipeline_emccamp.bat` opcao 7).
- **Config**: bloco `enriquecimento.emccamp_batimento` no `config.yaml` define o caminho da base bruta (`Emccamp.zip`), os componentes da chave (`NUM_VENDA` + `ID_PARCELA`), os filtros pelo arquivo de batimento e as colunas de contato exportadas.
- **Fluxo**:
  1. Carrega `data/input/emccamp/Emccamp.zip`, cria `CHAVE = NUM_VENDA-ID_PARCELA` e mantém apenas os registros encontrados em `data/output/batimento/emccamp_batimento_*.zip`.
  2. Normaliza telefones (somente dígitos) e descarta valores vazios.
  3. Descarta e-mails sem `@` quando `rules.descartar_email_sem_arroba=true`.
  4. Gera uma linha por contato com `OBSERVACAO = Base Emccamp - DD/MM/AAAA` e `TELEFONE PRINCIPAL = 1`.
  5. Deduplica por (`CPFCNPJ CLIENTE`, `CONTATO`, `TIPO`) e ordena TEL -> EMAIL.
- **Saida**: `data/output/enriquecimento_contato_emccamp/enriquecimento_contato_emccamp.zip`, contendo apenas contatos relativos aos títulos não encontrados na MAX.

**Saidas esperadas:**
- Cada comando exibe resumo formatado (80 caracteres)
- Metricas: registros processados, duracao, arquivos gerados
- Log consolidado em: `data/logs/execucao_emccamp.log`

---

### 14.4. Configuracao (config.yaml)

O arquivo `src/config/config.yaml` centraliza todas as configuracoes:

#### **Exemplo de configuracao critica:**

```yaml
global:
  csv_separator: ";"                   # Separador CSV
  encoding: "utf-8-sig"                # Encoding (compativel com Excel)
  date_format: "%Y-%m-%d"              # Formato de data
  add_timestamp_to_files: true         # Timestampar saidas

paths:
  input: "data/input"                  # Diretorio de entrada
  output: "data/output"                # Diretorio de saida
  logs: "data/logs"                    # Diretorio de logs

# Configuracao de Batimento
flags:
  filtros_batimento:
    habilitar: true                    # Ativar filtros TIPO_PAGTO
    tipos_remover:
      - "TAXA JUDICIAL"
      - "HONORARIO COBRANCA"

# Configuracao de Baixa
baixa:
  chaves:
    max: "PARCELA"                     # Coluna chave no MAX
    emccamp: "CHAVE"                   # Coluna chave no EMCCAMP
  
  filtros:
    max:
      campanhas:                       # Filtro de campanhas (IN)
        - "000041 - EMCCAMP"
        - "000041-EMCCAMP"
        - "EMCCAMP 41"
      status_titulo:                   # Filtro de status (==)
        - "ABERTO"
  
  export:
    filename_prefix: "emccamp_baixa"
    com_recebimento_prefix: "baixa_com_recebimento"
    sem_recebimento_prefix: "baixa_sem_recebimento"

# Validacao MAX
validation:
  max:
    parcela_regex: "^[0-9]{3,}-[0-9]{2,}$"   # Formato valido: 123-01

# Mapeamento de colunas
mappings:
  emccamp:
    CODIGO_CONTRATO: "CONTRATO"
    CODIGO_PARCELA: "PARCELA"
    DATA_VENCTO_PARCELA: "DATA_VENCIMENTO"
    VALOR_ATUALIZADO_PARCELA: "VALOR"
    DOCUMENTO_CLIENTE: "CPF_CNPJ"
    # ... outros mapeamentos
  
  max:
    numero_contrato: "NUMERO_CONTRATO"
    parcela: "PARCELA"
    data_vencimento: "DATA_VENCIMENTO"
    valor_titulo: "VALOR"
    documento: "CPF_CNPJ"
    # ... outros mapeamentos
```

**Como alterar configuracoes:**
1. Editar `src/config/config.yaml`
2. Nao precisa reiniciar ambiente
3. Proxima execucao usa nova configuracao

---

### 14.5. Variaveis de Ambiente (.env)

O arquivo `.env` armazena credenciais e parametros sensiveis:

```bash
# API TOTVS - Titulos (CANDIOTTO.001)
EMCCAMP_API_URL=https://totvs.emccamp.com.br:8051/api/framework/v1/consultaSQLServer/RealizaConsulta/CANDIOTTO.001/0/X
EMCCAMP_API_USER=seu_usuario_api
EMCCAMP_API_PASSWORD=sua_senha_api

# API TOTVS - Baixas (CANDIOTTO.002)
TOTVS_BASE_URL=https://totvs.emccamp.com.br:8051
TOTVS_USER=seu_usuario_api
TOTVS_PASS=sua_senha_api

# Filtros de extracao EMCCAMP
EMCCAMP_DATA_VENCIMENTO_INICIAL=2023-01-01    # Obrigatorio
EMCCAMP_DATA_VENCIMENTO_FINAL=AUTO            # Opcional (AUTO => hoje-6)

# SQL Server (MAX)
MSSQL_SERVER_STD=servidor.empresa.com.br
MSSQL_DATABASE_STD=MAX_DB
MSSQL_USER_STD=usuario_sql
MSSQL_PASSWORD_STD=senha_sql

# SQL Server (Autojur)
MSSQL_SERVER_CANDIOTTO=servidor2.empresa.com.br
MSSQL_DATABASE_CANDIOTTO=AUTOJUR_DB
MSSQL_USER_CANDIOTTO=usuario_sql
MSSQL_PASSWORD_CANDIOTTO=senha_sql

# Timeouts
HTTP_TIMEOUT=300                               # Timeout HTTP (segundos)
SQL_CONNECTION_TIMEOUT=30                      # Timeout conexao SQL
SQL_COMMAND_TIMEOUT=300                        # Timeout comando SQL

# Filtros MAX (opcional)
MAX_DATA_VENCIMENTO_INICIAL=2023-01-01
MAX_DATA_VENCIMENTO_FINAL=2023-12-31
```

**Seguranca:**
-  `.env` esta no `.gitignore` (nunca versionado)
-  Use `.env.example` como template
-  Rotacione credenciais periodicamente
-  Restrinja acesso ao arquivo no servidor

---

### 14.6. Fluxo de Desenvolvimento

#### **Setup inicial (primeira vez)**

```bash
# 1. Clonar repositorio
git clone https://github.com/MCSA-Tecnologia/Emccamp.git
cd Emccamp

# 2. Criar ambiente virtual
python -m venv venv
.\venv\Scripts\activate          # Windows
source venv/bin/activate         # Linux/Mac

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar credenciais
copy .env.example .env           # Windows
cp .env.example .env             # Linux/Mac
# Editar .env com suas credenciais

# 5. Validar setup
python main.py extract emccamp   # Teste de extracao
```

#### **Desenvolvimento de nova funcionalidade**

```bash
# 1. Criar branch
git checkout -b feature/nova-funcionalidade

# 2. Modificar codigo (ex.: src/processors/novo_processo.py)

# 3. Testar localmente
python main.py <comando>

# 4. Verificar logs
cat data/logs/execucao_emccamp.log    # Linux/Mac
type data\logs\execucao_emccamp.log   # Windows

# 5. Commit e push
git add .
git commit -m "feat: Adicionar nova funcionalidade"
git push origin feature/nova-funcionalidade
```

#### **Debugging**

```bash
# Executar com output detalhado (Python verbose)
python -v main.py extract emccamp

# Verificar erros de sintaxe
python -m py_compile src/processors/baixa.py

# Verificar importacoes
python -c "from src.processors import baixa; print('OK')"

# Limpar outputs para reprocessamento
rm -rf data/output/*                  # Linux/Mac
del /s /q data\output\*               # Windows
```

---

### 14.7. Arquitetura de Dados

#### **Convencoes de nomeacao de arquivos:**

```
# Entradas (extraidas)
data/input/emccamp/Emccamp_YYYYMMDD_HHMMSS.csv.zip
data/input/baixas/baixa_emccamp_YYYYMMDD_HHMMSS.csv.zip
data/input/base_max/MaxSmart_YYYYMMDD_HHMMSS.csv.zip
data/input/judicial/ClientesJudiciais_YYYYMMDD_HHMMSS.csv.zip
data/input/doublecheck_acordo/acordos_abertos.csv

# Saidas (tratadas)
data/output/emccamp_tratada/emccamp_tratada_YYYYMMDD_HHMMSS.zip
data/output/max_tratada/max_tratada_YYYYMMDD_HHMMSS.zip
data/output/inconsistencias/max_inconsistencias_YYYYMMDD_HHMMSS.zip

# Processos finais
data/output/batimento/emccamp_batimento_YYYYMMDD_HHMMSS.zip
  +-- judicial_YYYYMMDD_HHMMSS.csv
  +-- extrajudicial_YYYYMMDD_HHMMSS.csv

data/output/baixa/emccamp_baixa_YYYYMMDD_HHMMSS.zip
  +-- baixa_com_recebimento_YYYYMMDD_HHMMSS.csv
  +-- baixa_sem_recebimento_YYYYMMDD_HHMMSS.csv
```

**Pipeline sempre usa o arquivo mais recente** de cada tipo (ordenacao por timestamp).

#### **Formato de arquivos:**

- **Encoding**: UTF-8 com BOM (`utf-8-sig`)
- **Separador**: Ponto-e-virgula (`;`)
- **Compactacao**: ZIP (reduz ~70% do tamanho)
- **Estrutura**: 1 ZIP pode conter multiplos CSVs

---

### 14.8. Testes e Validacao

#### **Checklist de validacao:**

```bash
# 1. Validar extracao (volumes esperados)
python main.py extract all
# Verificar: EMCCAMP ~20k, MAX ~37k, Judicial ~1k

# 2. Validar tratamentos (inconsistencias controladas)
python main.py treat all
# Verificar: MAX ~680 inconsistencias (~2%)

# 3. Validar batimento (filtros aplicados)
python main.py batimento
# Verificar: Taxa de batimento ~17% (~3,4k registros)

# 4. Validar baixa (filtros e splits)
python main.py baixa
# Verificar: ~55 registros (37 com receb + 18 sem)

# 5. Verificar logs
grep "ERRO" data/logs/execucao_emccamp.log    # Linux/Mac
findstr "ERRO" data\logs\execucao_emccamp.log # Windows
```

#### **Volumes esperados (referencia):**

| Etapa | Volume Tipico | Taxa Esperada |
|-------|---------------|---------------|
| EMCCAMP extraido | 19,998 | 100% |
| MAX extraido | 35,668 | 100% |
| MAX tratado valido | 35,000 | 98% (2% inconsistencias) |
| Batimento total | 3,391 | 17% de EMCCAMP |
| Baixa total | 55 | 0.45% de MAX filtrado |

**Alertas:**
-  Volume zerado: revisar credenciais e filtros de data
-  Taxa de inconsistencias > 5%: problemas na fonte
-  Batimento > 25%: revisar filtros TIPO_PAGTO

---

### 14.9. Referencias Tecnicas

**Documentacao adicional:**
- `README.md` - Visao geral e quickstart
- `docs/fluxo_completo.md` - Fluxogramas e detalhes tecnicos
- `docs/mapa_arquivos.md` - Guia de navegacao do codigo

**Bibliotecas utilizadas:**
- [Pandas](https://pandas.pydata.org/docs/) - Manipulacao de dados
- [PyODBC](https://github.com/mkleehammer/pyodbc/wiki) - SQL Server
- [Requests](https://requests.readthedocs.io/) - HTTP/REST APIs
- [PyYAML](https://pyyaml.org/wiki/PyYAMLDocumentation) - Configuracao

**SQL Server drivers:**
- Windows: `ODBC Driver 17 for SQL Server` (pre-instalado)
- Linux: Instalar `msodbcsql17` via apt/yum

---

Com este apendice tecnico, o manual esta completo tanto para **analise de negocio** (secoes 1-13) quanto para **implementacao tecnica** (secao 14).
