# **Batimento – 4° Tabelionato**

## Configurar credenciais de acesso no .env


## **1. Recebimento da base**  
- A base é recebida por e-mail com o título:  
  *“Base de Dados e Relatório de Recebimento de Custas Postergadas do 4º Tabelionato de Protestos do dia (data atual)”*.  
- O remetente é **adriano@4protestobh.com**.

### **1.1 Arquivos recebidos**  
- O e-mail contém **2 arquivos** anexados:  
  - **Cobrança**  
  - **RecebimentoCustas**  
- **Por enquanto**, será utilizado apenas o arquivo **Cobrança**.

---

## **2. Estrutura do arquivo**  
- O arquivo vem compactado em **.zip**.  
- Senha do ZIP: **Mf4tab@**.  
- Dentro do ZIP, o conteúdo está em **formato .txt**.  
- Será necessário **converter o .txt em Excel** para manipulação.  

---

## **2. Extração base Max**  

- Usar credenciais de acesso ao bando de dados sql que devem ser armazenadas em .env

- Realizar a consulta: 
Consultas SQL para extração de dados dos bancos MAX para o contexto do tabelionato.
"""

# Query principal do MAX para Tabelionato (STD2016)
SQL_MAX_TABELIONATO = """
SELECT DISTINCT
    dbo.RetornaNomeCampanha(MoCampanhasID,1) AS 'CAMPANHA',
    dbo.RetornaNomeRazaoSocial(MoClientesID) AS 'CREDOR',
    dbo.RetornaCPFCNPJ(MoClientesID,1) AS 'CNPJ_CREDOR',
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS 'CPFCNPJ_CLIENTE',
    dbo.RetornaNomeRazaoSocial(MoInadimplentesID) AS 'NOME_RAZAO_SOCIAL',
    MoContrato AS 'NUMERO_CONTRATO',
    MoMatricula AS 'EMPREENDIMENTO',
    CAST(MoDataCriacaoRegistro AS DATE) AS 'DATA_CADASTRO',
    MoNumeroDocumento AS 'PARCELA',
    Movimentacoes_ID AS 'Movimentacoes_ID',
    MoDataVencimento AS 'VENCIMENTO',
    MoValorDocumento AS 'VALOR',
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS 'STATUS_TITULO',
    MoTipoDocumento AS 'TIPO_PARCELA'
FROM Movimentacoes
    INNER JOIN Pessoas ON MoInadimplentesID = Pessoas_ID
WHERE
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1) -- Filtrar status dos títulos 0 = Aberto e 1 = Liquidado com acordo
    AND MoClientesID = 2746 -- Filtrar Vic Engenharia
    AND MoOrigemMovimentacao in ('C', 'I') -- Filtrar títulos originais C = Cadastrado manualmente e I = importado
    AND MoCampanhasID IN (60, 80) -- Filtrar apenas campanhas 000058 e 000078 do Tabelionato
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""

# Query principal do Candiotto para Tabelionato (STD2016)
SQL_CANDIOTTO_TABELIONATO = """
SELECT DISTINCT
    dbo.RetornaNomeCampanha(MoCampanhasID,1) AS 'CAMPANHA',
    dbo.RetornaNomeRazaoSocial(MoClientesID) AS 'CREDOR',
    dbo.RetornaCPFCNPJ(MoClientesID,1) AS 'CNPJ_CREDOR',
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS 'CPFCNPJ_CLIENTE',
    dbo.RetornaNomeRazaoSocial(MoInadimplentesID) AS 'NOME_RAZAO_SOCIAL',
    MoContrato AS 'NUMERO_CONTRATO',
    MoMatricula AS 'EMPREENDIMENTO',
    CAST(MoDataCriacaoRegistro AS DATE) AS 'DATA_CADASTRO',
    MoNumeroDocumento AS 'PARCELA',
    MoDataVencimento AS 'VENCIMENTO',
    MoValorDocumento AS 'VALOR',
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS 'STATUS_TITULO',
    MoTipoDocumento AS 'TIPO_PARCELA'
FROM Movimentacoes
    INNER JOIN Pessoas ON MoInadimplentesID = Pessoas_ID
WHERE
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1) -- Filtrar status dos títulos 0 = Aberto e 1 = Liquidado com acordo
    AND MoClientesID = 2746 -- Filtrar Vic Engenharia
    AND MoOrigemMovimentacao in ('C', 'I') -- Filtrar títulos originais C = Cadastrado manualmente e I = importado
    AND MoCampanhasID IN (60, 80) -- Filtrar apenas campanhas 000058 e 000078 do Tabelionato
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""

## **Tratamento da base Max**

- Carrega a base do input

## **Limpeza e padronização**

## Incosistencias

* Parcelas em formatos fora do padrao

- com ,
- com datas
- com -
- com somente numeros curtos como 1 ou qualquer 

Exemplos: de formatos incosistentes 
2163.79-26/05/2025
1
217,32-06/09/2024
1604,41-45618

* Coluna vencimento fora do padrao

- 0219-09-10
- ou fora do padrao de data

## **Tratamento da bas Tabelionato**

### **3. Coluna C – Data e Devedor (dados colados)**  
- Na **coluna C**, a informação vem **concatenada** na mesma célula (Data de Anuência + Devedor).  
  - Ex.: `27/05/2022 00:00:00 BANCO DO BRASIL S/A`

### **3.1 Separação em colunas**  
- Separar em duas colunas: **DtAnuencia** e **Devedor**.  
  - Ex.: `DtAnuencia = 27/05/2022` | `Devedor = BANCO DO BRASIL S/A`

---

### **4.2 Tratamento do CPF/CNPJ**  
- A coluna do CPF/CNPJ vem com **espaçamentos extras**, por isso precisa ser tratada.  

#### Exemplo com **CNPJ**:  

| Coluna | Valor                  | Contador |  
|--------|------------------------|----------|  
| J      | 00.000.000/0412-03     | 20       |  
| L      | 00.000.000/0412-03     | 18       |  

#### Exemplo com **CPF**:  

| Coluna | Valor           | Contador |  
|--------|-----------------|----------|  
| J      | 000.077.536-32  | 16       |  
| L      | 000.077.536-32  | 14       |  

- Como se vê nos exemplos, o **J (sem tratamento)** tem mais caracteres por causa dos espaçamentos.  

#### **Regras de caracteres corretos**  
- **CNPJ:** deve ter **18 caracteres** (formato `##.###.###/####-##`).  
- **CPF:** deve ter **14 caracteres** (formato `###.###.###-##`).  

## Inconsistencias 

## **Exportar retirar e exportar inconsistencias

- DtAnuencia com formato incorreto fora do padrao de data ou vazio nulo ou nat

- Protocolo fora do padrao

Exemplos: de formatos incosistentes 
2163.79-26/05/2025
1
217,32-06/09/2024
1604,41-45618
nulos e vazios 

-


### **4. Definição do Aging**  

- Criar uma coluna aging 

- As parcelas são separadas por **Aging** para definir a campanha:  
  - **Campanha 14** → **Aging ≤ 1800**  
  - **Campanha 58** → **Aging > 1800**

### **4.1 Regra para clientes com múltiplas parcelas**  
- Se um cliente tiver parcelas acima e abaixo de 1800, basta existir 1 parcela com **Aging ≤ 1800** para ele ir para a **Campanha 14**. 



---

## **Batimento**

### **5. Regra principal**  
- O batimento será feito com as colunas CHAVE das duas bases tratadas. 

Como o PROCV identifica as parcelas ausentes

    O batimento parte da base tratada do Tabelionato, normaliza a coluna CHAVE e a cruza com a coluna CHAVE da base MAX. Após o merge, ficam retidas apenas as parcelas do Tabelionato que não aparecem no MAX — ou seja, exatamente os registros que o PROCV não encontrou — e elas seguem para as etapas seguintes do batimento.

Regra para protocolos com um CNPJ e vários CPFs

    Quando o anti-join encontra protocolos duplicados, o processamento calcula uma prioridade por tipo de documento, atribuindo peso menor ao CNPJ (prioridade 0) do que ao CPF (prioridade 1). Dessa forma, o registro com CNPJ é escolhido como linha principal do protocolo e permanece na base que alimenta o arquivo de batimento.

As linhas excedentes do mesmo protocolo — como CPFs adicionais — são separadas em um grupo complementar. Todos os documentos desse grupo são armazenados na coluna Enriquecimento associada ao protocolo principal, pronta para alimentar o relatório de enriquecimento.

Mesmo quando não há duplicados, a coluna CPF_ENRIQUECIMENTO é criada para manter o layout consistente; Na etapa de mapeamento para o layout final, essa coluna é preservada para que o arquivo de saída contenha tanto o protocolo priorizado (com o CNPJ) quanto a lista dos CPFs que devem seguir para o enriquecimento e assim podemos criar as 3 planilhas, uma com os registros do procv da campanha 14, outro do 58 , e outra dos que foram separados para enriquecimento

### **5.1 Segunda regra – protocolos duplicados**  
- Em alguns casos, existem **protocolos duplicados** na base.  
- A regra aplicada nesses casos será:  
  - **Manter o CNPJ** como prioritário para subir para o sistema.  
  - **Usar o CPF apenas para enriquecimento** dos clientes.  

#### Exemplo de protocolos duplicados:  

| Protocolo | Valor   | DtAnuencia | Campanha   | Devedor                                   | CpfCnpj             | Contador | Custas | Credor                                           |  
|-----------|---------|------------|------------|-------------------------------------------|---------------------|----------|--------|-------------------------------------------------|  
| 43942775  | 4841,72 | 12/06/2023 | Campanha14 | POWER GREEN CONSULTORIA TÉCNICA LTDA      | 30.152.092/0001-74  | 18       | 530,72 | FAZENDA NACIONAL DIV ATIVA CONTRIBUICAO SOC     |  
| 43942775  | N/D     | 12/06/2023 | Campanha14 | HELO S/A                                   | 112.592.296-04      | 14       | 530,72 | FAZENDA NACIONAL DIV ATIVA CONTRIBUICAO SOC     |  

**Obs:** Se caso você precisar saber o porquê deles virem duplicados, você pode tirar sua dúvida com o Rodrigo ou propriamente comigo (Álvaro), para não render esse passo a passo.  

---

## **6. Layout de importação**  
Na importação, os campos devem ser mapeados da seguinte forma:  

- **Chave → NUMERO CONTRATO**  
- **Chave → PARCELA**  
- **Devedor → NOME / RAZÃO SOCIAL**  
- **DtAnuencia → VENCIMENTO**  
- **CpfCnpj → CPF/CNPJ**  
- **Custas → VALOR**  
- **Credor → OBSERVAÇÃO CONTRATO**  
- Criar uma coluna Id_negociador que vai ficar em branco
---

## **7. Baixa**
- A baixa será feita **posteriormente apenas por pagamentos**.
- O foco inicial deve estar no **batimento para importação no sistema**.
- Quando o batimento estiver validado e em funcionamento, a baixa será tratada em separado.
