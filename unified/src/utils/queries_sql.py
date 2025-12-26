#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consultas SQL - Projeto VIC

Consultas SQL para extração de dados dos bancos MAX e Judicial.
"""

# Query principal do MAX (STD2016)
SQL_MAX = """
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
    -- Filtrar status dos títulos 0 = Aberto e 1 = Liquidado com acordo
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1) 
    AND MoClientesID = 232 -- Filtrar Vic Engenharia 
    -- Filtrar títulos originais C = Cadastrado manualmente e I = importado
    AND MoOrigemMovimentacao in ('C', 'I') 
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""

# Query principal do VIC (STD2016)
SQL_VIC = """
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
    -- Filtrar status dos títulos 0 = Aberto e 1 = Liquidado com acordo
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1) 
    AND MoClientesID = 232 -- Filtrar Vic Engenharia 
    -- Filtrar títulos originais C = Cadastrado manualmente e I = importado
    AND MoOrigemMovimentacao in ('C', 'I') 
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""

# Query para dados judiciais - Autojur
SQL_AUTOJUR = """
SELECT DISTINCT 
    [cpf_cnpj_parte_contraria_principal] as CPF_CNPJ,
    'AUTOJUR' as ORIGEM
FROM [Autojur].[dbo].[Pastas_New] 
WHERE 
    grupo_empresarial = 'Vic Engenharia' 
    AND numero_cnj <> '' 
    AND numero_cnj <> 'none' 
    AND cpf_cnpj_parte_contraria_principal <> '' 
    AND cpf_cnpj_parte_contraria_principal IS NOT NULL
"""

# Query para dados judiciais - MaxSmart
SQL_MAXSMART_JUDICIAL = """
SELECT DISTINCT 
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) as CPF_CNPJ,
    'MAX_SMART' as ORIGEM
FROM Movimentacoes 
WHERE 
    MoCampanhasID = 4
    AND dbo.RetornaCPFCNPJ(MoInadimplentesID,1) IS NOT NULL
    AND dbo.RetornaCPFCNPJ(MoInadimplentesID,1) <> ''
"""

# Query de teste de conexão
SQL_TEST = "SELECT 1 as test"

# Dicionário com todas as queries
QUERIES = {
    'max': SQL_MAX,
    'vic': SQL_VIC,
    'autojur': SQL_AUTOJUR,
    'maxsmart_judicial': SQL_MAXSMART_JUDICIAL,
    'test': SQL_TEST
}

def get_query(query_name: str) -> str:
    """Retorna uma query pelo nome."""
    return QUERIES.get(query_name, '')
