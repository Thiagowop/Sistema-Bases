from __future__ import annotations

import os
from typing import Any, Dict

from src.config.loader import LoadedConfig

SQL_MAX_TEMPLATE = """
SELECT DISTINCT
    dbo.RetornaNomeCampanha(MoCampanhasID,1) AS CAMPANHA,
    dbo.RetornaNomeRazaoSocial(MoClientesID) AS CREDOR,
    dbo.RetornaCPFCNPJ(MoClientesID,1) AS CNPJ_CREDOR,
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS CPFCNPJ_CLIENTE,
    dbo.RetornaNomeRazaoSocial(MoInadimplentesID) AS NOME_RAZAO_SOCIAL,
    MoContrato AS NUMERO_CONTRATO,
    MoMatricula AS EMPREENDIMENTO,
    CAST(MoDataCriacaoRegistro AS DATE) AS DATA_CADASTRO,
    MoNumeroDocumento AS PARCELA,
    MoDataVencimento AS VENCIMENTO,
    MoValorDocumento AS VALOR,
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS STATUS_TITULO,
    MoTipoDocumento AS TIPO_PARCELA
FROM Movimentacoes
WHERE
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1)
    AND MoClientesID = {mo_cliente_id}
    AND MoOrigemMovimentacao IN ('C', 'I')
    {extra_conditions}
ORDER BY MoDataVencimento ASC
"""

SQL_AUTOJUR_TEMPLATE = """
SELECT DISTINCT
    [cpf_cnpj_parte_contraria_principal] AS CPF_CNPJ,
    'AUTOJUR' AS ORIGEM
FROM [Autojur].[dbo].[Pastas_New]
WHERE
    grupo_empresarial = '{grupo_empresarial}'
    AND numero_cnj <> ''
    AND numero_cnj <> 'none'
    AND cpf_cnpj_parte_contraria_principal <> ''
    AND cpf_cnpj_parte_contraria_principal IS NOT NULL
"""

SQL_MAXSMART_JUDICIAL_TEMPLATE = """
SELECT DISTINCT
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS CPF_CNPJ,
    'MAX_SMART' AS ORIGEM
FROM Movimentacoes
WHERE
    MoCampanhasID = {campanhas_id}
    AND dbo.RetornaCPFCNPJ(MoInadimplentesID,1) IS NOT NULL
    AND dbo.RetornaCPFCNPJ(MoInadimplentesID,1) <> ''
"""

SQL_DOUBLECHECK_ACORDO_TEMPLATE = """
SELECT DISTINCT
    dbo.RetornaNomeCampanha(MoCampanhasID,1) AS CAMPANHA,
    dbo.RetornaNomeRazaoSocial(MoClientesID) AS CREDOR,
    dbo.RetornaCPFCNPJ(MoClientesID,1) AS CNPJ_CREDOR,
    dbo.RetornaCPFCNPJ(MoInadimplentesID,1) AS CPFCNPJ_CLIENTE,
    dbo.RetornaNomeRazaoSocial(MoInadimplentesID) AS NOME_RAZAO_SOCIAL,
    MoContrato AS NUMERO_CONTRATO,
    MoObservacao AS OBSERVACAO_CONTRATO,
    MoMatricula AS EMPREENDIMENTO,
    CAST(MoDataCriacaoRegistro AS DATE) AS DATA_CADASTRO,
    CAST(MoDataRecebimento AS DATE) AS DATA_RECEBIMENTO,
    MoNumeroDocumento AS PARCELA,
    MoDataVencimento AS VENCIMENTO,
    MoValorDocumento AS VALOR,
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS STATUS_TITULO,
    MoDataDevolucao AS DATA_DEVOLUCAO_MANUAL,
    MovVariaveis8 AS DATA_DEVOLUCAO_MASSA,
    MoTipoDocumento AS TIPO_PARCELA
FROM Movimentacoes
INNER JOIN Pessoas ON MoInadimplentesID = Pessoas_ID
WHERE
    MoStatusMovimentacao IN (0)
    AND MoClientesID = 77398
    AND MoNumeroDocumento LIKE 'AC%'
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""


def get_query(config: LoadedConfig, name: str) -> str:
    template, params = _resolve_template_and_params(config, name)
    return template.format(**params)


def _resolve_template_and_params(config: LoadedConfig, name: str) -> tuple[str, Dict[str, Any]]:
    templates = {
        'max': SQL_MAX_TEMPLATE,
        'autojur': SQL_AUTOJUR_TEMPLATE,
        'maxsmart_judicial': SQL_MAXSMART_JUDICIAL_TEMPLATE,
        'doublecheck_acordo': SQL_DOUBLECHECK_ACORDO_TEMPLATE,
    }

    template = templates.get(name)
    if not template:
        raise KeyError(f"Template de query nao registrado: {name}")

    params = config.get('queries', {}).get(name, {}).get('params', {}).copy()
    filters = config.get('queries', {}).get(name, {}).get('filters', {})
    
    # Monta condições extras de filtro de data (se configuradas)
    extra_conditions: list[str] = []
    venc = filters.get('vencimento', {})
    start_env = venc.get('start_env')
    end_env = venc.get('end_env')
    
    if start_env:
        start_value = os.getenv(start_env)
        if start_value:
            extra_conditions.append(f"AND MoDataVencimento >= '{start_value}'")
    
    if end_env:
        end_value = os.getenv(end_env)
        if end_value:
            extra_conditions.append(f"AND MoDataVencimento <= '{end_value}'")

    params['extra_conditions'] = '\n    '.join(extra_conditions) if extra_conditions else ''
    return template, params
