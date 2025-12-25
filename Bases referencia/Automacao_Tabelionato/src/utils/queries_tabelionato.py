#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consultas SQL - Projeto Tabelionato

Consultas SQL para extrao de dados dos bancos MAX para o contexto do tabelionato.
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
    MoNumeroDocumento AS 'PARCELA',
    Movimentacoes_ID AS 'Movimentacoes_ID',
    MoDataVencimento AS 'VENCIMENTO',
    MoValorDocumento AS 'VALOR',
    dbo.RetornaStatusMovimentacao(MoStatusMovimentacao) AS 'STATUS_TITULO'
FROM Movimentacoes
    INNER JOIN Pessoas ON MoInadimplentesID = Pessoas_ID
WHERE
    (MoStatusMovimentacao = 0 OR MoStatusMovimentacao = 1) -- Filtrar status dos ttulos 0 = Aberto e 1 = Liquidado com acordo
    AND MoClientesID = 2746 -- Filtrar Tabelionato
    AND MoOrigemMovimentacao in ('C', 'I') -- Filtrar ttulos originais C = Cadastrado manualmente e I = importado
    AND MoCampanhasID IN (60,80,96) -- Filtrar apenas campanhas 000058, 000078 e 000096 do Tabelionato
ORDER BY dbo.RetornaCPFCNPJ(MoInadimplentesID,1), MoDataVencimento ASC
"""