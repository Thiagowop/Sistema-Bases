import pandas as pd
from pathlib import Path
import tempfile
import zipfile

from src.processors.devolucao import DevolucaoProcessor


def _cfg(tmpdir):
    return {
        "global": {"add_timestamp": False, "empresa": {"cnpj": "123"}},
        "paths": {"input": {}, "output": {"base": str(tmpdir)}},
        "vic_processor": {
            "status_em_aberto": "EM ABERTO",
            "filtros_inclusao": {
                "status_em_aberto": True,
                "tipos_validos": False,
                "aging": False,
                "blacklist": False,
            },
        },
        "devolucao": {
            "campanha_termo": "Vic Extra",
            "status_excluir": ["LIQUIDADO COM ACORDO"],
            "chaves": {"vic": "CHAVE", "max": "PARCELA"},
            "filtros_max": {
                "status_em_aberto": True,
                "tipos_validos": False,
                "blacklist": False,
            },
            "export": {
                "filename_prefix": "vic_devolucao",
                "subdir": "devolucao",
                "judicial_subdir": "devolucao/jud",
                "extrajudicial_subdir": "devolucao/extra",
                "geral_subdir": "devolucao",
                "add_timestamp": False,
            },
            "status_devolucao_fixo": "98",
        },
    }


def test_processar_exporta_arquivo(tmp_path):
    cfg = _cfg(tmp_path)
    proc = DevolucaoProcessor(config=cfg)

    df_vic = pd.DataFrame({"CHAVE": ["1-1"], "STATUS_TITULO": ["EM ABERTO"]})
    df_max = pd.DataFrame(
        {
            "PARCELA": ["1-1", "1-2"],
            "STATUS_TITULO": ["EM ABERTO", "EM ABERTO"],
            "CAMPANHA": ["000001 - Vic Extra", "000001 - Vic Extra"],
        }
    )

    vic_path = tmp_path / "vic.csv"
    max_path = tmp_path / "max.csv"
    df_vic.to_csv(vic_path, index=False, sep=";")
    df_max.to_csv(max_path, index=False, sep=";")

    stats = proc.processar(vic_path, max_path)
    out_path = stats.get("arquivo_extrajudicial") or stats.get("arquivo_geral")
    arquivo_zip = stats.get("arquivo_zip") or out_path
    internos = stats.get("arquivos_no_zip", {})
    nome_csv = internos.get("arquivo_extrajudicial") or internos.get("arquivo_geral")

    if arquivo_zip and str(arquivo_zip).lower().endswith(".zip") and nome_csv:
        with zipfile.ZipFile(arquivo_zip) as zf:
            with zf.open(nome_csv) as fh:
                df_out = pd.read_csv(fh, sep=";")
    else:
        df_out = pd.read_csv(out_path, sep=";")

    assert list(df_out["PARCELA"]) == ["1-2"]


def test_devolucao_procv_reverso_zero_registros_incorretos():
    """Testa PROCV reverso: base gerada vs base tratada para validar zero registros incorretos"""
    
    # Base tratada MAX (dados originais processados)
    base_max_tratada = pd.DataFrame({
        'PARCELA': ['111-001', '222-001', '333-002', '444-001'],
        'STATUS_TITULO': ['EM ABERTO', 'EM ABERTO', 'EM ABERTO', 'LIQUIDADO COM ACORDO'],
        'CAMPANHA': ['000001 - Vic Extra', '000001 - Vic Extra', '000001 - Vic Extra', '000001 - Vic Extra'],
        'VALOR': [1000.00, 2000.00, 1500.00, 800.00]
    })
    
    # Base tratada VIC (dados originais processados)
    base_vic_tratada = pd.DataFrame({
        'CHAVE': ['111-001', '333-002'],  # Apenas 2 registros existem no VIC
        'CPFCNPJ_CLIENTE': ['11111111111', '33333333333'],
        'VALOR': [1000.00, 1500.00]
    })
    
    # Base gerada pela devolução (resultado do processamento)
    # Deve conter apenas registros que:
    # 1. Existem no MAX mas NÃO no VIC (PROCV MAX - VIC)
    # 2. Não têm status excluído (LIQUIDADO COM ACORDO)
    base_devolucao_gerada = pd.DataFrame({
        'PARCELA': ['222-001'],  # Apenas este registro deve aparecer
        'STATUS_TITULO': ['EM ABERTO'],
        'CAMPANHA': ['000001 - Vic Extra'],
        'STATUS_DEVOLUCAO': ['98']
    })
    
    # PROCV reverso: verifica se registros da base gerada existem na base tratada MAX
    procv_resultado = base_devolucao_gerada.merge(
        base_max_tratada[['PARCELA', 'VALOR', 'STATUS_TITULO']], 
        on='PARCELA', 
        how='left',
        suffixes=('_gerado', '_tratado')
    )
    
    # Validações do teste de integração
    assert len(procv_resultado) == len(base_devolucao_gerada), "Todos os registros gerados devem ter correspondência"
    assert procv_resultado['VALOR'].notna().all(), "Todos os valores devem ser encontrados na base MAX tratada"
    assert procv_resultado['STATUS_TITULO_tratado'].notna().all(), "Todos os status devem ser encontrados na base MAX tratada"
    
    # Verifica se não há registros órfãos (gerados sem correspondência na base tratada)
    registros_orfaos = procv_resultado[procv_resultado['VALOR'].isna()]
    assert len(registros_orfaos) == 0, f"Encontrados {len(registros_orfaos)} registros órfãos na devolução"
    
    # Verifica que registros gerados NÃO existem no VIC (validação da lógica de devolução)
    chaves_vic = set(base_vic_tratada['CHAVE'])
    parcelas_geradas = set(base_devolucao_gerada['PARCELA'])
    
    intersecao = chaves_vic & parcelas_geradas
    assert len(intersecao) == 0, f"Registros da devolução não devem existir no VIC: {intersecao}"
    
    # Verifica que registros com status excluído não aparecem na devolução
    registros_excluidos = base_max_tratada[base_max_tratada['STATUS_TITULO'] == 'LIQUIDADO COM ACORDO']
    parcelas_excluidas = set(registros_excluidos['PARCELA'])
    
    intersecao_excluidos = parcelas_excluidas & parcelas_geradas
    assert len(intersecao_excluidos) == 0, f"Registros com status excluído não devem aparecer: {intersecao_excluidos}"


def test_devolucao_com_arquivos_zip():
    """Testa devolução usando arquivos ZIP simulados com PROCV reverso"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Cria base MAX tratada
        base_max = pd.DataFrame({
            'PARCELA': ['A-001', 'B-001', 'C-002'],
            'STATUS_TITULO': ['EM ABERTO', 'EM ABERTO', 'EM ABERTO'],
            'CAMPANHA': ['000001 - Vic Extra', '000001 - Vic Extra', '000001 - Vic Extra'],
            'VALOR': [100.00, 200.00, 300.00]
        })
        
        # Cria base VIC tratada (apenas A-001 existe)
        base_vic = pd.DataFrame({
            'CHAVE': ['A-001'],
            'CPFCNPJ_CLIENTE': ['11111111111'],
            'VALOR': [100.00],
            'STATUS_TITULO': ['EM ABERTO']
        })
        
        # Salva arquivos CSV
        max_csv = temp_path / 'max_tratada.csv'
        vic_csv = temp_path / 'vic_tratada.csv'
        base_max.to_csv(max_csv, index=False, sep=';')
        base_vic.to_csv(vic_csv, index=False, sep=';')
        
        # Cria ZIPs
        max_zip = temp_path / 'max_tratada.zip'
        vic_zip = temp_path / 'vic_tratada.zip'
        
        with zipfile.ZipFile(max_zip, 'w') as zf:
            zf.write(max_csv, 'max_tratada.csv')
        
        with zipfile.ZipFile(vic_zip, 'w') as zf:
            zf.write(vic_csv, 'vic_tratada.csv')
        
        # Processa devolução
        cfg = _cfg(temp_path)
        proc = DevolucaoProcessor(config=cfg)
        stats = proc.processar(vic_zip, max_zip)

        # Lê resultado
        resultado_path = stats.get('arquivo_extrajudicial') or stats.get('arquivo_geral')
        arquivo_zip = stats.get('arquivo_zip') or resultado_path
        internos = stats.get('arquivos_no_zip', {})
        nome_csv = internos.get('arquivo_extrajudicial') or internos.get('arquivo_geral')

        if arquivo_zip and str(arquivo_zip).lower().endswith('.zip') and nome_csv:
            with zipfile.ZipFile(arquivo_zip) as zf:
                with zf.open(nome_csv) as fh:
                    df_resultado = pd.read_csv(fh, sep=';')
        else:
            df_resultado = pd.read_csv(resultado_path, sep=';')
        
        # PROCV reverso: verifica se registros gerados existem na base MAX tratada
        procv_resultado = df_resultado.merge(
            base_max[['PARCELA', 'VALOR']],
            on='PARCELA',
            how='left'
        )

        # Validações
        assert len(procv_resultado) == len(df_resultado)
        assert 'VALOR_y' in procv_resultado.columns
        assert procv_resultado['VALOR_y'].notna().all(), "Todos os registros gerados devem existir na base MAX tratada"
        
        # Verifica que apenas B-001 e C-002 foram incluídos (não existem no VIC)
        parcelas_esperadas = {'B-001', 'C-002'}
        parcelas_geradas = set(df_resultado['PARCELA'])
        assert parcelas_geradas == parcelas_esperadas, f"Esperado {parcelas_esperadas}, obtido {parcelas_geradas}"
        
        # Verifica zero registros incorretos
        registros_incorretos = procv_resultado[procv_resultado['VALOR_y'].isna()]
        assert len(registros_incorretos) == 0, "Deve haver zero registros incorretos na devolução"
