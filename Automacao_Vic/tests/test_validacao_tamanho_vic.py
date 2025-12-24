#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Teste de validação de tamanho de arquivo da base VIC.

Este teste valida se o sistema está detectando corretamente bases 
com tamanho incorreto (muito abaixo do esperado).
"""

import sys
from pathlib import Path

# Configuração de path
BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

def test_tamanho_arquivos():
    """Verifica os tamanhos dos arquivos de teste."""
    
    # Arquivo de teste com erro (candiotto (3).zip)
    arquivo_erro = BASE / "tests" / "candiotto (3).zip"
    
    # Arquivo correto de referência
    arquivo_correto = BASE / "data" / "input" / "vic" / "VicCandiotto.zip"
    
    print("="*70)
    print("TESTE DE VALIDAÇÃO DE TAMANHO - BASE VIC")
    print("="*70)
    print()
    
    # Verifica arquivo com erro
    if arquivo_erro.exists():
        tamanho_erro_bytes = arquivo_erro.stat().st_size
        tamanho_erro_kb = tamanho_erro_bytes / 1024
        tamanho_erro_mb = tamanho_erro_bytes / (1024 * 1024)
        
        print(f"[TESTE] Arquivo com ERRO detectado:")
        print(f"        Caminho: {arquivo_erro}")
        print(f"        Tamanho: {tamanho_erro_kb:.2f} KB ({tamanho_erro_mb:.4f} MB)")
        print()
    else:
        print(f"[AVISO] Arquivo de teste não encontrado: {arquivo_erro}")
        print()
    
    # Verifica arquivo correto
    if arquivo_correto.exists():
        tamanho_correto_bytes = arquivo_correto.stat().st_size
        tamanho_correto_kb = tamanho_correto_bytes / 1024
        tamanho_correto_mb = tamanho_correto_bytes / (1024 * 1024)
        
        print(f"[TESTE] Arquivo CORRETO de referência:")
        print(f"        Caminho: {arquivo_correto}")
        print(f"        Tamanho: {tamanho_correto_kb:.2f} KB ({tamanho_correto_mb:.2f} MB)")
        print()
    else:
        print(f"[AVISO] Arquivo de referência não encontrado: {arquivo_correto}")
        print()
    
    # Análise comparativa
    if arquivo_erro.exists() and arquivo_correto.exists():
        diferenca_mb = tamanho_correto_mb - tamanho_erro_mb
        percentual = (tamanho_erro_mb / tamanho_correto_mb) * 100
        
        print("[ANÁLISE] Comparação:")
        print(f"        Diferença: {diferenca_mb:.2f} MB")
        print(f"        Arquivo com erro representa {percentual:.2f}% do tamanho esperado")
        print()
        
        # Validação baseada no config.yaml (min_file_size_mb: 1.0)
        tamanho_minimo_mb = 1.0
        
        print("[VALIDAÇÃO] Teste com tamanho mínimo de 1.0 MB:")
        
        if tamanho_erro_mb < tamanho_minimo_mb:
            print(f"        ✓ Arquivo com erro ({tamanho_erro_mb:.4f} MB) REPROVADO corretamente")
        else:
            print(f"        ✗ Arquivo com erro ({tamanho_erro_mb:.4f} MB) NÃO seria detectado")
        
        if tamanho_correto_mb >= tamanho_minimo_mb:
            print(f"        ✓ Arquivo correto ({tamanho_correto_mb:.2f} MB) APROVADO corretamente")
        else:
            print(f"        ✗ Arquivo correto ({tamanho_correto_mb:.2f} MB) seria rejeitado incorretamente")
        
        print()
    
    print("="*70)
    print("[INFO] Teste concluído")
    print("="*70)
    print()
    print("PRÓXIMOS PASSOS:")
    print("  1. Verificar config.yaml - parâmetro 'validation.min_file_size_mb'")
    print("  2. Testar extração de email com o arquivo de teste")
    print("  3. Confirmar que logs são gerados em data/logs/extracao_email_erros.log")
    print()


if __name__ == "__main__":
    test_tamanho_arquivos()
