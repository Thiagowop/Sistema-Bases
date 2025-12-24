#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Teste de Simulação de Extração de Email - Validação de Tamanho VIC

Simula o processo de extração de anexos de email, testando:
1. Email com base ERRADA (28/out/2024 - 76 KB)
2. Email com base CORRETA (data atual - 14 MB)

Demonstra a eficácia da validação de tamanho implementada.
"""

import sys
import shutil
from datetime import datetime
from pathlib import Path

# Configuração de path
BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(BASE / "src"))

from src.config.loader import load_cfg


def simular_extracao_email(arquivo_origem: Path, data_email: str, descricao: str, email_info: dict):
    """
    Simula o processo de extração de um anexo de email.
    
    Args:
        arquivo_origem: Caminho do arquivo que simula o anexo baixado
        data_email: Data do email para simulação
        descricao: Descrição do teste (ex: "Base ERRADA")
        email_info: Informações do email simulado
    """
    print("\n" + "="*70)
    print(f"  SIMULAÇÃO: {descricao}")
    print("="*70)
    
    # Informações do email simulado
    print(f"\n[EMAIL] Informações:")
    print(f"        Remetente: {email_info['remetente']}")
    print(f"        Assunto  : {email_info['assunto']}")
    print(f"        Data     : {email_info['data']}")
    print()
    
    # Verificar se arquivo existe
    if not arquivo_origem.exists():
        print(f"[ERRO] Arquivo de teste não encontrado: {arquivo_origem}")
        return False
    
    # Simular download copiando para pasta de destino temporária
    destino_temp = BASE / "data" / "input" / "vic" / f"VicCandiotto_TESTE_{descricao.replace(' ', '_')}.zip"
    destino_temp.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Simulando download do anexo...")
    print(f"        Origem : {arquivo_origem.name}")
    print(f"        Destino: {destino_temp.name}")
    
    # Copiar arquivo
    shutil.copy2(arquivo_origem, destino_temp)
    
    # Calcular tamanho
    tamanho_bytes = destino_temp.stat().st_size
    tamanho_kb = tamanho_bytes / 1024
    tamanho_mb = tamanho_bytes / (1024 * 1024)
    
    print(f"\n[ARQUIVO] Informações:")
    print(f"        Tamanho: {tamanho_kb:.2f} KB ({tamanho_mb:.4f} MB)")
    print(f"        Salvo em: {destino_temp}")
    
    # Carregar configuração
    config = load_cfg()
    email_cfg = config.get('email', {})
    validation_cfg = email_cfg.get('validation', {})
    min_size_mb = validation_cfg.get('min_file_size_mb', 0)
    
    print(f"\n[VALIDAÇÃO] Verificando tamanho mínimo...")
    print(f"        Tamanho mínimo configurado: {min_size_mb:.2f} MB")
    print(f"        Tamanho recebido: {tamanho_mb:.4f} MB")
    
    # Validação de tamanho (simulando a lógica do extrair_email.py)
    validacao_ok = True
    
    if min_size_mb > 0 and tamanho_mb < min_size_mb:
        validacao_ok = False
        print(f"\n{'='*70}")
        print("  ❌ [VALIDAÇÃO REPROVADA] BASE COM INCONFORMIDADE DETECTADA")
        print(f"{'='*70}")
        print(f"[ERRO] O arquivo possui tamanho MUITO ABAIXO do esperado!")
        print(f"[ERRO] Tamanho recebido: {tamanho_mb:.4f} MB")
        print(f"[ERRO] Tamanho mínimo esperado: {min_size_mb:.2f} MB")
        print(f"[ERRO] Diferença: {min_size_mb - tamanho_mb:.4f} MB abaixo do esperado")
        print(f"[ERRO] ")
        print(f"[ERRO] POSSÍVEIS CAUSAS:")
        print(f"[ERRO]   - Base enviada com formato incorreto")
        print(f"[ERRO]   - Arquivo corrompido ou incompleto")
        print(f"[ERRO]   - Email com anexo errado")
        print(f"[ERRO] ")
        print(f"[ERRO] AÇÃO NECESSÁRIA:")
        print(f"[ERRO]   - Verificar manualmente o arquivo")
        print(f"[ERRO]   - Contatar remetente: {email_info['remetente']}")
        print(f"[ERRO]   - Solicitar reenvio da base correta")
        print(f"{'='*70}")
        
        # Simular log de erro
        logs_dir = BASE / "data" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / "extracao_email_erros_TESTE.log"
        
        with open(log_file, 'a', encoding='utf-8-sig') as f:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"\n{'='*80}\n")
            f.write(f"[{timestamp}] TESTE SIMULADO - ERRO CRITICO - BASE COM INCONFORMIDADE\n")
            f.write(f"{'='*80}\n")
            f.write(f"Descricao do teste: {descricao}\n")
            f.write(f"Arquivo: {destino_temp}\n")
            f.write(f"Tamanho recebido: {tamanho_mb:.4f} MB\n")
            f.write(f"Tamanho minimo esperado: {min_size_mb:.2f} MB\n")
            f.write(f"Diferenca: {min_size_mb - tamanho_mb:.4f} MB abaixo do esperado\n")
            f.write(f"\nInformacoes do e-mail (simulado):\n")
            f.write(f"  Remetente: {email_info['remetente']}\n")
            f.write(f"  Assunto: {email_info['assunto']}\n")
            f.write(f"  Data: {email_info['data']}\n")
            f.write(f"\nAcao necessaria: Verificar arquivo e solicitar reenvio da base\n")
            f.write(f"{'='*80}\n")
        
        print(f"\n[LOG] Erro registrado em: {log_file}")
    else:
        print(f"\n{'='*70}")
        print("  ✅ [VALIDAÇÃO APROVADA] BASE COM TAMANHO ADEQUADO")
        print(f"{'='*70}")
        print(f"[OK] Arquivo atende ao tamanho mínimo configurado")
        print(f"[OK] Processamento pode continuar normalmente")
        print(f"{'='*70}")
    
    # Limpar arquivo temporário
    print(f"\n[LIMPEZA] Removendo arquivo temporário de teste...")
    destino_temp.unlink()
    
    print(f"\n[RESULTADO] Status da validação: {'❌ REPROVADO' if not validacao_ok else '✅ APROVADO'}")
    print("="*70)
    
    return validacao_ok


def main():
    """Executa simulação completa de extração com ambos os cenários."""
    
    print("\n" + "="*70)
    print("  TESTE DE SIMULAÇÃO - EXTRAÇÃO DE EMAIL COM VALIDAÇÃO")
    print("="*70)
    print("\nEste teste simula o processo de extração de anexos de email")
    print("demonstrando a eficácia da validação de tamanho implementada.")
    print()
    print("Cenários testados:")
    print("  1. Email com base ERRADA (28/out/2024 - ~76 KB)")
    print("  2. Email com base CORRETA (data atual - ~14 MB)")
    print("="*70)
    
    # Cenário 1: Base ERRADA (28/out/2024)
    arquivo_erro = BASE / "tests" / "candiotto (3).zip"
    email_info_erro = {
        'remetente': 'Asti - Candioto <noreply@fcleal.com.br>',
        'assunto': 'Envio automático planilha Candiotto',
        'data': 'ter., 28 de out., 12:27'  # Data do email com erro
    }
    
    resultado_erro = simular_extracao_email(
        arquivo_erro,
        "28/10/2024",
        "Base ERRADA (28/out)",
        email_info_erro
    )
    
    # Pausa visual
    print("\n\n")
    input("Pressione ENTER para continuar com o próximo teste...")
    print("\n")
    
    # Cenário 2: Base CORRETA (hoje)
    arquivo_correto = BASE / "data" / "input" / "vic" / "VicCandiotto.zip"
    hoje = datetime.now()
    email_info_correto = {
        'remetente': 'Asti - Candioto <noreply@fcleal.com.br>',
        'assunto': 'Envio automático planilha Candiotto',
        'data': hoje.strftime('%a., %d de %b., %H:%M')  # Data atual
    }
    
    resultado_correto = simular_extracao_email(
        arquivo_correto,
        hoje.strftime('%d/%m/%Y'),
        "Base CORRETA (hoje)",
        email_info_correto
    )
    
    # Resumo final
    print("\n\n")
    print("="*70)
    print("  RESUMO DOS TESTES")
    print("="*70)
    print()
    print(f"  Teste 1 - Base ERRADA (28/out):    {'❌ REPROVADO (esperado)' if not resultado_erro else '❓ APROVADO (inesperado)'}")
    print(f"  Teste 2 - Base CORRETA (hoje):     {'✅ APROVADO (esperado)' if resultado_correto else '❓ REPROVADO (inesperado)'}")
    print()
    
    if not resultado_erro and resultado_correto:
        print("  🎉 SUCESSO! Validação funcionando perfeitamente!")
        print("     - Detectou corretamente a base com erro")
        print("     - Aprovou corretamente a base válida")
    else:
        print("  ⚠️  ATENÇÃO! Resultado inesperado na validação")
        print("     - Verificar configuração em config.yaml")
        print("     - Valor de 'validation.min_file_size_mb' pode estar incorreto")
    
    print()
    print("="*70)
    print("\nArquivos verificados:")
    print(f"  - Base erro: {arquivo_erro}")
    print(f"  - Base correta: {arquivo_correto}")
    print(f"  - Log de teste: data/logs/extracao_email_erros_TESTE.log")
    print()
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INFO] Teste interrompido pelo usuário.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERRO] Falha durante o teste: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
