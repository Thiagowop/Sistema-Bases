#!/usr/bin/env python3
"""CLI unificada para processamento do pipeline VIC/MAX."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.config.loader import load_cfg
from src.processors import (
    VicProcessor,
    MaxProcessor,
    DevolucaoProcessor,
    BatimentoProcessor,
)
from src.utils.logger import get_logger


TAG_PREFIX_RE = re.compile(r"^\[[^\]]+\]\s*")


SUMMARY_FIELDS = [
    ("anexos_encontrados", "📥", "Anexos encontrados"),
    ("anexos_baixados", "📥", "Anexos baixados"),
    ("registros", "📊", "Total de registros extraídos"),
    ("arquivo", "📁", "Arquivo salvo em"),
    ("tempo", "⏱️", "Tempo de execução"),
    ("email_data", "📅", "Data/hora do e-mail"),
]


def _clean_line(line: str) -> str:
    """Remove prefixos de tags e espaços extras de uma linha."""

    return TAG_PREFIX_RE.sub("", line).strip()


def _extract_value(line: str) -> str:
    if ":" not in line:
        return ""
    return line.split(":", 1)[1].strip()


def _parse_summary(stdout: str) -> tuple[dict[str, str], list[str]]:
    summary: dict[str, str] = {}
    warnings: list[str] = []

    for raw_line in stdout.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue

        cleaned = _clean_line(stripped)
        if not cleaned:
            continue

        if all(char == "=" for char in cleaned):
            continue

        lower = cleaned.lower()

        if "[aviso]" in raw_line.lower():
            warnings.append(cleaned)

        if "anexos encontrados" in lower:
            summary["anexos_encontrados"] = _extract_value(cleaned)
            continue

        if "anexos baixados" in lower:
            summary["anexos_baixados"] = _extract_value(cleaned)
            continue

        if any(keyword in lower for keyword in ("registros extra", "registros encontrados", "registros únicos")):
            summary["registros"] = _extract_value(cleaned)
            continue

        if any(keyword in lower for keyword in ("arquivo salvo", "arquivo gerado", "caminho")):
            value = _extract_value(cleaned)
            if value:
                summary["arquivo"] = value
            continue

        if "tempo de execução" in lower:
            summary["tempo"] = _extract_value(cleaned)
            continue

        if "data/hora" in lower:
            summary["email_data"] = _extract_value(cleaned)

    return summary, warnings


def _run_extraction(
    script_name: str,
    exec_description: str,
    display_name: str,
    project_root: Path,
) -> bool:
    env = dict(os.environ)
    src_path = str(project_root / 'src')
    if 'PYTHONPATH' in env:
        env['PYTHONPATH'] = f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env['PYTHONPATH'] = src_path
    env['PYTHONIOENCODING'] = 'utf-8'
    env['PYTHONUTF8'] = '1'
    script_path = project_root / 'scripts' / script_name
    if not script_path.exists():
        print(f"❌ {display_name} - Script não encontrado: {script_path}")
        return False
    print(f"Executando {exec_description}...")
    result = subprocess.run([sys.executable, str(script_path)], cwd=project_root, env=env, text=True, capture_output=True, encoding='utf-8', errors='replace')
    stdout = result.stdout or ""
    stderr = result.stderr or ""

    if result.returncode == 0:
        summary, warnings = _parse_summary(stdout)
        print(f"✅ {display_name} - Extração concluída com sucesso")
        for key, emoji, label in SUMMARY_FIELDS:
            value = summary.get(key)
            if value:
                print(f"   {emoji} {label}: {value}")
        if warnings:
            print("   ⚠️ Avisos:")
            for warning in warnings:
                print(f"      - {warning}")
        print()
        return True

    print(f"❌ {display_name} - Falha na extração (código {result.returncode})")
    if stdout.strip():
        print("   📄 Saída (stdout):")
        for line in stdout.splitlines():
            line = line.rstrip()
            if line:
                print(f"      {line}")
    if stderr.strip():
        print("   ⚠️ Saída (stderr):")
        for line in stderr.splitlines():
            line = line.rstrip()
            if line:
                print(f"      {line}")
    print()
    return False



def main() -> bool:
    """Ponto de entrada para a CLI."""
    parser = argparse.ArgumentParser(
        description="CLI unificada para processamento VIC/MAX"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    vic_parser = subparsers.add_parser("vic", help="Processa dados VIC")
    vic_parser.add_argument(
        "entrada",
        nargs="?",
        help="Arquivo CSV ou ZIP (opcional - extrai do banco se omitido)",
    )
    vic_parser.add_argument("-o", "--output", help="Diretório de saída")
    vic_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        help="Não adicionar timestamp aos arquivos de saída",
    )

    max_parser = subparsers.add_parser("max", help="Processa dados MAX")
    max_parser.add_argument(
        "entrada",
        nargs="?",
        help="Arquivo CSV ou ZIP (opcional - extrai do banco se omitido)",
    )
    max_parser.add_argument("-o", "--output", help="Diretório de saída")
    max_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        help="Não adicionar timestamp aos arquivos de saída",
    )

    dev_parser = subparsers.add_parser("devolucao", help="Processa devolução")
    dev_parser.add_argument("vic_file", help="Arquivo VIC tratado")
    dev_parser.add_argument("max_file", help="Arquivo MAX tratado")
    dev_parser.add_argument("-o", "--output", help="Diretório de saída")
    dev_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        help="Não adicionar timestamp aos arquivos de saída",
    )

    bat_parser = subparsers.add_parser("batimento", help="Processa batimento")
    extrair_parser = subparsers.add_parser("extrair", help="Executa extrações de dados")
    extrair_parser.add_argument("tipo", choices=["email", "max", "judicial", "todas"], help="Tipo de extração a executar")

    bat_parser.add_argument("vic_file", help="Arquivo VIC tratado")
    bat_parser.add_argument("max_file", help="Arquivo MAX tratado")
    bat_parser.add_argument("-o", "--output", help="Diretório de saída")
    bat_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        help="Não adicionar timestamp aos arquivos de saída",
    )

    args = parser.parse_args()

    config = load_cfg()
    logger = get_logger("pipeline_cli", config)

    if args.command == "vic":
        processor = VicProcessor(config=config, logger=logger)
        if args.no_timestamp:
            processor.add_timestamp = False
        entrada = Path(args.entrada) if args.entrada else None
        saida = Path(args.output) if args.output else None
        resultado = processor.processar(entrada, saida)
        logger.info("VIC OK")
        print(f"Arquivo gerado: {resultado['arquivo_gerado']}")
        return True

    if args.command == "max":
        processor = MaxProcessor(config=config, logger=logger)
        if args.no_timestamp:
            processor.add_timestamp = False
        entrada = Path(args.entrada) if args.entrada else None
        saida = Path(args.output) if args.output else None
        resultado = processor.processar(entrada, saida)
        logger.info("MAX OK")
        print(f"Arquivo gerado: {resultado['arquivo_gerado']}")
        return True

    if args.command == "devolucao":
        processor = DevolucaoProcessor(config=config, logger=logger)
        if args.no_timestamp:
            processor.add_timestamp = False
        resultado = processor.processar(Path(args.vic_file), Path(args.max_file))
        logger.info("DEVOLUCAO OK")
        print(f"Arquivo gerado: {resultado.get('arquivo_gerado')}")
        return True

    if args.command == "batimento":
        processor = BatimentoProcessor(config=config, logger=logger)
        if args.no_timestamp:
            processor.add_timestamp = False
        saida = Path(args.output) if args.output else None
        resultado = processor.processar(Path(args.vic_file), Path(args.max_file), saida)
        logger.info("BATIMENTO OK")
        print(f"Arquivo gerado: {resultado.get('arquivo_gerado')}")
        print(
            f"Judicial: {resultado.get('judicial')} | Extrajudicial: {resultado.get('extrajudicial')}"
        )
        return True

    if args.command == "extrair":
        scripts_map = {
            "email": ("extrair_email.py", "extração VIC (Email)", "VIC (Email)"),
            "max": ("extrair_basemax.py", "extração MAX (DB)", "MAX (DB)"),
            "judicial": ("extrair_judicial.py", "extração Judicial (DB)", "Judicial (DB)"),
        }
        tipos = scripts_map.keys() if args.tipo == "todas" else [args.tipo]
        sucesso = True
        for tipo in tipos:
            script, exec_desc, label = scripts_map[tipo]
            ok = _run_extraction(script, exec_desc, label, project_root)
            sucesso &= ok
            if not ok:
                break
        return sucesso

    return False


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
