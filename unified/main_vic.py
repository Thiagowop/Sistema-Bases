#!/usr/bin/env python3
"""Orquestrador principal do Pipeline VIC/MAX refatorado.

Este é o entry point principal que coordena a execução de todos os processadores
com modo de comparação automática para validação de resultados.
"""

import argparse
import os
import sys
import re
import subprocess
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import pandas as pd

# Adicionar src ao path para importações
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Tentar forcar stdout em UTF-8 para evitar erros de encode no Windows/CP1252
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

from src.config.loader import ConfigLoader
from src.processors.vic.tratamento_vic import VicProcessor
from src.processors.vic.enriquecimento_vic import EnriquecimentoVicProcessor
from src.processors.shared.tratamento_max import MaxProcessor
from src.processors.shared.batimento import BatimentoProcessor
from src.processors.shared.baixa import BaixaProcessor
from src.processors.shared.devolucao import DevolucaoProcessor
from src.utils.logger import get_logger
from src.utils.validator import ValidadorConsistencia


_TAG_PREFIX_RE = re.compile(r"^\[[^\]]+\]\s*")

_SUMMARY_FIELDS = [
    ("anexos_encontrados", "📥", "Anexos encontrados"),
    ("anexos_baixados", "📥", "Anexos baixados"),
    ("registros", "📊", "Total de registros extraídos"),
    ("arquivo", "📁", "Arquivo salvo em"),
    ("tempo", "⏱️", "Tempo de execução"),
    ("email_data", "📅", "Data/hora do e-mail"),
]


def _clean_extraction_line(line: str) -> str:
    """Remove prefixos e espaços extras de uma linha de log de extração."""

    return _TAG_PREFIX_RE.sub("", line).strip()


def _extract_extraction_value(line: str) -> str:
    if ":" not in line:
        return ""
    return line.split(":", 1)[1].strip()


def _parse_extraction_summary(stdout: str) -> Tuple[Dict[str, str], list[str]]:
    resumo: Dict[str, str] = {}
    avisos: list[str] = []

    for linha in stdout.splitlines():
        trecho = linha.strip()
        if not trecho:
            continue

        limpa = _clean_extraction_line(trecho)
        if not limpa:
            continue

        if all(char == "=" for char in limpa):
            continue

        texto_minusculo = limpa.lower()

        if "[aviso]" in linha.lower():
            avisos.append(limpa)

        if "anexos encontrados" in texto_minusculo:
            resumo["anexos_encontrados"] = _extract_extraction_value(limpa)
            continue

        if "anexos baixados" in texto_minusculo:
            resumo["anexos_baixados"] = _extract_extraction_value(limpa)
            continue

        if any(
            palavra in texto_minusculo
            for palavra in ("registros extra", "registros encontrados", "registros únicos")
        ):
            resumo["registros"] = _extract_extraction_value(limpa)
            continue

        if any(
            palavra in texto_minusculo
            for palavra in ("arquivo salvo", "arquivo gerado", "caminho")
        ):
            valor = _extract_extraction_value(limpa)
            if valor:
                resumo["arquivo"] = valor
            continue

        if "tempo de execução" in texto_minusculo:
            resumo["tempo"] = _extract_extraction_value(limpa)
            continue

        if "data/hora" in texto_minusculo:
            resumo["email_data"] = _extract_extraction_value(limpa)

    return resumo, avisos


class PipelineOrchestrator:
    """Orquestrador principal do pipeline refatorado."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa o orquestrador.
        
        Args:
            config: Configurações do projeto (se None, carrega do config.yaml)
        """
        self.config_loader = ConfigLoader()
        self.config = config or self.config_loader.load()
        self.logger = get_logger(__name__, self.config)

        self.paths_config = self.config_loader.get_nested_value(self.config, 'paths', {})

        # Inicializar processadores
        self.max_processor = MaxProcessor(self.config, self.logger)
        self.vic_processor = VicProcessor(self.config, self.logger)
        self.devolucao_processor = DevolucaoProcessor(self.config, self.logger)
        self.batimento_processor = BatimentoProcessor(self.config, self.logger)
        self.baixa_processor = BaixaProcessor(self.config, self.logger)
        self.enriquecimento_processor = EnriquecimentoVicProcessor(
            self.config, self.logger
        )

        # Validador de consistência para comparação
        self.validador = ValidadorConsistencia(self.config, self.logger)

        # Metadados compartilhados entre etapas
        self._ultima_data_base_vic: Optional[str] = None
        
    def processar_max(self, entrada: Optional[Path] = None, saida: Optional[Path] = None) -> Dict[str, Any]:
        """Processa dados MAX.
        
        Args:
            entrada: Arquivo de entrada (se None, extrai do banco)
            saida: Diretório de saída (se None, usa config)
            
        Returns:
            Estatísticas do processamento
        """
        try:
            self.logger.info("Iniciando processamento MAX...")
            # Preferir arquivo mais recente em data/input/max se 'entrada' não fornecida
            if entrada is None:
                try:
                    base = Path(self.config.get('paths', {}).get('input', {}).get('max', ''))
                    files = list(base.glob('*.zip')) + list(base.glob('*.csv')) if base.exists() else []
                    if files:
                        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                        entrada = files[0]
                        self.logger.info(f"Usando arquivo de input MAX: {entrada}")
                except Exception:
                    pass
            resultado = self.max_processor.processar(entrada, saida)
            self.logger.info("Processamento MAX concluído com sucesso")
            return resultado
            
        except Exception as e:
            self.logger.error(f"Erro no processamento MAX: {e}")
            raise
            
    def processar_vic(self, entrada: Optional[Path] = None, saida: Optional[Path] = None) -> Dict[str, Any]:
        """Processa dados VIC.
        
        Args:
            entrada: Arquivo de entrada (se None, extrai do banco)
            saida: Diretório de saída (se None, usa config)
            
        Returns:
            Estatísticas do processamento
        """
        try:
            self.logger.info("Iniciando processamento VIC...")
            # Preferir arquivo mais recente em data/input/vic se 'entrada' não fornecida
            if entrada is None:
                try:
                    base = Path(self.config.get('paths', {}).get('input', {}).get('vic', ''))
                    files = list(base.glob('*.zip')) + list(base.glob('*.csv')) if base.exists() else []
                    if files:
                        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                        entrada = files[0]
                        self.logger.info(f"Usando arquivo de input VIC: {entrada}")
                except Exception:
                    pass
            resultado = self.vic_processor.processar(
                entrada=entrada,
                saida=saida,
                data_base=self._ultima_data_base_vic,
            )
            self.logger.info("Processamento VIC concluído com sucesso")
            return resultado
            
        except Exception as e:
            self.logger.error(f"Erro no processamento VIC: {e}")
            raise
            
    def processar_devolucao(
        self,
        vic_path: Path,
        max_path: Path,
        baixa_resultado: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Processa devoluções.

        Args:
            vic_path: Caminho para arquivo VIC tratado
            max_path: Caminho para arquivo MAX tratado
            baixa_resultado: Artefatos gerados na etapa de baixa (opcional)

        Returns:
            Estatísticas do processamento
        """
        try:
            self.logger.info("Iniciando processamento de devolução...")
            resultado = self.devolucao_processor.processar(
                vic_path, max_path, baixa_resultado
            )
            self.logger.info("Processamento de devolução concluído com sucesso")
            return resultado

        except Exception as e:
            self.logger.error(f"Erro no processamento de devolução: {e}")
            raise
            
    def processar_batimento(self, vic_path: Path, max_path: Path, saida: Optional[Path] = None) -> Dict[str, Any]:
        """Processa batimento.

        Args:
            vic_path: Caminho para arquivo VIC tratado
            max_path: Caminho para arquivo MAX tratado
            saida: Diretório de saída (se None, usa config)

        Returns:
            Estatísticas do processamento
        """
        try:
            self.logger.info("Iniciando processamento de batimento...")
            resultado = self.batimento_processor.processar(vic_path, max_path, saida)
            self.logger.info("Processamento de batimento concluído com sucesso")
            return resultado

        except Exception as e:
            self.logger.error(f"Erro no processamento de batimento: {e}")
            raise

    def processar_baixa(
        self, vic_path: Path, max_path: Path
    ) -> Dict[str, Any]:
        """Processa a etapa de baixa (VIC baixado × MAX em aberto)."""

        try:
            self.logger.info("Iniciando processamento de baixa...")
            resultado = self.baixa_processor.processar(vic_path, max_path)
            self.logger.info("Processamento de baixa concluído com sucesso")
            return resultado

        except Exception as e:
            self.logger.error(f"Erro no processamento de baixa: {e}")
            raise

    def processar_enriquecimento(
        self, vic_path: Path, batimento_path: Path
    ) -> Dict[str, Any]:
        """Processa o arquivo de enriquecimento a partir do VIC tratado."""

        try:
            self.logger.info("Iniciando processamento de enriquecimento...")
            resultado = self.enriquecimento_processor.processar(
                vic_path, batimento_path
            )
            self.logger.info("Processamento de enriquecimento concluído com sucesso")
            return resultado

        except Exception as e:
            self.logger.error(f"Erro no processamento de enriquecimento: {e}")
            raise
    def _verificar_arquivos_entrada_existem(self) -> None:
        """Verifica se existem arquivos de entrada obrigatórios (sem executar extração automática)."""
        vic_path = Path(self.config.get('paths', {}).get('input', {}).get('vic', ''))
        max_path = Path(self.config.get('paths', {}).get('input', {}).get('max', ''))
        judicial_path = Path(self.config.get('paths', {}).get('input', {}).get('judicial', ''))
        
        vic_files = []
        max_files = []
        judicial_files = []
        
        if vic_path.exists():
            vic_files = list(vic_path.glob('*.zip')) + list(vic_path.glob('*.csv'))
        
        if max_path.exists():
            max_files = list(max_path.glob('*.zip')) + list(max_path.glob('*.csv'))
            
        if judicial_path.exists():
            judicial_files = list(judicial_path.glob('*.zip')) + list(judicial_path.glob('*.csv'))
        
        missing_files = []
        if not vic_files:
            missing_files.append(f"VIC em {vic_path}")
        if not max_files:
            missing_files.append(f"MAX em {max_path}")
        if not judicial_files:
            missing_files.append(f"JUDICIAL em {judicial_path}")
            
        if missing_files:
            self.logger.error("❌ ARQUIVOS DE ENTRADA OBRIGATÓRIOS NÃO ENCONTRADOS:")
            for missing in missing_files:
                self.logger.error(f"   • {missing}")
            self.logger.error("Execute primeiro: python main.py --extrair-bases")
            self.logger.error("Ou use: python main.py --pipeline-completo (sem --skip-extraction)")
            raise FileNotFoundError("Arquivos de entrada obrigatórios não encontrados")
    
    def _verificar_arquivos_entrada(self) -> None:
        """Verifica se existem arquivos de entrada e executa extração automática se necessário."""
        vic_path = Path(self.config.get('paths', {}).get('input', {}).get('vic', ''))
        max_path = Path(self.config.get('paths', {}).get('input', {}).get('max', ''))
        judicial_path = Path(self.config.get('paths', {}).get('input', {}).get('judicial', ''))
        
        vic_files = []
        max_files = []
        judicial_files = []
        
        if vic_path.exists():
            vic_files = list(vic_path.glob('*.zip')) + list(vic_path.glob('*.csv'))
        
        if max_path.exists():
            max_files = list(max_path.glob('*.zip')) + list(max_path.glob('*.csv'))
            
        if judicial_path.exists():
            judicial_files = list(judicial_path.glob('*.zip')) + list(judicial_path.glob('*.csv'))
        
        if not vic_files or not max_files or not judicial_files:
            self.logger.info("="*60)
            self.logger.warning("⚠️  ARQUIVOS DE ENTRADA NÃO ENCONTRADOS")
            self.logger.info("="*60)
            
            if not vic_files:
                self.logger.warning(f"❌ Nenhum arquivo VIC encontrado em: {vic_path}")
            if not max_files:
                self.logger.warning(f"❌ Nenhum arquivo MAX encontrado em: {max_path}")
            if not judicial_files:
                self.logger.warning(f"❌ Nenhum arquivo JUDICIAL encontrado em: {judicial_path}")
                
            self.logger.info("")
            self.logger.info("🔄 EXECUTANDO EXTRAÇÃO AUTOMÁTICA DAS BASES...")
            self.logger.info("   • VIC (Email)")
            self.logger.info("   • MAX (SQL Server)")
            self.logger.info("   • JUDICIAL (SQL Server)")
            self.logger.info("="*60)
            
            # Executar extração automática
            try:
                resultado_extracao = self.extrair_bases()
                self.logger.info("✅ Extração automática concluída com sucesso!")
                self.logger.info("Continuando com o pipeline completo...")
                self.logger.info("="*60)
            except Exception as e:
                self.logger.error(f"❌ Erro na extração automática: {e}")
                self.logger.error("Pipeline interrompido. Verifique as configurações de conexão.")
                raise
            
    def pipeline_completo(self, saida: Optional[Path] = None, comparar_com_atual: bool = False, skip_extraction: bool = False) -> Dict[str, Any]:
        """Executa pipeline completo: Extração → Tratamento VIC → Tratamento MAX → Inclusão → Baixa → Devolução.
        
        Args:
            saida: Diretório de saída (se None, usa config)
            comparar_com_atual: Se True, compara resultados com sistema atual
            skip_extraction: Se True, pula a etapa de extração das bases
            
        Returns:
            Estatísticas consolidadas do processamento
        """
        inicio = datetime.now()
        resultados = {}
        
        try:
            self.logger.info("=" * 60)
            self.logger.info("INICIANDO PIPELINE COMPLETO VIC/MAX REFATORADO")
            self.logger.info("=" * 60)

            # Limpar diretórios de saída antes de nova execução
            self._limpar_outputs()

            # Executar extração das bases por padrão (a menos que skip_extraction seja True)
            if not skip_extraction:
                self.logger.info("🔄 EXECUTANDO EXTRAÇÃO AUTOMÁTICA DAS BASES...")
                self.logger.info("   • VIC (Email)")
                self.logger.info("   • MAX (SQL Server)")
                self.logger.info("   • JUDICIAL (SQL Server)")
                self.logger.info("="*60)
                
                try:
                    resultado_extracao = self.extrair_bases()
                    self.logger.info("✅ Extração automática concluída com sucesso!")
                    self.logger.info("Continuando com o pipeline completo...")
                    self.logger.info("="*60)
                    resultados['extracao'] = resultado_extracao
                except Exception as e:
                    self.logger.error(f"❌ Erro na extração automática: {e}")
                    self.logger.error("Pipeline interrompido. Verifique as configurações de conexão.")
                    raise
            else:
                self.logger.info("⏭️  Pulando extração das bases (--skip-extraction ativado)")
                self.logger.info("="*60)
                # Verificar se existem arquivos de entrada quando pular extração
                self._verificar_arquivos_entrada_existem()

            # 1. Tratamento VIC
            self.logger.info("\n[1/6] Tratamento VIC...")
            resultado_vic = self.processar_vic(saida=saida)
            resultados['vic'] = resultado_vic
            vic_path = resultado_vic.get('arquivo_gerado')

            # 2. Tratamento MAX
            self.logger.info("\n[2/6] Tratamento MAX...")
            resultado_max = self.processar_max(saida=saida)
            resultados['max'] = resultado_max
            max_path = resultado_max.get('arquivo_gerado')

            if vic_path and max_path:
                # 3. Batimento
                self.logger.info("\n[3/6] Batimento VIC×MAX...")
                resultado_batimento = self.processar_batimento(
                    Path(vic_path), Path(max_path), saida
                )
                resultados['batimento'] = resultado_batimento

                batimento_path = resultado_batimento.get('arquivo_gerado')
                if batimento_path:
                    # 4. Enriquecimento
                    self.logger.info("   ↳ [4/6] Enriquecimento de Contato...")
                    resultado_enriquecimento = self.processar_enriquecimento(
                        Path(vic_path), Path(batimento_path)
                    )
                    resultados['enriquecimento'] = resultado_enriquecimento
                else:
                    self.logger.warning(
                        "Resultado do batimento sem arquivo gerado; pulando enriquecimento"
                    )

                # 5. Baixa
                self.logger.info("\n[5/6] Baixa — VIC baixado × MAX em aberto...")
                resultado_baixa = self.processar_baixa(Path(vic_path), Path(max_path))
                resultados['baixa'] = resultado_baixa

                # 6. Devolução
                self.logger.info("\n[6/6] Devolução — MAX→VIC...")
                resultado_devolucao = self.processar_devolucao(
                    Path(vic_path), Path(max_path), resultado_baixa
                )
                resultados['devolucao'] = resultado_devolucao
            else:
                self.logger.warning(
                    "Pulando etapas dependentes - arquivos VIC/MAX não disponíveis"
                )
                
            # Comparação com sistema atual (se solicitado)
            if comparar_com_atual:
                self.logger.info("\n[EXTRA] Comparando com sistema atual...")
                resultado_comparacao = self._comparar_com_atual(resultados)
                resultados['comparacao'] = resultado_comparacao
                
            # Estatísticas finais
            duracao = datetime.now() - inicio
            resultados['duracao_total'] = duracao.total_seconds()
            resultados['timestamp'] = inicio.isoformat()
            
            self.logger.info("\n" + "=" * 60)
            self.logger.info("PIPELINE COMPLETO FINALIZADO COM SUCESSO")
            self.logger.info(f"Duração total: {duracao}")
            self.logger.info("=" * 60)
            
            return resultados
            
        except Exception as e:
            self.logger.error(f"Erro no pipeline completo: {e}")
            raise
            
    def _carregar_primeiro_csv(self, zip_path: Path) -> pd.DataFrame:
        if not zip_path.exists():
            raise FileNotFoundError(f'Referência não encontrada: {zip_path}')
        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
            if not csv_names:
                raise ValueError(f'Nenhum CSV encontrado em {zip_path}')
            with zf.open(csv_names[0]) as fh:
                return pd.read_csv(fh, sep=';', encoding='utf-8-sig')

    def _comparar_com_atual(self, resultados: Dict[str, Any]) -> Dict[str, Any]:
        """Compara resultados com sistema atual usando ValidadorConsistencia.
        
        Args:
            resultados: Resultados do pipeline refatorado
            
        Returns:
            Resultado da comparação
        """

        comparacao_cfg = self.config.get('comparacao', {})
        legacy_dir_cfg = comparacao_cfg.get('legacy_dir')
        if not legacy_dir_cfg:
            return {'status': 'legacy_indisponivel', 'motivo': 'config.comparacao.legacy_dir não definido'}
        legacy_base = Path(legacy_dir_cfg)
        if not legacy_base.exists():
            return {'status': 'legacy_indisponivel', 'motivo': f'Diretório legado não encontrado: {legacy_base}'}

        comparacao_dir = Path(self.paths_config.get('output', {}).get('base', 'data/output')) / 'comparacoes'
        comparacao_dir.mkdir(parents=True, exist_ok=True)

        patterns = {
            'vic': ('vic', 'vic_tratada_*.zip'),
            'max': ('max', 'max_tratada_*.zip'),
            'devolucao': ('devolucao', 'vic_devolucao_*.zip'),
            'batimento': ('batimento', 'vic_batimento_*.zip'),
        }

        resumo = []
        for chave, (subdir, pattern) in patterns.items():
            entry = {'dataset': chave}
            atual_info = resultados.get(chave, {})
            atual_path = atual_info.get('arquivo_gerado')
            if not atual_path:
                entry.update({'status': 'sem_arquivo_atual'})
                resumo.append(entry)
                continue
            atual_df = self._carregar_primeiro_csv(Path(atual_path))
            entry['atual_arquivo'] = Path(atual_path).name
            entry['atual_registros'] = len(atual_df)

            legacy_subdir = legacy_base / subdir
            legacy_files = sorted(legacy_subdir.glob(pattern)) if legacy_subdir.exists() else []
            if not legacy_files:
                entry.update({'status': 'sem_legacy', 'legacy_registros': 0, 'diferenca': len(atual_df)})
                resumo.append(entry)
                continue

            legacy_latest = legacy_files[-1]
            legacy_df = self._carregar_primeiro_csv(legacy_latest)
            entry['legacy_arquivo'] = legacy_latest.name
            entry['legacy_registros'] = len(legacy_df)
            entry['diferenca'] = len(atual_df) - len(legacy_df)

            chave_col = 'CHAVE' if chave in ('vic', 'batimento') else 'PARCELA'
            if chave == 'max':
                chave_col = 'PARCELA'
            if chave == 'devolucao':
                chave_col = 'PARCELA' if 'PARCELA' in atual_df.columns else None
            if chave_col and chave_col in atual_df.columns and chave_col in legacy_df.columns:
                atual_keys = set(atual_df[chave_col].astype(str).str.strip())
                legacy_keys = set(legacy_df[chave_col].astype(str).str.strip())
                entry['intersecao'] = len(atual_keys & legacy_keys)
                entry['apenas_atual'] = len(atual_keys - legacy_keys)
                entry['apenas_legacy'] = len(legacy_keys - atual_keys)
            entry['status'] = 'ok'
            resumo.append(entry)

        resumo_df = pd.DataFrame(resumo)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        arquivo = comparacao_dir / f'comparacao_{timestamp}.csv'
        resumo_df.to_csv(arquivo, index=False, encoding='utf-8-sig')
        return {'status': 'ok', 'arquivo': str(arquivo), 'resumo': resumo}


            
    def _limpar_outputs(self) -> None:
        output_cfg = self.paths_config.get('output', {})
        base_dir = Path(output_cfg.get('base', 'data/output'))
        subdirs = ['vic_tratada', 'max_tratada', 'devolucao', 'batimento', 'inconsistencias', 'comparacoes']
        for sub in subdirs:
            dir_path = base_dir / sub
            if not dir_path.exists():
                continue
            for item in dir_path.iterdir():
                try:
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        if item.name == '.gitkeep':
                            continue
                        item.unlink()
                except Exception:
                    continue

    def extrair_bases(self) -> Dict[str, Any]:
        """Executa extração das bases VIC (email), MAX (DB) e Judicial (DB)."""
        print("\n" + "=" * 60)
        print("EXTRACAO DE BASES - VIC, MAX E JUDICIAL")
        print("=" * 60)

        inicio = datetime.now()
        resultados: Dict[str, Any] = {}
        scripts_dir = Path(__file__).parent / "scripts"

        scripts = [
            ("extrair_vic_email.py", "VIC (Email)"),
            ("extrair_vic_max.py", "MAX (DB)"),
            ("extrair_judicial.py", "Judicial (DB)"),
        ]

        for script_name, descricao in scripts:
            script_path = scripts_dir / script_name

            if not script_path.exists():
                self.logger.error("Script não encontrado: %s", script_path)
                resultados[script_name] = {"status": "erro", "erro": "Script não encontrado"}
                continue

            print(f"\nExecutando extração {descricao}...")

            try:
                env = dict(os.environ)
                src_path = str(Path(__file__).parent / "src")
                env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env.get('PYTHONPATH', '')}".rstrip(os.pathsep)
                env["PYTHONIOENCODING"] = "utf-8"
                env["PYTHONUTF8"] = "1"

                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    cwd=Path(__file__).parent,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                )

                stdout = result.stdout or ""
                stderr = result.stderr or ""

                if result.returncode == 0:
                    resumo, avisos = _parse_extraction_summary(stdout)
                    print(f"✅ {descricao} - Extração concluída com sucesso")
                    for chave, emoji, rotulo in _SUMMARY_FIELDS:
                        valor = resumo.get(chave)
                        if valor:
                            print(f"   {emoji} {rotulo}: {valor}")
                    if avisos:
                        print("   ⚠️ Avisos:")
                        for aviso in avisos:
                            print(f"      - {aviso}")
                    print()
                    resultados[script_name] = {
                        "status": "sucesso",
                        "output": stdout,
                        "resumo": resumo,
                        "avisos": avisos,
                    }
                else:
                    print(f"❌ {descricao} - Falha na extração (código {result.returncode})")
                    if stdout.strip():
                        print("   📄 Saída (stdout):")
                        for line in stdout.splitlines():
                            if line.strip():
                                print(f"      {line}")
                    if stderr.strip():
                        print("   ⚠️ Saída (stderr):")
                        for line in stderr.splitlines():
                            if line.strip():
                                print(f"      {line}")
                    print()
                    raise RuntimeError(f"Falha na extração {descricao}. Verifique conexão/VPN e execute novamente.")

            except Exception as exc:
                raise RuntimeError(f"Falha na extração {descricao}: {exc}")

        # Persistir metadado da data-base VIC (quando disponível)
        resumo_vic = resultados.get("extrair_email.py", {})
        if isinstance(resumo_vic, dict):
            resumo_info = resumo_vic.get("resumo")
            if isinstance(resumo_info, dict):
                data_email = resumo_info.get("email_data")
                if data_email:
                    self._ultima_data_base_vic = data_email

        duracao = (datetime.now() - inicio).total_seconds()
        sucessos = len(resultados)

        print(f"\n[INFO] Extração concluída em {duracao:.2f}s – {sucessos} de {len(scripts)} etapas concluídas")

        return {
            'duracao_total': duracao,
            'scripts_executados': len(scripts),
            'sucessos': sucessos,
            'resultados': resultados,
        }

def main():
    """Função principal do orquestrador."""
    parser = argparse.ArgumentParser(
        description='Pipeline VIC/MAX Refatorado - Orquestrador Principal',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python main.py --pipeline-completo                    # Pipeline completo (MAX → VIC → Devolução → Batimento)
  python main.py --max                                  # Processa apenas dados MAX (requer arquivo de entrada)
  python main.py --vic                                  # Processa apenas dados VIC (requer arquivo de entrada)
  python main.py --devolucao vic.csv max.csv           # Gera apenas relatório de devolução
  python main.py --batimento vic.csv max.csv           # Gera apenas relatório de batimento
  python main.py --extrair-bases                       # Extrai bases: VIC (Gmail), MAX (SQL Server), Judicial (SQL Server)
                                                        # Mostra arquivos gerados, estatísticas e tempo de execução
  python main.py --pipeline-completo --comparar        # Pipeline completo com comparação de resultados

Funcionalidades de extração:
  --extrair-bases: Executa scripts de extração que geram arquivos em data/input/
    • VIC: Baixa anexos do Gmail (data/input/vic/)
    • MAX: Extrai dados do SQL Server (data/input/max/)
    • Judicial: Extrai dados judiciais (data/input/judicial/)
        """
    )
    
    # Argumentos de modo de operação
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--pipeline-completo', action='store_true',
                      help='Executa pipeline completo (MAX → VIC → Devolução → Batimento)')
    group.add_argument('--max', action='store_true',
                      help='Processa apenas dados MAX')
    group.add_argument('--vic', action='store_true',
                      help='Processa apenas dados VIC')
    group.add_argument('--devolucao', nargs=2, metavar=('VIC_FILE', 'MAX_FILE'),
                      help='Processa apenas devolução com arquivos VIC e MAX')
    group.add_argument('--batimento', nargs=2, metavar=('VIC_FILE', 'MAX_FILE'),
                      help='Processa apenas batimento com arquivos VIC e MAX')
    group.add_argument('--extrair-bases', action='store_true',
                      help='Extrai bases VIC (email), MAX (DB) e Judicial (DB)')
    
    # Argumentos opcionais
    parser.add_argument('-o', '--output', type=Path,
                       help='Diretório de saída (usa config.yaml se não informado)')
    parser.add_argument('--entrada', type=Path,
                       help='Arquivo de entrada para processadores MAX/VIC')
    parser.add_argument('--comparar', action='store_true',
                       help='Compara resultados com sistema atual (apenas --pipeline-completo)')
    parser.add_argument('--no-timestamp', action='store_true',
                       help='Não adicionar timestamp aos arquivos de saída')
    parser.add_argument('--skip-extraction', action='store_true',
                       help='Pula extração automática no pipeline completo (requer arquivos já existentes)')
    
    args = parser.parse_args()
    
    try:
        # Inicializar orquestrador
        orchestrator = PipelineOrchestrator()
        
        # Executar operação solicitada
        if args.pipeline_completo:
            resultado = orchestrator.pipeline_completo(
                saida=args.output,
                comparar_com_atual=args.comparar,
                skip_extraction=args.skip_extraction
            )
            
        elif args.max:
            resultado = orchestrator.processar_max(
                entrada=args.entrada,
                saida=args.output
            )
            
        elif args.vic:
            resultado = orchestrator.processar_vic(
                entrada=args.entrada,
                saida=args.output
            )
            
        elif args.devolucao:
            vic_file, max_file = args.devolucao
            resultado = orchestrator.processar_devolucao(
                vic_path=Path(vic_file),
                max_path=Path(max_file)
            )
            
        elif args.batimento:
            vic_file, max_file = args.batimento
            resultado = orchestrator.processar_batimento(
                vic_path=Path(vic_file),
                max_path=Path(max_file),
                saida=args.output
            )
            
        elif args.extrair_bases:
            resultado = orchestrator.extrair_bases()
            

    except KeyboardInterrupt:
        print("\n❌ Execucao interrompida pelo usuario")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Erro na execucao: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()




