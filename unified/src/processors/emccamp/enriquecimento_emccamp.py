from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set
import zipfile

import pandas as pd

from src.config.loader import ConfigLoader, LoadedConfig
from src.utils.io import DatasetIO
from src.utils.output_formatter import OutputFormatter
from src.utils.path_manager import PathManager


@dataclass(slots=True)
class ContactEnrichmentStats:
    input_rows: int
    phone_rows: int
    phone_discarded: int
    email_rows: int
    email_discarded: int
    deduplicated: int
    output_path: Path
    output_records: int


class ContactEnrichmentProcessor:
    """Normaliza telefones e e-mails de uma base tratada, gerando layout de enriquecimento."""

    def __init__(self, config: LoadedConfig, dataset_key: str) -> None:
        self.config = config
        self.dataset_key = dataset_key
        self.paths = PathManager(config.base_path, config.data)
        self.settings = self._load_settings()

        csv_cfg = self.settings.get("csv", {})
        global_cfg = config.data.get("global", {}) if isinstance(config.data, dict) else {}
        separator = csv_cfg.get("delimiter") or global_cfg.get("csv_separator", ";")
        encoding = csv_cfg.get("encoding") or global_cfg.get("encoding", "utf-8-sig")
        self.io = DatasetIO(separator=separator, encoding=encoding)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _load_settings(self) -> Dict[str, Any]:
        data = self.config.data if isinstance(self.config.data, dict) else {}
        enrich_cfg = data.get("enriquecimento", {})
        dataset_cfg = enrich_cfg.get(self.dataset_key)
        if not isinstance(dataset_cfg, dict):
            raise RuntimeError(
                f"Configuracao enriquecimento.{self.dataset_key} nao encontrada em config.yaml"
            )
        return dataset_cfg

    def _resolve_path(self, raw_path: str) -> Path:
        if not raw_path:
            raise ValueError("Caminho nao definido para enriquecimento.")

        path_obj = Path(raw_path)
        base_path = Path(self.config.base_path)
        resolved_path = path_obj if path_obj.is_absolute() else (base_path / path_obj)
        if "*" in resolved_path.name:
            directory = resolved_path.parent
            pattern = resolved_path.name
            candidates = sorted(
                directory.glob(pattern), key=lambda f: f.stat().st_mtime, reverse=True
            )
            if not candidates:
                raise FileNotFoundError(
                    f"Nenhum arquivo encontrado para o padrao {directory / pattern}"
                )
            return candidates[0]

        if not resolved_path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {resolved_path}")
        return resolved_path

    def _resolve_input_file(self) -> Path:
        input_cfg = self.settings.get("input", {})
        raw_path = input_cfg.get("path")
        if not raw_path:
            raise ValueError(
                f"Configuracao enriquecimento.{self.dataset_key}.input.path nao definida"
            )
        return self._resolve_path(raw_path)

    @staticmethod
    def _ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
        missing = [col for col in required if col and col not in df.columns]
        if missing:
            raise KeyError(f"Colunas ausentes na base de origem: {missing}")

    @staticmethod
    def _normalize_phone(value: Any) -> str:
        return re.sub(r"\D", "", str(value or ""))

    @staticmethod
    def _format_date(value: Any) -> str:
        if pd.isna(value) or value is None:
            return date.today().strftime("%d/%m/%Y")
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return date.today().strftime("%d/%m/%Y")
        return parsed.strftime("%d/%m/%Y")

    def _collect_keys(self, path: Path, column: str, members: Optional[List[str]] = None) -> Set[str]:
        keys: Set[str] = set()
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                names = members or [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not names:
                    raise FileNotFoundError(
                        f"Arquivo {path} nao contem CSVs para aplicar filtro de chave."
                    )
                for name in names:
                    with zf.open(name) as fh:
                        df = pd.read_csv(fh, sep=self.io.separator, encoding=self.io.encoding)
                        if column not in df.columns:
                            raise KeyError(
                                f"Coluna {column} ausente no arquivo {name} dentro de {path}"
                            )
                        keys.update(df[column].astype(str).str.strip())
        else:
            df = pd.read_csv(path, sep=self.io.separator, encoding=self.io.encoding)
            if column not in df.columns:
                raise KeyError(f"Coluna {column} ausente em {path}")
            keys.update(df[column].astype(str).str.strip())

        keys.discard("")
        return keys

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def run(self) -> ContactEnrichmentStats:
        input_file = self._resolve_input_file()
        mapping = self.settings.get("mapping", {})
        rules = self.settings.get("rules", {})
        output_cfg = self.settings.get("output", {})

        cpf_col = mapping.get("cpf")
        nome_col = mapping.get("nome")
        data_base_col = mapping.get("data_base")
        telefone_cols = mapping.get("telefones", []) or []
        email_cols = mapping.get("emails", []) or []

        key_cfg = self.settings.get("key", {}) or {}
        key_components = key_cfg.get("components", []) or []
        key_separator = key_cfg.get("separator", "-")
        key_column_name = key_cfg.get("column_name") or key_cfg.get("target_column") or "CHAVE"
        target_key_column = key_cfg.get("target_column", key_column_name)

        required = [cpf_col, nome_col] + telefone_cols + email_cols + key_components
        if data_base_col:
            required.append(data_base_col)

        # remove None entries
        required = [col for col in required if col]

        df_source = self.io.read(input_file)
        self._ensure_columns(df_source, required)

        if key_components:
            self._ensure_columns(df_source, key_components)
            composed = (
                df_source[key_components]
                .astype(str)
                .apply(lambda row: key_separator.join(part.strip() for part in row), axis=1)
            )
            df_source[key_column_name] = composed

        filters_cfg = self.settings.get("filters", {})
        key_filter_cfg = filters_cfg.get("key") if isinstance(filters_cfg, dict) else None
        if key_filter_cfg:
            filter_path_raw = key_filter_cfg.get("source_path")
            filter_column = key_filter_cfg.get("column")
            members = key_filter_cfg.get("members")
            if not filter_path_raw or not filter_column:
                raise ValueError("Filtro de chave exige source_path e column configurados.")
            filter_path = self._resolve_path(filter_path_raw)
            keys = self._collect_keys(filter_path, filter_column, members)
            if not keys:
                raise RuntimeError(
                    f"Filtro de chave em {filter_path} nao retornou nenhum valor para {filter_column}."
                )
            if target_key_column not in df_source.columns:
                raise KeyError(
                    f"Coluna {target_key_column} ausente para aplicar filtro de chave na base de origem."
                )
            df_source[target_key_column] = df_source[target_key_column].astype(str).str.strip()
            df_source = df_source[df_source[target_key_column].isin(keys)].copy()

        limpar_telefone = bool(rules.get("limpar_telefone", True))
        descartar_email_sem_arroba = bool(rules.get("descartar_email_sem_arroba", True))
        dedup_keys = rules.get("deduplicar_por") or ["CPFCNPJ CLIENTE", "CONTATO", "TIPO"]
        observacao_prefix = rules.get("observacao_prefix", "Base")
        telefone_principal_value = str(rules.get("telefone_principal_value", "1"))

        registros: List[Dict[str, Any]] = []
        phone_order = {col: idx for idx, col in enumerate(telefone_cols)}
        email_order = {col: idx for idx, col in enumerate(email_cols)}

        phone_created = phone_discarded = 0
        email_created = email_discarded = 0

        for _, row in df_source.iterrows():
            cpf_value = row.get(cpf_col, "")
            nome_value = row.get(nome_col, "")
            # Usar data atual da base em vez da data individual do registro
            observacao = f"{observacao_prefix} - {date.today().strftime('%d/%m/%Y')}"

            # Telefones
            for col in telefone_cols:
                valor = row.get(col, "")
                if pd.isna(valor) or str(valor).strip() == "":
                    continue
                telefone = (
                    self._normalize_phone(valor) if limpar_telefone else str(valor).strip()
                )
                if not telefone:
                    phone_discarded += 1
                    continue
                registros.append(
                    {
                        "CPFCNPJ CLIENTE": cpf_value,
                        "TELEFONE": telefone,
                        "EMAIL": "",
                        "OBSERVACAO": observacao,
                        "NOME": nome_value,
                        "TELEFONE PRINCIPAL": telefone_principal_value,
                        "TIPO": "TEL",
                        "CONTATO": telefone,
                        "ORDEM_CONTATO": phone_order.get(col, 0),
                    }
                )
                phone_created += 1

            # E-mails
            for col in email_cols:
                valor = row.get(col, "")
                if pd.isna(valor):
                    continue
                email = str(valor).strip()
                if not email:
                    continue
                if descartar_email_sem_arroba and "@" not in email:
                    email_discarded += 1
                    continue
                registros.append(
                    {
                        "CPFCNPJ CLIENTE": cpf_value,
                        "TELEFONE": "",
                        "EMAIL": email,
                        "OBSERVACAO": observacao,
                        "NOME": nome_value,
                        "TELEFONE PRINCIPAL": telefone_principal_value,
                        "TIPO": "EMAIL",
                        "CONTATO": email.lower(),
                        "ORDEM_CONTATO": email_order.get(col, 0),
                    }
                )
                email_created += 1

        if not registros:
            raise RuntimeError("Nenhum contato valido encontrado na base de origem.")

        df_saida = pd.DataFrame(registros)
        df_saida["ORD_TIPO"] = df_saida["TIPO"].map({"TEL": 0, "EMAIL": 1}).fillna(2)

        duplicated_mask = df_saida.duplicated(subset=dedup_keys, keep="first")
        deduplicated = int(duplicated_mask.sum())
        if deduplicated:
            df_saida = df_saida[~duplicated_mask].copy()

        df_saida = df_saida.sort_values(
            by=["ORD_TIPO", "CPFCNPJ CLIENTE", "ORDEM_CONTATO", "NOME"], kind="stable"
        ).reset_index(drop=True)

        df_saida = df_saida[
            ["CPFCNPJ CLIENTE", "TELEFONE", "EMAIL", "OBSERVACAO", "NOME", "TELEFONE PRINCIPAL"]
        ]

        output_dir = output_cfg.get("dir", "data/output/enriquecimento_contato")
        output_dir_path = (
            Path(output_dir) if Path(output_dir).is_absolute() else self.paths.base_path / output_dir
        )
        output_dir_path.mkdir(parents=True, exist_ok=True)

        csv_name = output_cfg.get("csv_name", f"enriquecimento_contato_{self.dataset_key}.csv")
        zip_name = output_cfg.get("zip_name", f"enriquecimento_contato_{self.dataset_key}.zip")
        output_zip = output_dir_path / zip_name

        self.io.write_zip({csv_name: df_saida}, output_zip)

        return ContactEnrichmentStats(
            input_rows=len(df_source),
            phone_rows=phone_created,
            phone_discarded=phone_discarded,
            email_rows=email_created,
            email_discarded=email_discarded,
            deduplicated=deduplicated,
            output_path=output_zip,
            output_records=len(df_saida),
        )

    def print_summary(self, stats: ContactEnrichmentStats) -> None:
        print(OutputFormatter.header(f"ENRIQUECIMENTO CONTATO ({self.dataset_key.upper()})"))
        print(OutputFormatter.metric("Linhas origem", stats.input_rows))
        print(OutputFormatter.metric("Telefones gerados", stats.phone_rows))
        print(OutputFormatter.metric("Telefones descartados", stats.phone_discarded))
        print(OutputFormatter.metric("Emails gerados", stats.email_rows))
        print(OutputFormatter.metric("Emails descartados", stats.email_discarded))
        print(OutputFormatter.metric("Removidos na deduplicacao", stats.deduplicated))
        print(OutputFormatter.section("Arquivo gerado"))
        print(OutputFormatter.file_info("Saida ZIP", str(stats.output_path), stats.output_records))
        print(OutputFormatter.footer())


def run(dataset_key: str = "default", loader: Optional[ConfigLoader] = None) -> ContactEnrichmentStats:
    loader = loader or ConfigLoader()
    config = loader.load()
    processor = ContactEnrichmentProcessor(config, dataset_key)
    stats = processor.run()
    processor.print_summary(stats)
    return stats
