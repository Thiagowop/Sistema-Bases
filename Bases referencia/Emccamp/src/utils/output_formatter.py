"""
Módulo para formatação padronizada de saídas do pipeline.
Garante consistência visual em todas as etapas.
"""

import numbers
from typing import Any, Optional


class OutputFormatter:
    """Formatador padrão para saídas do console."""
    
    WIDTH = 80
    SEPARATOR = "=" * WIDTH

    @staticmethod
    def _format_integer(value: numbers.Integral) -> str:
        """Inteiros: separador de milhar com ponto."""
        return f"{int(value):,}".replace(",", ".")

    @staticmethod
    def _format_decimal(value: numbers.Real, decimals: int = 2) -> str:
        """Reais: separador de milhar com ponto e decimal com vírgula."""
        formatted = format(value, f",.{decimals}f")
        return formatted.replace(",", "_").replace(".", ",").replace("_", ".")

    @staticmethod
    def format_count(value: Any) -> str:
        """Formata contadores (inteiros) com ponto de milhar."""
        if isinstance(value, numbers.Integral):
            return OutputFormatter._format_integer(value)
        return f"{value}"

    @staticmethod
    def header(title: str) -> str:
        """Cabeçalho de seção."""
        return f"\n{OutputFormatter.SEPARATOR}\n{title.upper()}\n{OutputFormatter.SEPARATOR}"
    
    @staticmethod
    def section(title: str) -> str:
        """Título de subseção."""
        return f"\n>>> {title}"
    
    @staticmethod
    def metric(label: str, value: any, unit: str = "") -> str:
        """Métrica formatada com separador decimal brasileiro (vírgula)."""
        unit_str = f" {unit}" if unit else ""
        if isinstance(value, numbers.Integral):
            # Inteiros (inclui numpy.integer): milhar com ponto
            val_str = OutputFormatter._format_integer(value)
        elif isinstance(value, numbers.Real):
            # Reais (inclui numpy.floating, Decimal): milhar com ponto, decimal com vírgula
            val_str = OutputFormatter._format_decimal(value)
        else:
            val_str = f"{value}"
        return f"  {label}: {val_str}{unit_str}"
    
    @staticmethod
    def file_info(label: str, path: str, records: Optional[int] = None) -> str:
        """Informação de arquivo gerado."""
        if records is not None:
            return f"  {label}: {path} ({OutputFormatter.format_count(records)} registros)"
        return f"  {label}: {path}"
    
    @staticmethod
    def step(message: str) -> str:
        """Passo de processo."""
        return f"  -> {message}"
    
    @staticmethod
    def footer() -> str:
        """Rodapé de seção."""
        return OutputFormatter.SEPARATOR


def format_extraction_output(
    source: str,
    output_file: str,
    records: int,
    duration: float,
    steps: Optional[list[str]] = None
) -> None:
    """Formata saída padrão de extração com fluxo detalhado."""
    print(OutputFormatter.header(f"EXTRAÇÃO {source}"))
    
    if steps:
        print(OutputFormatter.section("Fluxo de execução"))
        for step in steps:
            print(OutputFormatter.step(step))
    
    print(OutputFormatter.section("Resultado"))
    print(OutputFormatter.metric("Registros extraídos", records))
    print(OutputFormatter.metric("Tempo de execução", duration, "segundos"))
    
    print(OutputFormatter.section("Arquivo gerado"))
    print(OutputFormatter.file_info("Local", output_file))
    print(OutputFormatter.footer())


def format_extraction_judicial_output(
    autojur_records: int,
    maxsmart_records: int,
    duplicates_removed: int,
    total_unique: int,
    output_file: str,
    duration: float
) -> None:
    """Formata saída de extração judicial (2 fontes)."""
    print(OutputFormatter.header("EXTRAÇÃO JUDICIAL (AUTOJUR + MAX SMART)"))
    
    print(OutputFormatter.section("Fluxo de execução"))
    print(OutputFormatter.step(f"Consulta AUTOJUR: {OutputFormatter.format_count(autojur_records)} registros"))
    print(OutputFormatter.step(f"Consulta MAX Smart: {OutputFormatter.format_count(maxsmart_records)} registros"))
    print(OutputFormatter.step(f"Combinação das bases: {OutputFormatter.format_count(autojur_records + maxsmart_records)} registros"))
    print(OutputFormatter.step(f"Remoção de duplicatas: {OutputFormatter.format_count(duplicates_removed)} registros"))
    
    print(OutputFormatter.section("Resultado"))
    print(OutputFormatter.metric("Registros únicos", total_unique))
    print(OutputFormatter.metric("Tempo de execução", duration, "segundos"))
    
    print(OutputFormatter.section("Arquivo gerado"))
    print(OutputFormatter.file_info("Local", output_file))
    print(OutputFormatter.footer())


def format_treatment_output(
    source: str,
    input_records: int,
    output_records: int,
    inconsistencies: int,
    output_file: str,
    inconsistencies_file: Optional[str] = None,
    duration: Optional[float] = None
) -> None:
    """Formata saída padrão de tratamento."""
    print(OutputFormatter.header(f"TRATAMENTO {source}"))
    print(OutputFormatter.metric("Registros originais", input_records))
    if inconsistencies > 0:
        print(OutputFormatter.metric("Inconsistências removidas", inconsistencies))
    print(OutputFormatter.metric("Registros finais", output_records))
    
    taxa = (output_records / input_records * 100) if input_records > 0 else 0
    print(OutputFormatter.metric("Taxa de aproveitamento", taxa, "%"))
    
    print(OutputFormatter.section("Arquivos gerados"))
    print(OutputFormatter.file_info("Base tratada", output_file))
    if inconsistencies_file and inconsistencies > 0:
        print(OutputFormatter.file_info("Inconsistências", inconsistencies_file))
    
    if duration is not None:
        print(OutputFormatter.section("Desempenho"))
        print(OutputFormatter.metric("Tempo de execução", duration, "segundos"))
    
    print(OutputFormatter.footer())


def format_batimento_output(
    emccamp_records: int,
    max_records: int,
    max_dedup: int,
    batimento_records: int,
    judicial: int,
    extrajudicial: int,
    output_file: str,
    duration: float
) -> None:
    """Formata saída padrão de batimento."""
    print(OutputFormatter.header("BATIMENTO EMCCAMP x MAX"))
    
    print(OutputFormatter.section("Volumes"))
    print(OutputFormatter.metric("EMCCAMP tratado", emccamp_records))
    print(OutputFormatter.metric("MAX tratado", max_records))
    print(OutputFormatter.metric("MAX deduplicado", max_dedup))
    
    print(OutputFormatter.section("Resultado"))
    print(OutputFormatter.metric("Ausentes no MAX (total)", batimento_records))
    print(OutputFormatter.metric("  - Judicial", judicial))
    print(OutputFormatter.metric("  - Extrajudicial", extrajudicial))
    
    taxa = (batimento_records / emccamp_records * 100) if emccamp_records > 0 else 0
    print(OutputFormatter.metric("Taxa de batimento", taxa, "%"))
    
    print(OutputFormatter.section("Saída"))
    print(OutputFormatter.file_info("Arquivo gerado", output_file))
    print(OutputFormatter.metric("Tempo", duration, "segundos"))
    print(OutputFormatter.footer())


def format_baixa_output(
    emccamp_records: int,
    max_records_raw: int,
    max_records_filtered: int,
    baixa_records: int,
    com_receb: int,
    sem_receb: int,
    output_file: str,
    duration: float,
    filtros_aplicados: Optional[dict] = None,
    flow_steps: Optional[dict[str, int]] = None
) -> None:
    """Formata saída padrão da etapa de baixa com fluxo completo."""
    print(OutputFormatter.header("BAIXA MAX - EMCCAMP (RIGHT ANTI-JOIN)"))
    
    print(OutputFormatter.section("Bases carregadas"))
    print(OutputFormatter.metric("EMCCAMP tratado", emccamp_records))
    print(OutputFormatter.metric("MAX tratado (bruto)", max_records_raw))
    
    if filtros_aplicados:
        print(OutputFormatter.section("Filtros aplicados"))
        for filtro, info in filtros_aplicados.items():
            antes = info.get('antes', 0)
            depois = info.get('depois', 0)
            valores = info.get('valores', [])
            valores_str = ", ".join(str(v) for v in valores) if valores else ""
            print(f"  {filtro}: {OutputFormatter.format_count(antes)} -> {OutputFormatter.format_count(depois)} ({valores_str})")
        print(OutputFormatter.metric("MAX (após filtros)", max_records_filtered))
    
    if flow_steps:
        print(OutputFormatter.section("Fluxo de processamento"))
        if 'anti_join' in flow_steps:
            print(OutputFormatter.step(f"Anti-join MAX - EMCCAMP: {OutputFormatter.format_count(flow_steps['anti_join'])} registros"))
        if 'acordos_loaded' in flow_steps:
            print(OutputFormatter.step(f"Acordos abertos carregados: {OutputFormatter.format_count(flow_steps['acordos_loaded'])} registros"))
        if 'acordos_removed' in flow_steps:
            print(OutputFormatter.step(f"Registros removidos (acordo): {OutputFormatter.format_count(flow_steps['acordos_removed'])} registros"))
        if 'apos_filtro_acordo' in flow_steps:
            print(OutputFormatter.step(f"Após filtro de acordo: {OutputFormatter.format_count(flow_steps['apos_filtro_acordo'])} registros"))
        if 'baixas_loaded' in flow_steps:
            print(OutputFormatter.step(f"Baixas EMCCAMP carregadas: {OutputFormatter.format_count(flow_steps['baixas_loaded'])} registros"))
        if 'procv_baixas' in flow_steps:
            print(OutputFormatter.step(f"PROCV com baixas: {OutputFormatter.format_count(flow_steps['procv_baixas'])} registros com recebimento preenchido"))
    
    print(OutputFormatter.section("Resultado final"))
    print(OutputFormatter.metric("Total para baixa", baixa_records))
    print(OutputFormatter.metric("  -> Com recebimento", com_receb))
    print(OutputFormatter.metric("  -> Sem recebimento", sem_receb))
    
    taxa = (baixa_records / max_records_filtered * 100) if max_records_filtered > 0 else 0
    print(OutputFormatter.metric("Taxa de baixa", taxa, "%"))
    
    print(OutputFormatter.section("Saída"))
    print(OutputFormatter.file_info("Arquivo gerado", output_file))
    print(OutputFormatter.metric("Tempo", duration, "segundos"))
    print(OutputFormatter.footer())
