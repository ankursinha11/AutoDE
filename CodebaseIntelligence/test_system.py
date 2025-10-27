#!/usr/bin/env python3
"""
Test and Validation Script
Tests the system with sample data from your repositories
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
import json

# Configure logger
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def test_abinitio_parser():
    """Test Ab Initio parser"""
    logger.info("=" * 60)
    logger.info("TEST 1: Ab Initio Parser")
    logger.info("=" * 60)

    from parsers.abinitio import AbInitioParser

    # Test with your sample files
    abi_path = "/Users/ankurshome/Desktop/Hadoop_Parser/OneDrive_1_7-25-2025/Abinitio"

    if not Path(abi_path).exists():
        logger.warning(f"Ab Initio path not found: {abi_path}")
        logger.warning("Skipping Ab Initio parser test")
        return None

    try:
        parser = AbInitioParser()
        result = parser.parse_directory(abi_path)

        logger.info(f"✓ Parsed {len(result['processes'])} processes")
        logger.info(f"✓ Extracted {len(result['components'])} components")
        logger.info(f"✓ Identified {len(result['data_flows'])} data flows")

        # Print sample process
        if result['processes']:
            process = result['processes'][0]
            logger.info(f"\nSample Process: {process.name}")
            logger.info(f"  - Type: {process.process_type.value}")
            logger.info(f"  - Components: {process.component_count}")
            logger.info(f"  - Business Function: {process.business_function}")

        # Print sample component
        if result['components']:
            comp = result['components'][0]
            logger.info(f"\nSample Component: {comp.name}")
            logger.info(f"  - Type: {comp.component_type.value}")
            logger.info(f"  - Inputs: {comp.input_datasets[:2]}")
            logger.info(f"  - Outputs: {comp.output_datasets[:2]}")

        logger.info("\n✓ Ab Initio Parser Test PASSED\n")
        return result

    except Exception as e:
        logger.error(f"✗ Ab Initio Parser Test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_databricks_parser():
    """Test Databricks parser"""
    logger.info("=" * 60)
    logger.info("TEST 2: Databricks Parser")
    logger.info("=" * 60)

    from parsers.databricks import DatabricksParser

    # Test with Databricks notebooks/ADF
    databricks_path = "/Users/ankurshome/Desktop/Hadoop_Parser/OneDrive_1_7-25-2025/Databricks"

    if not Path(databricks_path).exists():
        logger.warning(f"Databricks path not found: {databricks_path}")
        logger.warning("Skipping Databricks parser test")
        return None

    try:
        parser = DatabricksParser()
        result = parser.parse_directory(databricks_path)

        logger.info(f"✓ Parsed {len(result['processes'])} processes")
        logger.info(f"✓ Extracted {len(result['components'])} components")

        # Print sample process
        if result['processes']:
            process = result['processes'][0]
            logger.info(f"\nSample Process: {process.name}")
            logger.info(f"  - Type: {process.process_type.value}")
            logger.info(f"  - Components: {process.component_count}")

        # Print sample component
        if result['components']:
            comp = result['components'][0]
            logger.info(f"\nSample Component: {comp.name}")
            logger.info(f"  - Type: {comp.component_type.value}")
            logger.info(f"  - File: {comp.file_path}")

        logger.info("\n✓ Databricks Parser Test PASSED\n")
        return result

    except Exception as e:
        logger.error(f"✗ Databricks Parser Test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_hadoop_parser():
    """Test Hadoop parser"""
    logger.info("=" * 60)
    logger.info("TEST 3: Hadoop Parser")
    logger.info("=" * 60)

    from parsers.hadoop import HadoopParser

    # Test with your Hadoop repo
    hadoop_path = "/Users/ankurshome/Desktop/Hadoop_Parser/OneDrive_1_7-25-2025/Hadoop"

    if not Path(hadoop_path).exists():
        logger.warning(f"Hadoop path not found: {hadoop_path}")
        logger.warning("Skipping Hadoop parser test")
        return None

    try:
        parser = HadoopParser()
        result = parser.parse_directory(hadoop_path)

        logger.info(f"✓ Parsed {len(result['processes'])} processes")
        logger.info(f"✓ Extracted {len(result['components'])} components")

        # Print sample process
        if result['processes']:
            process = result['processes'][0]
            logger.info(f"\nSample Process: {process.name}")
            logger.info(f"  - Type: {process.process_type.value}")
            logger.info(f"  - Components: {process.component_count}")
            logger.info(f"  - Input Sources: {len(process.input_sources)}")
            logger.info(f"  - Output Targets: {len(process.output_targets)}")

        # Print sample component
        if result['components']:
            comp = result['components'][0]
            logger.info(f"\nSample Component: {comp.name}")
            logger.info(f"  - Type: {comp.component_type.value}")
            logger.info(f"  - File: {comp.file_path}")

        logger.info("\n✓ Hadoop Parser Test PASSED\n")
        return result

    except Exception as e:
        logger.error(f"✗ Hadoop Parser Test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_sttm_generator(abi_result, databricks_result, hadoop_result):
    """Test STTM generator"""
    logger.info("=" * 60)
    logger.info("TEST 4: STTM Generator")
    logger.info("=" * 60)

    if not abi_result and not databricks_result and not hadoop_result:
        logger.warning("No parser results available. Skipping STTM test")
        return

    from core.sttm_generator import STTMGenerator

    try:
        generator = STTMGenerator()

        # Test with Ab Initio
        if abi_result and abi_result['processes']:
            process = abi_result['processes'][0]
            components = abi_result['components']

            report = generator.generate_from_process(process, components)

            logger.info(f"✓ Generated STTM for process: {process.name}")
            logger.info(f"  - Total Mappings: {report.total_mappings}")
            logger.info(f"  - Direct Mappings: {report.direct_mappings}")
            logger.info(f"  - Derived Mappings: {report.derived_mappings}")
            logger.info(f"  - Source Tables: {len(report.source_tables)}")
            logger.info(f"  - Target Tables: {len(report.target_tables)}")

            # Print sample mapping
            if report.entries:
                entry = report.entries[0]
                logger.info(f"\nSample Mapping:")
                logger.info(f"  - Source: {entry.source_table}.{entry.source_column}")
                logger.info(f"  - Target: {entry.target_table}.{entry.target_column}")
                logger.info(f"  - Type: {entry.mapping_type}")

        logger.info("\n✓ STTM Generator Test PASSED\n")

    except Exception as e:
        logger.error(f"✗ STTM Generator Test FAILED: {e}")
        import traceback
        traceback.print_exc()


def test_process_matcher(abi_result, databricks_result, hadoop_result):
    """Test process matcher"""
    logger.info("=" * 60)
    logger.info("TEST 5: Process Matcher")
    logger.info("=" * 60)

    # Combine target systems
    target_processes = []
    target_components = []

    if hadoop_result:
        target_processes.extend(hadoop_result['processes'])
        target_components.extend(hadoop_result['components'])

    if databricks_result:
        target_processes.extend(databricks_result['processes'])
        target_components.extend(databricks_result['components'])

    if not abi_result or not target_processes:
        logger.warning("Need both Ab Initio and target system results. Skipping matcher test")
        return None

    from core.matchers import ProcessMatcher

    try:
        matcher = ProcessMatcher()

        mappings = matcher.match_processes(
            abi_result['processes'],
            target_processes,
            abi_result['components'],
            target_components,
        )

        logger.info(f"✓ Matched {len(mappings)} process pairs")

        for src_id, (tgt_id, score) in list(mappings.items())[:3]:
            src_proc = next(p for p in abi_result['processes'] if p.id == src_id)
            tgt_proc = next(p for p in target_processes if p.id == tgt_id)
            logger.info(f"  - {src_proc.name} -> {tgt_proc.name} (score: {score:.2f})")

        logger.info("\n✓ Process Matcher Test PASSED\n")
        return mappings

    except Exception as e:
        logger.error(f"✗ Process Matcher Test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_gap_analyzer(abi_result, databricks_result, hadoop_result, mappings):
    """Test gap analyzer"""
    logger.info("=" * 60)
    logger.info("TEST 6: Gap Analyzer")
    logger.info("=" * 60)

    # Combine target systems
    target_processes = []
    target_components = []

    if hadoop_result:
        target_processes.extend(hadoop_result['processes'])
        target_components.extend(hadoop_result['components'])

    if databricks_result:
        target_processes.extend(databricks_result['processes'])
        target_components.extend(databricks_result['components'])

    if not abi_result or not target_processes or not mappings:
        logger.warning("Need parser results and mappings. Skipping gap analyzer test")
        return None

    from core.gap_analyzer import GapAnalyzer

    try:
        analyzer = GapAnalyzer()

        # Convert mappings format
        mappings_dict = {src_id: tgt_id for src_id, (tgt_id, score) in mappings.items()}

        gaps = analyzer.analyze(
            abi_result['processes'],
            target_processes,
            abi_result['components'],
            target_components,
            mappings_dict,
        )

        logger.info(f"✓ Identified {len(gaps)} gaps")

        summary = analyzer.get_summary()
        logger.info(f"  - By Type: {summary['by_type']}")
        logger.info(f"  - By Severity: {summary['by_severity']}")

        # Print sample gaps
        for gap in gaps[:3]:
            logger.info(f"\nSample Gap:")
            logger.info(f"  - Type: {gap.gap_type.value}")
            logger.info(f"  - Severity: {gap.severity.value}")
            logger.info(f"  - Title: {gap.title}")
            logger.info(f"  - Description: {gap.description[:100]}...")

        logger.info("\n✓ Gap Analyzer Test PASSED\n")
        return gaps

    except Exception as e:
        logger.error(f"✗ Gap Analyzer Test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_excel_exporter(abi_result, databricks_result, hadoop_result, gaps):
    """Test Excel exporter"""
    logger.info("=" * 60)
    logger.info("TEST 7: Excel Exporter")
    logger.info("=" * 60)

    if not abi_result:
        logger.warning("Need parser results. Skipping exporter test")
        return

    from utils import ExcelExporter
    from core.sttm_generator import STTMGenerator

    try:
        exporter = ExcelExporter("./outputs/test_reports")
        generator = STTMGenerator()

        # Export STTM
        if abi_result['processes']:
            process = abi_result['processes'][0]
            report = generator.generate_from_process(process, abi_result['components'])

            sttm_path = exporter.export_sttm_report(report, "TEST_STTM.xlsx")
            logger.info(f"✓ Exported STTM: {sttm_path}")

        # Export gaps
        if gaps:
            gap_path = exporter.export_gap_analysis(gaps, "TEST_Gap_Analysis.xlsx")
            logger.info(f"✓ Exported Gap Analysis: {gap_path}")

        logger.info("\n✓ Excel Exporter Test PASSED\n")

    except Exception as e:
        logger.error(f"✗ Excel Exporter Test FAILED: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    logger.info("\n" + "=" * 60)
    logger.info("CODEBASE INTELLIGENCE - SYSTEM TEST")
    logger.info("=" * 60 + "\n")

    # Run tests
    abi_result = test_abinitio_parser()
    databricks_result = test_databricks_parser()
    hadoop_result = test_hadoop_parser()
    test_sttm_generator(abi_result, databricks_result, hadoop_result)
    mappings = test_process_matcher(abi_result, databricks_result, hadoop_result)
    gaps = test_gap_analyzer(abi_result, databricks_result, hadoop_result, mappings)
    test_excel_exporter(abi_result, databricks_result, hadoop_result, gaps)

    # Summary
    logger.info("=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info("✓ All tests completed!")
    logger.info("✓ Check ./outputs/test_reports for generated Excel files")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
