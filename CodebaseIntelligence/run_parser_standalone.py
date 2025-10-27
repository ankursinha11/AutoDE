#!/usr/bin/env python3
"""
Standalone Parser Runner
Run individual parsers (Hadoop, Ab Initio, or Databricks) independently
without requiring other systems
"""

import sys
import argparse
import json
from pathlib import Path
from loguru import logger
from datetime import datetime

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from parsers.abinitio import AbInitioParser
from parsers.hadoop import HadoopParser
from parsers.databricks import DatabricksParser


def setup_logging(log_level="INFO"):
    """Setup logging"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level,
    )
    logger.add(
        "outputs/logs/standalone_parser.log",
        rotation="100 MB",
        level=log_level,
    )


def parse_abinitio(repo_path: str, output_dir: str):
    """Parse Ab Initio repository standalone"""
    logger.info("=" * 80)
    logger.info("Parsing Ab Initio Repository")
    logger.info("=" * 80)
    
    parser = AbInitioParser()
    result = parser.parse_directory(repo_path)
    
    logger.info(f"✓ Parsed {len(result['processes'])} processes")
    logger.info(f"✓ Extracted {len(result['components'])} components")
    logger.info(f"✓ Found {len(result['data_flows'])} data flows")
    
    # Export to Excel
    output_path = Path(output_dir) / "AbInitio_Parsed_Output.xlsx"
    parser.export_to_excel(str(output_path))
    logger.info(f"✓ Exported to: {output_path}")
    
    # Save JSON summary
    json_path = Path(output_dir) / "abinitio_summary.json"
    with open(json_path, "w") as f:
        json.dump(result["summary"], f, indent=2)
    logger.info(f"✓ Summary saved to: {json_path}")
    
    return result


def parse_hadoop(repo_path: str, output_dir: str):
    """Parse Hadoop repository standalone"""
    logger.info("=" * 80)
    logger.info("Parsing Hadoop Repository")
    logger.info("=" * 80)
    
    parser = HadoopParser()
    result = parser.parse_directory(repo_path)
    
    logger.info(f"✓ Parsed {len(result['processes'])} processes")
    logger.info(f"✓ Extracted {len(result['components'])} components")
    logger.info(f"✓ Found {len(result['data_flows'])} data flows")
    
    # Save JSON summary
    json_path = Path(output_dir) / "hadoop_summary.json"
    with open(json_path, "w") as f:
        json.dump(result["summary"], f, indent=2)
    logger.info(f"✓ Summary saved to: {json_path}")
    
    # Save detailed results as JSON
    details_path = Path(output_dir) / "hadoop_detailed_results.json"
    with open(details_path, "w") as f:
        # Convert objects to dicts for JSON serialization
        processes_dict = [
            {
                "id": p.id,
                "name": p.name,
                "system": p.system.value if hasattr(p.system, 'value') else str(p.system),
                "file_path": p.file_path,
                "description": p.description,
                "component_count": p.component_count,
                "input_sources": p.input_sources,
                "output_targets": p.output_targets,
                "tables_involved": p.tables_involved,
            }
            for p in result["processes"]
        ]
        components_dict = [
            {
                "id": c.id,
                "name": c.name,
                "component_type": c.component_type.value if hasattr(c.component_type, 'value') else str(c.component_type),
                "process_id": c.process_id,
                "file_path": c.file_path,
                "input_datasets": c.input_datasets,
                "output_datasets": c.output_datasets,
                "tables_read": c.tables_read,
                "tables_written": c.tables_written,
            }
            for c in result["components"]
        ]
        
        json.dump({
            "processes": processes_dict,
            "components": components_dict,
            "summary": result["summary"]
        }, f, indent=2)
    logger.info(f"✓ Detailed results saved to: {details_path}")
    
    return result


def parse_databricks(repo_path: str, output_dir: str):
    """Parse Databricks repository standalone"""
    logger.info("=" * 80)
    logger.info("Parsing Databricks Repository")
    logger.info("=" * 80)
    
    parser = DatabricksParser()
    result = parser.parse_directory(repo_path)
    
    logger.info(f"✓ Parsed {len(result['processes'])} processes")
    logger.info(f"✓ Extracted {len(result['components'])} components")
    logger.info(f"✓ Found {len(result['flows'])} flows")
    
    # Save JSON summary
    json_path = Path(output_dir) / "databricks_summary.json"
    summary = {
        "total_processes": len(result.get("processes", [])),
        "total_components": len(result.get("components", [])),
        "total_flows": len(result.get("flows", []))
    }
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"✓ Summary saved to: {json_path}")
    
    # Save detailed results as JSON
    details_path = Path(output_dir) / "databricks_detailed_results.json"
    with open(details_path, "w") as f:
        processes_dict = [
            {
                "id": p.id,
                "name": p.name,
                "file_path": p.file_path,
                "description": p.description,
                "component_count": len(p.component_ids),
                "input_sources": p.input_sources,
                "output_targets": p.output_targets,
            }
            for p in result.get("processes", [])
        ]
        components_dict = [
            {
                "id": c.id,
                "name": c.name,
                "component_type": c.component_type.value if hasattr(c.component_type, 'value') else str(c.component_type),
                "process_id": c.process_id,
                "file_path": c.file_path,
            }
            for c in result.get("components", [])
        ]
        
        json.dump({
            "processes": processes_dict,
            "components": components_dict,
            "summary": summary
        }, f, indent=2)
    logger.info(f"✓ Detailed results saved to: {details_path}")
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Standalone Parser - Run individual parsers independently"
    )
    
    # Parser type
    parser.add_argument(
        "parser_type",
        choices=["abinitio", "hadoop", "databricks"],
        help="Type of parser to run"
    )
    
    # Repository path
    parser.add_argument(
        "repo_path",
        type=str,
        help="Path to the repository to parse"
    )
    
    # Output directory
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./outputs/standalone_parser",
        help="Output directory for results (default: ./outputs/standalone_parser)"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level"
    )
    
    args = parser.parse_args()
    
    # Setup
    setup_logging(args.log_level)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_output_dir = output_dir / f"{args.parser_type}_{timestamp}"
    run_output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 80)
    logger.info(f"Standalone Parser: {args.parser_type.upper()}")
    logger.info(f"Repository: {args.repo_path}")
    logger.info(f"Output Directory: {run_output_dir}")
    logger.info("=" * 80)
    
    # Run appropriate parser
    try:
        if args.parser_type == "abinitio":
            result = parse_abinitio(args.repo_path, str(run_output_dir))
        elif args.parser_type == "hadoop":
            result = parse_hadoop(args.repo_path, str(run_output_dir))
        elif args.parser_type == "databricks":
            result = parse_databricks(args.repo_path, str(run_output_dir))
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ Parsing Complete!")
        logger.info(f"✓ Results saved to: {run_output_dir}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Error during parsing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

