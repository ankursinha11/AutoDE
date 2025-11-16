"""
Excel Generator for STAG Comparison Reports

Generates professional Excel workbooks with 5 sheets matching the exact
format specification for system comparison reports.

Sheets:
1. Overview - Business stage comparison
2. Databricks Logic - Detailed Databricks implementation
3. [Source] Logic - Detailed source system implementation
4. Logic Comparison - Side-by-side technical comparison
5. STTM - Source-to-Target Mapping (column-level)

Features:
- Professional formatting (bold headers, colors, borders)
- Auto-adjusted column widths
- Code snippet formatting (monospace)
- Wrapped text for readability
- Frozen header rows
"""

import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from loguru import logger
from datetime import datetime

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    logger.warning("openpyxl not installed - Excel generation will be limited")


class ExcelGenerator:
    """Generate professional Excel comparison reports"""

    def __init__(self):
        """Initialize Excel Generator"""
        if not OPENPYXL_AVAILABLE:
            logger.error("openpyxl is required for Excel generation")
            raise ImportError("Please install openpyxl: pip install openpyxl")

        # Define color scheme
        self.header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        self.header_font = Font(bold=True, color="FFFFFF", size=11)
        self.similar_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        self.different_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        self.border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

    def generate_comparison_excel(
        self,
        source_system: str,
        source_workflow: str,
        databricks_pipeline: str,
        business_stages: List[Dict[str, Any]],
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any],
        sttm_mappings: List[Dict[str, Any]],
        output_folder: str = "outputs/stag_comparisons"
    ) -> str:
        """
        Generate complete comparison Excel file

        Args:
            source_system: "hadoop" or "abinitio"
            source_workflow: Source workflow name
            databricks_pipeline: Databricks pipeline name
            business_stages: Business stage comparison data
            source_logic: Source system logic
            databricks_logic: Databricks logic
            sttm_mappings: STTM column mappings
            output_folder: Output directory

        Returns:
            Path to generated Excel file
        """
        logger.info(f"📊 Generating Excel comparison: {source_workflow} → {databricks_pipeline}")

        # Create output folder
        os.makedirs(output_folder, exist_ok=True)

        # Create workbook
        wb = Workbook()
        wb.remove(wb.active)  # Remove default sheet

        # Generate each sheet
        logger.info("   Creating Overview sheet...")
        self._create_overview_sheet(wb, source_system, source_workflow, databricks_pipeline, business_stages)

        logger.info("   Creating Databricks Logic sheet...")
        self._create_databricks_logic_sheet(wb, databricks_logic)

        logger.info(f"   Creating {source_system.capitalize()} Logic sheet...")
        self._create_source_logic_sheet(wb, source_system, source_logic)

        logger.info("   Creating Logic Comparison sheet...")
        self._create_logic_comparison_sheet(wb, source_system, source_logic, databricks_logic)

        logger.info("   Creating STTM sheet...")
        self._create_sttm_sheet(wb, sttm_mappings)

        # Save workbook
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Sanitize filenames to remove invalid characters for Windows/Linux
        safe_source = source_workflow.replace(':', '_').replace('/', '_').replace('\\', '_').replace('|', '_').replace('?', '_').replace('*', '_').replace('<', '_').replace('>', '_').replace('"', '_').strip()
        safe_databricks = databricks_pipeline.replace(':', '_').replace('/', '_').replace('\\', '_').replace('|', '_').replace('?', '_').replace('*', '_').replace('<', '_').replace('>', '_').replace('"', '_').strip()

        filename = f"{safe_source}_vs_{safe_databricks}_{timestamp}.xlsx"
        output_path = os.path.join(output_folder, filename)

        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)

        try:
            wb.save(output_path)
            logger.info(f"✅ Excel file saved: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"❌ Failed to save Excel file: {e}")
            # Try with even simpler filename
            simple_filename = f"STAG_comparison_{timestamp}.xlsx"
            fallback_path = os.path.join(output_folder, simple_filename)
            wb.save(fallback_path)
            logger.info(f"✅ Excel file saved (fallback): {fallback_path}")
            return fallback_path

    def _create_overview_sheet(
        self,
        wb: Workbook,
        source_system: str,
        source_workflow: str,
        databricks_pipeline: str,
        business_stages: List[Dict[str, Any]]
    ):
        """Create Overview sheet with business stage comparison"""
        ws = wb.create_sheet("Overview", 0)

        # Title row
        ws['A1'] = f"Workflow Comparison: {source_workflow} → {databricks_pipeline}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:E1')

        # Metadata
        ws['A2'] = f"Source System: {source_system.capitalize()}"
        ws['A3'] = f"Target System: Databricks"
        ws['A4'] = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # Headers (row 6)
        headers = ['Stage Name', 'Databricks Description', f'{source_system.capitalize()} Description', 'Comparison', 'Notes']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=6, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Data rows
        for row_idx, stage in enumerate(business_stages, 7):
            ws.cell(row=row_idx, column=1, value=stage.get('stage_name', ''))
            ws.cell(row=row_idx, column=2, value=stage.get('databricks_description', ''))
            ws.cell(row=row_idx, column=3, value=stage.get('source_description', ''))
            ws.cell(row=row_idx, column=4, value=stage.get('comparison', ''))
            ws.cell(row=row_idx, column=5, value=stage.get('notes', ''))

            # Apply coloring based on comparison
            comparison = stage.get('comparison', '')
            if comparison == 'Similar':
                for col in range(1, 6):
                    ws.cell(row=row_idx, column=col).fill = self.similar_fill
            elif comparison == 'Different':
                for col in range(1, 6):
                    ws.cell(row=row_idx, column=col).fill = self.different_fill

            # Apply borders and alignment
            for col in range(1, 6):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        # Adjust column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 40
        ws.column_dimensions['C'].width = 40
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 35

        # Freeze header row
        ws.freeze_panes = 'A7'

    def _create_databricks_logic_sheet(self, wb: Workbook, databricks_logic: Dict[str, Any]):
        """Create Databricks Logic sheet"""
        ws = wb.create_sheet("Databricks Logic")

        # Title
        ws['A1'] = f"Databricks Pipeline: {databricks_logic.get('pipeline_name', 'Unknown')}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:F1')

        # Headers
        headers = ['Activity Name', 'Type', 'Notebook', 'Purpose', 'Inputs', 'Outputs']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.border

        # Data rows
        activities = databricks_logic.get('activities', [])
        for row_idx, activity in enumerate(activities, 4):
            ws.cell(row=row_idx, column=1, value=activity.get('name', ''))
            ws.cell(row=row_idx, column=2, value=activity.get('type', ''))
            ws.cell(row=row_idx, column=3, value=activity.get('notebook', ''))
            ws.cell(row=row_idx, column=4, value=activity.get('purpose', ''))
            ws.cell(row=row_idx, column=5, value=', '.join(activity.get('inputs', [])))
            ws.cell(row=row_idx, column=6, value=', '.join(activity.get('outputs', [])))

            # Apply formatting
            for col in range(1, 7):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        # Add code snippets section (below activities)
        if activities and any(a.get('code_snippets') for a in activities):
            code_start_row = len(activities) + 6
            ws.cell(row=code_start_row, column=1, value="Code Snippets")
            ws.cell(row=code_start_row, column=1).font = Font(bold=True, size=12)

            code_row = code_start_row + 1
            for activity in activities:
                if activity.get('code_snippets'):
                    ws.cell(row=code_row, column=1, value=f"{activity['name']}:")
                    ws.cell(row=code_row, column=1).font = Font(bold=True)
                    code_row += 1

                    for snippet in activity['code_snippets']:
                        ws.cell(row=code_row, column=1, value=snippet)
                        ws.cell(row=code_row, column=1).font = Font(name='Courier New', size=9)
                        ws.cell(row=code_row, column=1).alignment = Alignment(wrap_text=True)
                        ws.merge_cells(f'A{code_row}:F{code_row}')
                        code_row += 1

        # Column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 20
        ws.column_dimensions['C'].width = 30
        ws.column_dimensions['D'].width = 35
        ws.column_dimensions['E'].width = 30
        ws.column_dimensions['F'].width = 30

        ws.freeze_panes = 'A4'

    def _create_source_logic_sheet(self, wb: Workbook, source_system: str, source_logic: Dict[str, Any]):
        """Create Source Logic sheet (Hadoop or Ab Initio)"""
        sheet_name = f"{source_system.capitalize()} Logic"
        ws = wb.create_sheet(sheet_name)

        # Title
        workflow_name = source_logic.get('workflow_name') or source_logic.get('graph_name', 'Unknown')
        ws['A1'] = f"{source_system.capitalize()} Workflow: {workflow_name}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:F1')

        if source_system == 'hadoop':
            self._populate_hadoop_logic(ws, source_logic)
        elif source_system == 'abinitio':
            self._populate_abinitio_logic(ws, source_logic)

        ws.freeze_panes = 'A4'

    def _populate_hadoop_logic(self, ws, hadoop_logic: Dict[str, Any]):
        """Populate Hadoop-specific logic"""
        # Headers
        headers = ['Job Name', 'Script', 'Purpose', 'Inputs', 'Outputs', 'Transformations']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.border

        # Data rows
        jobs = hadoop_logic.get('jobs', [])
        for row_idx, job in enumerate(jobs, 4):
            ws.cell(row=row_idx, column=1, value=job.get('name', ''))
            ws.cell(row=row_idx, column=2, value=job.get('script', ''))
            ws.cell(row=row_idx, column=3, value=job.get('purpose', ''))
            ws.cell(row=row_idx, column=4, value=', '.join(job.get('inputs', [])))
            ws.cell(row=row_idx, column=5, value=', '.join(job.get('outputs', [])))
            ws.cell(row=row_idx, column=6, value='; '.join(job.get('transformations', [])))

            for col in range(1, 7):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        # Column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 25
        ws.column_dimensions['C'].width = 35
        ws.column_dimensions['D'].width = 30
        ws.column_dimensions['E'].width = 30
        ws.column_dimensions['F'].width = 40

    def _populate_abinitio_logic(self, ws, abinitio_logic: Dict[str, Any]):
        """Populate Ab Initio-specific logic"""
        # Headers
        headers = ['Step', 'Component', 'Type', 'Transformation', 'Inputs', 'Outputs']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.border

        # Data rows
        steps = abinitio_logic.get('steps', [])
        for row_idx, step in enumerate(steps, 4):
            ws.cell(row=row_idx, column=1, value=step.get('step_number', ''))
            ws.cell(row=row_idx, column=2, value=step.get('dataset', ''))
            ws.cell(row=row_idx, column=3, value=step.get('component_type', ''))
            ws.cell(row=row_idx, column=4, value=step.get('transformation_rules', ''))
            ws.cell(row=row_idx, column=5, value=', '.join(step.get('inputs', [])))
            ws.cell(row=row_idx, column=6, value=', '.join(step.get('outputs', [])))

            for col in range(1, 7):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        # DML Files section
        dml_files = abinitio_logic.get('dml_files', [])
        if dml_files:
            dml_row = len(steps) + 6
            ws.cell(row=dml_row, column=1, value="DML Files:")
            ws.cell(row=dml_row, column=1).font = Font(bold=True)
            ws.cell(row=dml_row, column=2, value=', '.join(dml_files))
            ws.merge_cells(f'B{dml_row}:F{dml_row}')

        # Column widths
        ws.column_dimensions['A'].width = 10
        ws.column_dimensions['B'].width = 30
        ws.column_dimensions['C'].width = 20
        ws.column_dimensions['D'].width = 40
        ws.column_dimensions['E'].width = 30
        ws.column_dimensions['F'].width = 30

    def _create_logic_comparison_sheet(
        self,
        wb: Workbook,
        source_system: str,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ):
        """Create Logic Comparison sheet (side-by-side)"""
        ws = wb.create_sheet("Logic Comparison")

        # Title
        ws['A1'] = "Side-by-Side Logic Comparison"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:F1')

        # Headers
        headers = [
            f'{source_system.capitalize()} Item',
            f'{source_system.capitalize()} Details',
            'Comparison',
            'Databricks Item',
            'Databricks Details',
            'Notes'
        ]
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.border

        # Build comparison rows
        comparison_rows = self._build_logic_comparison(source_system, source_logic, databricks_logic)

        for row_idx, comp_row in enumerate(comparison_rows, 4):
            ws.cell(row=row_idx, column=1, value=comp_row['source_item'])
            ws.cell(row=row_idx, column=2, value=comp_row['source_details'])
            ws.cell(row=row_idx, column=3, value=comp_row['comparison'])
            ws.cell(row=row_idx, column=4, value=comp_row['databricks_item'])
            ws.cell(row=row_idx, column=5, value=comp_row['databricks_details'])
            ws.cell(row=row_idx, column=6, value=comp_row['notes'])

            # Color coding
            if comp_row['comparison'] == '✓ Similar':
                for col in range(1, 7):
                    ws.cell(row=row_idx, column=col).fill = self.similar_fill
            elif comp_row['comparison'] == '✗ Different':
                for col in range(1, 7):
                    ws.cell(row=row_idx, column=col).fill = self.different_fill

            # Formatting
            for col in range(1, 7):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

        # Column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 35
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 25
        ws.column_dimensions['E'].width = 35
        ws.column_dimensions['F'].width = 30

        ws.freeze_panes = 'A4'

    def _build_logic_comparison(
        self,
        source_system: str,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Build side-by-side comparison rows"""
        rows = []

        # Get items from both systems
        source_items = self._get_logic_items(source_system, source_logic)
        databricks_items = self._get_logic_items('databricks', databricks_logic)

        # Simple matching (by index for now - could be enhanced with AI matching)
        max_items = max(len(source_items), len(databricks_items))

        for i in range(max_items):
            source_item = source_items[i] if i < len(source_items) else {}
            databricks_item = databricks_items[i] if i < len(databricks_items) else {}

            rows.append({
                'source_item': source_item.get('name', ''),
                'source_details': source_item.get('details', ''),
                'comparison': '✓ Similar' if (source_item and databricks_item) else '✗ Different',
                'databricks_item': databricks_item.get('name', ''),
                'databricks_details': databricks_item.get('details', ''),
                'notes': self._generate_comparison_note(source_item, databricks_item)
            })

        return rows

    def _get_logic_items(self, system: str, logic: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract logic items for comparison"""
        items = []

        if system == 'hadoop':
            for job in logic.get('jobs', []):
                items.append({
                    'name': job.get('name', ''),
                    'details': f"Purpose: {job.get('purpose', '')}\nTransformations: {'; '.join(job.get('transformations', []))}"
                })
        elif system == 'abinitio':
            for step in logic.get('steps', []):
                items.append({
                    'name': step.get('dataset', ''),
                    'details': f"Type: {step.get('component_type', '')}\nTransformation: {step.get('transformation_rules', '')}"
                })
        elif system == 'databricks':
            for activity in logic.get('activities', []):
                items.append({
                    'name': activity.get('name', ''),
                    'details': f"Type: {activity.get('type', '')}\nPurpose: {activity.get('purpose', '')}"
                })

        return items

    def _generate_comparison_note(self, source_item: Dict, databricks_item: Dict) -> str:
        """Generate comparison note"""
        if not source_item:
            return "New in Databricks"
        if not databricks_item:
            return "Removed from Databricks"
        return "Corresponding implementation"

    def _create_sttm_sheet(self, wb: Workbook, sttm_mappings: List[Dict[str, Any]]):
        """Create STTM (Source-to-Target Mapping) sheet"""
        ws = wb.create_sheet("STTM")

        # Title
        ws['A1'] = "Source-to-Target Mapping (Column Level)"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:F1')

        # Headers
        headers = ['Target Column', 'Data Type', 'Source Columns', 'Transformation Logic', 'Dependencies', 'Confidence']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = self.border

        # Data rows
        for row_idx, mapping in enumerate(sttm_mappings, 4):
            ws.cell(row=row_idx, column=1, value=mapping.get('target_column', ''))
            ws.cell(row=row_idx, column=2, value=mapping.get('data_type', ''))
            ws.cell(row=row_idx, column=3, value=', '.join(mapping.get('source_columns', [])))
            ws.cell(row=row_idx, column=4, value=mapping.get('transformation_logic', ''))
            ws.cell(row=row_idx, column=5, value=', '.join(mapping.get('dependencies', [])))
            ws.cell(row=row_idx, column=6, value=f"{mapping.get('confidence', 0):.2f}")

            # Formatting
            for col in range(1, 7):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

            # Color code by confidence
            confidence = mapping.get('confidence', 0)
            if confidence >= 0.8:
                for col in range(1, 7):
                    ws.cell(row=row_idx, column=col).fill = self.similar_fill
            elif confidence < 0.6:
                for col in range(1, 7):
                    ws.cell(row=row_idx, column=col).fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")

        # Column widths
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 15
        ws.column_dimensions['C'].width = 30
        ws.column_dimensions['D'].width = 45
        ws.column_dimensions['E'].width = 30
        ws.column_dimensions['F'].width = 12

        ws.freeze_panes = 'A4'


# Example usage
if __name__ == "__main__":
    if not OPENPYXL_AVAILABLE:
        print("ERROR: openpyxl not installed. Please run: pip install openpyxl")
        exit(1)

    print("\n" + "=" * 80)
    print("EXCEL GENERATOR TEST")
    print("=" * 80)

    generator = ExcelGenerator()

    # Sample data
    business_stages = [
        {
            'stage_name': 'Data Ingestion',
            'databricks_description': 'Load customer data from ADLS',
            'source_description': 'Load customer data from HDFS',
            'comparison': 'Similar',
            'notes': 'Both load from respective cloud storage'
        }
    ]

    source_logic = {
        'system': 'hadoop',
        'workflow_name': 'ES_BDF_DOWNLOAD',
        'jobs': [
            {'name': 'Load_Data', 'script': 'load.pig', 'purpose': 'Load data', 'inputs': ['raw_data'], 'outputs': ['processed'], 'transformations': ['Filter', 'Transform']}
        ]
    }

    databricks_logic = {
        'system': 'databricks',
        'pipeline_name': 'pl_cdd_bdf_download',
        'activities': [
            {'name': 'Ingest_Data', 'type': 'DatabricksNotebook', 'notebook': 'nb_load', 'purpose': 'Load data', 'inputs': ['adls_raw'], 'outputs': ['delta_table']}
        ]
    }

    sttm_mappings = [
        {
            'target_column': 'customer_id',
            'data_type': 'STRING',
            'source_columns': ['cust_id'],
            'transformation_logic': 'DIRECT',
            'dependencies': [],
            'confidence': 0.95
        }
    ]

    print("\nGenerating test Excel file...")
    output_path = generator.generate_comparison_excel(
        source_system='hadoop',
        source_workflow='ES_BDF_DOWNLOAD',
        databricks_pipeline='pl_cdd_bdf_download',
        business_stages=business_stages,
        source_logic=source_logic,
        databricks_logic=databricks_logic,
        sttm_mappings=sttm_mappings,
        output_folder='test_output'
    )

    print(f"\n✅ Test Excel file generated: {output_path}")
    print("\n" + "=" * 80)
