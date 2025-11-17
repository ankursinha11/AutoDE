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

        logger.info("   Creating STTM sheets (Source, Target, Comparison)...")
        self._create_sttm_sheets(wb, sttm_mappings, source_system, source_logic, databricks_logic)

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
        """Create Databricks Logic sheet with enhanced step-by-step logic and code snippets"""
        ws = wb.create_sheet("Databricks Logic")

        # Title
        ws['A1'] = f"Databricks Pipeline: {databricks_logic.get('pipeline_name', 'Unknown')}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:D1')

        # Headers (4 columns as per manual format)
        headers = ['Pipeline Step/Activity', 'Notebook', 'Purpose', 'Key Code Snippet / Logic']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Data rows with step-by-step logic and code snippets
        current_row = 4
        activities = databricks_logic.get('activities', [])

        for activity in activities:
            activity_name = activity.get('name', '')
            notebook = activity.get('notebook', '')
            purpose = activity.get('purpose', '')

            # Main activity row
            ws.cell(row=current_row, column=1, value=activity_name)
            ws.cell(row=current_row, column=2, value=notebook)
            ws.cell(row=current_row, column=3, value=purpose)
            ws.cell(row=current_row, column=4, value='')

            # Bold activity name
            ws.cell(row=current_row, column=1).font = Font(bold=True)

            # Apply borders
            for col in range(1, 5):
                ws.cell(row=current_row, column=col).border = self.border
                ws.cell(row=current_row, column=col).alignment = Alignment(vertical='top', wrap_text=True)

            current_row += 1

            # Add step-by-step logic items (if available)
            step_by_step = activity.get('step_by_step_logic', [])
            if step_by_step:
                for i, step in enumerate(step_by_step, 1):
                    ws.cell(row=current_row, column=1, value=f"  Step {i}")
                    ws.cell(row=current_row, column=2, value='')
                    ws.cell(row=current_row, column=3, value='')
                    ws.cell(row=current_row, column=4, value=step)

                    # Indent step
                    ws.cell(row=current_row, column=1).alignment = Alignment(indent=2, vertical='top', wrap_text=True)

                    for col in range(1, 5):
                        ws.cell(row=current_row, column=col).border = self.border
                        ws.cell(row=current_row, column=col).alignment = Alignment(vertical='top', wrap_text=True)

                    current_row += 1

            # Add code snippets (if available)
            code_snippets = activity.get('code_snippets', [])
            if code_snippets:
                for snippet in code_snippets:
                    # Extract code text from snippet dict or use string directly
                    if isinstance(snippet, dict):
                        code_text = snippet.get('code', '') or snippet.get('snippet', '')
                    else:
                        code_text = str(snippet)

                    if code_text:
                        ws.cell(row=current_row, column=1, value='')
                        ws.cell(row=current_row, column=2, value='')
                        ws.cell(row=current_row, column=3, value='')
                        ws.cell(row=current_row, column=4, value=code_text)

                        # Format as code (monospace, wrapped)
                        ws.cell(row=current_row, column=4).font = Font(name='Courier New', size=9)
                        ws.cell(row=current_row, column=4).alignment = Alignment(wrap_text=True, vertical='top')

                        for col in range(1, 5):
                            ws.cell(row=current_row, column=col).border = self.border

                        current_row += 1

            # Add blank row between activities
            current_row += 1

        # Column widths (4-column format)
        ws.column_dimensions['A'].width = 30   # Pipeline Step/Activity
        ws.column_dimensions['B'].width = 30   # Notebook
        ws.column_dimensions['C'].width = 40   # Purpose
        ws.column_dimensions['D'].width = 60   # Key Code Snippet / Logic

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
        """Populate Hadoop-specific logic with step-by-step breakdown"""
        # Headers (4 columns to match Databricks format)
        headers = ['Step', 'Script', 'Purpose', 'Logic']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Data rows with step-by-step logic
        current_row = 4
        jobs = hadoop_logic.get('jobs', [])

        for job in jobs:
            job_name = job.get('name', '')
            script = job.get('script', '')
            purpose = job.get('purpose', '')

            # Main job row
            ws.cell(row=current_row, column=1, value=job_name)
            ws.cell(row=current_row, column=2, value=script)
            ws.cell(row=current_row, column=3, value=purpose)
            ws.cell(row=current_row, column=4, value='')

            # Bold job name
            ws.cell(row=current_row, column=1).font = Font(bold=True)

            # Apply borders
            for col in range(1, 5):
                ws.cell(row=current_row, column=col).border = self.border
                ws.cell(row=current_row, column=col).alignment = Alignment(vertical='top', wrap_text=True)

            current_row += 1

            # Add step-by-step logic items
            step_by_step = job.get('step_by_step_logic', [])
            if step_by_step:
                for i, step in enumerate(step_by_step, 1):
                    ws.cell(row=current_row, column=1, value=f"  {i}")
                    ws.cell(row=current_row, column=2, value='')
                    ws.cell(row=current_row, column=3, value='')
                    ws.cell(row=current_row, column=4, value=step)

                    # Indent step number
                    ws.cell(row=current_row, column=1).alignment = Alignment(indent=2, vertical='top')

                    for col in range(1, 5):
                        ws.cell(row=current_row, column=col).border = self.border
                        ws.cell(row=current_row, column=col).alignment = Alignment(vertical='top', wrap_text=True)

                    current_row += 1

            # Add code snippets
            code_snippets = job.get('code_snippets', [])
            if code_snippets:
                for snippet in code_snippets:
                    # Extract code text
                    if isinstance(snippet, dict):
                        code_text = snippet.get('code', '') or snippet.get('snippet', '')
                    else:
                        code_text = str(snippet)

                    if code_text:
                        ws.cell(row=current_row, column=1, value='')
                        ws.cell(row=current_row, column=2, value='')
                        ws.cell(row=current_row, column=3, value='')
                        ws.cell(row=current_row, column=4, value=code_text)

                        # Format as code
                        ws.cell(row=current_row, column=4).font = Font(name='Courier New', size=9)
                        ws.cell(row=current_row, column=4).alignment = Alignment(wrap_text=True, vertical='top')

                        for col in range(1, 5):
                            ws.cell(row=current_row, column=col).border = self.border

                        current_row += 1

            # Blank row between jobs
            current_row += 1

        # Column widths (4-column format)
        ws.column_dimensions['A'].width = 30   # Step
        ws.column_dimensions['B'].width = 30   # Script
        ws.column_dimensions['C'].width = 40   # Purpose
        ws.column_dimensions['D'].width = 60   # Logic

    def _populate_abinitio_logic(self, ws, abinitio_logic: Dict[str, Any]):
        """Populate Ab Initio-specific logic with step-by-step breakdown"""
        # Headers (4 columns to match format)
        headers = ['Step', 'Component', 'Type', 'Transformation Logic']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Data rows with detailed transformation logic
        current_row = 4
        steps = abinitio_logic.get('steps', [])

        for step in steps:
            step_num = step.get('step_number', '')
            component = step.get('dataset', '') or step.get('component', '')
            comp_type = step.get('component_type', '')
            transformation = step.get('transformation_rules', '')

            # Main component row
            ws.cell(row=current_row, column=1, value=step_num)
            ws.cell(row=current_row, column=2, value=component)
            ws.cell(row=current_row, column=3, value=comp_type)
            ws.cell(row=current_row, column=4, value=transformation)

            # Bold component name
            ws.cell(row=current_row, column=2).font = Font(bold=True)

            # Apply borders
            for col in range(1, 5):
                ws.cell(row=current_row, column=col).border = self.border
                ws.cell(row=current_row, column=col).alignment = Alignment(vertical='top', wrap_text=True)

            current_row += 1

            # Add step-by-step logic items (if available)
            step_by_step = step.get('step_by_step_logic', [])
            if step_by_step:
                for i, logic_step in enumerate(step_by_step, 1):
                    ws.cell(row=current_row, column=1, value=f"  {i}")
                    ws.cell(row=current_row, column=2, value='')
                    ws.cell(row=current_row, column=3, value='')
                    ws.cell(row=current_row, column=4, value=logic_step)

                    # Indent step number
                    ws.cell(row=current_row, column=1).alignment = Alignment(indent=2, vertical='top')

                    for col in range(1, 5):
                        ws.cell(row=current_row, column=col).border = self.border
                        ws.cell(row=current_row, column=col).alignment = Alignment(vertical='top', wrap_text=True)

                    current_row += 1

            # Blank row between components
            current_row += 1

        # DML Files section
        dml_files = abinitio_logic.get('dml_files', [])
        if dml_files:
            current_row += 1
            ws.cell(row=current_row, column=1, value="DML Files:")
            ws.cell(row=current_row, column=1).font = Font(bold=True)
            ws.cell(row=current_row, column=2, value=', '.join(dml_files))
            ws.merge_cells(f'B{current_row}:D{current_row}')

        # Column widths (4-column format)
        ws.column_dimensions['A'].width = 15   # Step
        ws.column_dimensions['B'].width = 35   # Component
        ws.column_dimensions['C'].width = 25   # Type
        ws.column_dimensions['D'].width = 60   # Transformation Logic

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

    def _create_sttm_sheets(
        self,
        wb: Workbook,
        sttm_mappings: List[Dict[str, Any]],
        source_system: str,
        source_logic: Dict[str, Any],
        databricks_logic: Dict[str, Any]
    ):
        """
        Create 3 STTM sheets as requested:
        1. Source System STTM (e.g., Hadoop cdd: bdf_download)
        2. Target System STTM (e.g., Databricks pl_cdd_bdf_download)
        3. STTM Comparison (column-level mapping)
        """
        # Sheet 1: Source System STTM
        source_workflow = source_logic.get('workflow_name', 'Source')
        self._create_source_sttm_sheet(wb, source_logic, source_system, source_workflow)

        # Sheet 2: Target System STTM
        target_workflow = databricks_logic.get('pipeline_name', 'Target')
        self._create_target_sttm_sheet(wb, databricks_logic, target_workflow)

        # Sheet 3: STTM Comparison
        self._create_sttm_comparison_sheet(wb, sttm_mappings, source_system, source_workflow, target_workflow)

    def _create_source_sttm_sheet(
        self,
        wb: Workbook,
        source_logic: Dict[str, Any],
        source_system: str,
        workflow_name: str
    ):
        """Create Source System STTM sheet (tables/columns from source system)"""
        ws = wb.create_sheet(f"Source STTM ({source_system.upper()})")

        # Title
        ws['A1'] = f"Source System STTM - {workflow_name}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:M1')

        # Headers (same 13-column format)
        headers = [
            'Order',                 # 1
            'Schema',                # 2
            'Table Name',            # 3
            'Column Name',           # 4
            'Data Type',             # 5
            'pk?',                   # 6
            'PII?',                  # 7
            'Field Type',            # 8
            'Depends On',            # 9
            'Transformation',        # 10
            'Source Script',         # 11
            'Source Line',           # 12
            'Description'            # 13
        ]

        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Extract tables from source logic outputs
        row_idx = 4
        order = 1

        jobs = source_logic.get('jobs', [])
        for job in jobs:
            script_name = job.get('script_file', job.get('name', 'Unknown'))
            outputs = job.get('outputs', [])

            for table_name in outputs:
                # Add row for each output table
                # NOTE: We don't have column-level schema here, so we'll add table-level entries
                ws.cell(row=row_idx, column=1, value=order)
                ws.cell(row=row_idx, column=2, value=source_logic.get('workflow_name', ''))
                ws.cell(row=row_idx, column=3, value=table_name)
                ws.cell(row=row_idx, column=4, value='(See columns below)')
                ws.cell(row=row_idx, column=5, value='TABLE')
                ws.cell(row=row_idx, column=6, value='')
                ws.cell(row=row_idx, column=7, value='')
                ws.cell(row=row_idx, column=8, value='Output Table')
                ws.cell(row=row_idx, column=9, value='')
                ws.cell(row=row_idx, column=10, value='')
                ws.cell(row=row_idx, column=11, value=script_name)
                ws.cell(row=row_idx, column=12, value='')
                ws.cell(row=row_idx, column=13, value=f"Output table from {script_name}")

                # Formatting
                for col in range(1, 14):
                    cell = ws.cell(row=row_idx, column=col)
                    cell.border = self.border
                    cell.alignment = Alignment(vertical='top', wrap_text=True)

                row_idx += 1
                order += 1

        # If no outputs found, show message
        if row_idx == 4:
            ws.cell(row=4, column=1, value="No source tables extracted")
            ws.merge_cells('A4:M4')

        # Column widths
        self._set_sttm_column_widths(ws)
        ws.freeze_panes = 'A4'

    def _create_target_sttm_sheet(
        self,
        wb: Workbook,
        databricks_logic: Dict[str, Any],
        workflow_name: str
    ):
        """Create Target System STTM sheet (tables/columns from Databricks)"""
        ws = wb.create_sheet("Target STTM (Databricks)")

        # Title
        ws['A1'] = f"Target System STTM - {workflow_name}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:M1')

        # Headers
        headers = [
            'Order',                 # 1
            'Schema',                # 2
            'Table Name',            # 3
            'Column Name',           # 4
            'Data Type',             # 5
            'pk?',                   # 6
            'PII?',                  # 7
            'Field Type',            # 8
            'Depends On',            # 9
            'Transformation',        # 10
            'Source Activity',       # 11
            'Notebook Path',         # 12
            'Description'            # 13
        ]

        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Extract tables from Databricks activities
        row_idx = 4
        order = 1

        activities = databricks_logic.get('activities', [])
        for activity in activities:
            activity_name = activity.get('name', 'Unknown')
            notebook_path = activity.get('notebook', '')
            outputs = activity.get('outputs', [])

            for output in outputs:
                # Add row for each output
                ws.cell(row=row_idx, column=1, value=order)
                ws.cell(row=row_idx, column=2, value=databricks_logic.get('pipeline_name', ''))
                ws.cell(row=row_idx, column=3, value=output)
                ws.cell(row=row_idx, column=4, value='(See columns below)')
                ws.cell(row=row_idx, column=5, value='TABLE')
                ws.cell(row=row_idx, column=6, value='')
                ws.cell(row=row_idx, column=7, value='')
                ws.cell(row=row_idx, column=8, value='Output Table')
                ws.cell(row=row_idx, column=9, value='')
                ws.cell(row=row_idx, column=10, value='')
                ws.cell(row=row_idx, column=11, value=activity_name)
                ws.cell(row=row_idx, column=12, value=notebook_path)
                ws.cell(row=row_idx, column=13, value=f"Output from {activity_name}")

                # Formatting
                for col in range(1, 14):
                    cell = ws.cell(row=row_idx, column=col)
                    cell.border = self.border
                    cell.alignment = Alignment(vertical='top', wrap_text=True)

                row_idx += 1
                order += 1

        # If no outputs found, show message
        if row_idx == 4:
            ws.cell(row=4, column=1, value="No target tables extracted")
            ws.merge_cells('A4:M4')

        # Column widths
        self._set_sttm_column_widths(ws)
        ws.freeze_panes = 'A4'

    def _create_sttm_comparison_sheet(
        self,
        wb: Workbook,
        sttm_mappings: List[Dict[str, Any]],
        source_system: str,
        source_workflow: str,
        target_workflow: str
    ):
        """Create STTM Comparison sheet (column-level mappings)"""
        ws = wb.create_sheet("STTM Comparison")

        # Title
        ws['A1'] = f"STTM Comparison: {source_workflow} → {target_workflow}"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:M1')

        # 13-column headers matching manual comparison format
        headers = [
            'Processing Order',      # 1
            'Schema',                # 2
            'Target Table',          # 3
            'Target Field',          # 4
            'Data Type',             # 5
            'pk?',                   # 6
            'contains_pii',          # 7
            'Field Type',            # 8
            'Field Depends On',      # 9
            'Pre Processing Rules',  # 10
            'Source Field Names',    # 11
            'Source Dataset',        # 12
            'Field Definition'       # 13
        ]

        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col, value=header)
            cell.font = self.header_font
            cell.fill = self.header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.border = self.border

        # Data rows
        for row_idx, mapping in enumerate(sttm_mappings, 4):
            # Column 1: Processing Order
            ws.cell(row=row_idx, column=1, value=mapping.get('processing_order', row_idx - 3))

            # Column 2: Schema
            ws.cell(row=row_idx, column=2, value=mapping.get('schema', ''))

            # Column 3: Target Table
            ws.cell(row=row_idx, column=3, value=mapping.get('target_table', ''))

            # Column 4: Target Field
            ws.cell(row=row_idx, column=4, value=mapping.get('target_field', ''))

            # Column 5: Data Type
            ws.cell(row=row_idx, column=5, value=mapping.get('data_type', ''))

            # Column 6: pk? (is_primary_key)
            is_pk = mapping.get('is_pk', False)
            ws.cell(row=row_idx, column=6, value='Yes' if is_pk else 'No')

            # Column 7: contains_pii
            contains_pii = mapping.get('contains_pii', False)
            ws.cell(row=row_idx, column=7, value='Yes' if contains_pii else 'No')

            # Column 8: Field Type
            ws.cell(row=row_idx, column=8, value=mapping.get('field_type', ''))

            # Column 9: Field Depends On
            depends_on = mapping.get('field_depends_on', [])
            if isinstance(depends_on, list):
                depends_on_str = ', '.join(depends_on)
            else:
                depends_on_str = str(depends_on) if depends_on else ''
            ws.cell(row=row_idx, column=9, value=depends_on_str)

            # Column 10: Pre Processing Rules (CRITICAL - must include activity name + formula)
            pre_processing = mapping.get('pre_processing_rules', '')
            ws.cell(row=row_idx, column=10, value=pre_processing)

            # Column 11: Source Field Names
            source_fields = mapping.get('source_field_names', '')
            if isinstance(source_fields, list):
                source_fields = ', '.join(source_fields)
            ws.cell(row=row_idx, column=11, value=source_fields)

            # Column 12: Source Dataset
            ws.cell(row=row_idx, column=12, value=mapping.get('source_dataset', ''))

            # Column 13: Field Definition
            ws.cell(row=row_idx, column=13, value=mapping.get('field_definition', ''))

            # Formatting
            for col in range(1, 14):
                cell = ws.cell(row=row_idx, column=col)
                cell.border = self.border
                cell.alignment = Alignment(vertical='top', wrap_text=True)

            # Center align processing order
            ws.cell(row=row_idx, column=1).alignment = Alignment(horizontal='center', vertical='top')

            # Color code PII columns
            if contains_pii:
                ws.cell(row=row_idx, column=7).fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

            # Color code primary keys
            if is_pk:
                ws.cell(row=row_idx, column=6).fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")

        # Column widths
        self._set_sttm_column_widths(ws)
        ws.freeze_panes = 'A4'

    def _set_sttm_column_widths(self, ws):
        """Set consistent column widths for all STTM sheets"""
        ws.column_dimensions['A'].width = 12   # Order/Processing Order
        ws.column_dimensions['B'].width = 18   # Schema
        ws.column_dimensions['C'].width = 20   # Table
        ws.column_dimensions['D'].width = 22   # Field/Column
        ws.column_dimensions['E'].width = 12   # Data Type
        ws.column_dimensions['F'].width = 8    # pk?
        ws.column_dimensions['G'].width = 12   # PII
        ws.column_dimensions['H'].width = 15   # Field Type
        ws.column_dimensions['I'].width = 25   # Depends On
        ws.column_dimensions['J'].width = 50   # Transformation/Rules
        ws.column_dimensions['K'].width = 25   # Source Fields/Script
        ws.column_dimensions['L'].width = 25   # Source Dataset/Path
        ws.column_dimensions['M'].width = 40   # Description/Definition


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
