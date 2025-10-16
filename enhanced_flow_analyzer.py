import os
import re
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from pathlib import Path
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
import json

class HadoopPipelineConsolidator:
    """
    Consolidates all Hadoop repository pipelines into a single comprehensive Excel report
    """
    
    def __init__(self):
        self.all_pipelines = []
        self.all_actions = []
        self.technology_counts = Counter()
        self.pipeline_counts = Counter()
        self.repo_summary = {}
        
    def find_all_hadoop_repos(self, base_path):
        """Find all Hadoop repositories in the base path"""
        base_path = Path(base_path)
        hadoop_repos = []
        
        # Look for directories starting with 'app-'
        for item in base_path.iterdir():
            if item.is_dir() and item.name.startswith('app-'):
                hadoop_repos.append(item)
        
        return hadoop_repos
    
    def analyze_single_repository(self, repo_path):
        """Analyze a single Hadoop repository"""
        repo_path = Path(repo_path)
        repo_name = repo_path.name
        
        print(f"\n🔍 Analyzing repository: {repo_name}")
        print(f"  📁 Repository path: {repo_path}")
        
        # Find Oozie workflows and coordinators
        workflows = self.find_oozie_workflows(repo_path)
        coordinators = self.find_oozie_coordinators(repo_path)
        
        print(f"  📄 Found {len(workflows)} workflows and {len(coordinators)} coordinators")
        
        # Show specific files found for debugging
        if workflows:
            print(f"  📋 Workflow files:")
            for wf in workflows:
                print(f"    - {wf.relative_to(repo_path)}")
        
        if coordinators:
            print(f"  📋 Coordinator files:")
            for coord in coordinators:
                print(f"    - {coord.relative_to(repo_path)}")
        
        repo_stats = {
            'repo_name': repo_name,
            'workflows': len(workflows),
            'coordinators': len(coordinators),
            'total_pipelines': len(workflows) + len(coordinators)
        }
        
        # Analyze workflows
        for workflow_file in workflows:
            pipeline_info = self.analyze_workflow(workflow_file, repo_name)
            if pipeline_info:
                self.all_pipelines.append(pipeline_info)
                self.all_actions.extend(pipeline_info.get('actions', []))
                self.pipeline_counts[repo_name] += 1
        
        # Analyze coordinators
        for coordinator_file in coordinators:
            coordinator_info = self.analyze_coordinator(coordinator_file, repo_name)
            if coordinator_info:
                self.all_pipelines.append(coordinator_info)
                self.pipeline_counts[repo_name] += 1
        
        self.repo_summary[repo_name] = repo_stats
        
        print(f"  ✅ Analyzed {repo_stats['total_pipelines']} pipelines")
        
        return repo_stats
    
    def find_oozie_workflows(self, repo_path):
        """Find all Oozie workflow files"""
        workflows = []
        
        # Look for ALL workflow files - comprehensive coverage including individual workflows
        patterns = [
            "**/*workflow.xml",                  # ALL workflow files (including individual ones)
            "**/workflow.xml",                   # Main workflow.xml files
            "**/oozie/**/*workflow.xml",         # CDD-style nested oozie folders
            "**/workflows/**/*workflow.xml",     # Standard workflows folder
            "**/workflows/**/oozie/*workflow.xml", # Nested oozie in workflows
            "**/coordinators/**/*workflow.xml",  # Workflows in coordinators folder
            "**/jobs/**/*workflow.xml",          # Workflows in jobs folder
            "**/pipelines/**/*workflow.xml",     # Workflows in pipelines folder
            "**/processes/**/*workflow.xml",     # Workflows in processes folder
            "**/etl/**/*workflow.xml",           # Workflows in etl folder
            "**/data/**/*workflow.xml",          # Workflows in data folder
            "**/ingestion/**/*workflow.xml",     # Workflows in ingestion folder
            "**/processing/**/*workflow.xml",    # Workflows in processing folder
            "**/batch/**/*workflow.xml",         # Workflows in batch folder
            "**/streaming/**/*workflow.xml",     # Workflows in streaming folder
            "**/realtime/**/*workflow.xml",      # Workflows in realtime folder
            "**/scheduled/**/*workflow.xml",     # Workflows in scheduled folder
            "**/automated/**/*workflow.xml",     # Workflows in automated folder
            "**/manual/**/*workflow.xml",        # Workflows in manual folder
            "**/adhoc/**/*workflow.xml",         # Workflows in adhoc folder
            "**/temp/**/*workflow.xml",          # Workflows in temp folder
            "**/test/**/*workflow.xml",          # Workflows in test folder
            "**/dev/**/*workflow.xml",           # Workflows in dev folder
            "**/prod/**/*workflow.xml",          # Workflows in prod folder
            "**/staging/**/*workflow.xml",      # Workflows in staging folder
            "**/qa/**/*workflow.xml"             # Workflows in qa folder
        ]
        
        for pattern in patterns:
            found_files = list(repo_path.rglob(pattern))
            if found_files:
                print(f"    Pattern '{pattern}': Found {len(found_files)} files")
                workflows.extend(found_files)
        
        unique_workflows = list(set(workflows))  # Remove duplicates
        print(f"    Total unique workflows found: {len(unique_workflows)}")
        
        return unique_workflows
    
    def find_oozie_coordinators(self, repo_path):
        """Find all Oozie coordinator files"""
        coordinators = []
        
        # Look for coordinator.xml files - comprehensive coverage
        patterns = [
            "**/coordinator.xml",                    # Any coordinator.xml anywhere
            "**/coordinators/**/coordinator.xml",    # Standard coordinators folder
            "**/oozie/**/coordinator.xml",           # CDD-style nested oozie folders
            "**/workflows/**/coordinator.xml",       # Coordinators in workflows folder
            "**/jobs/**/coordinator.xml",            # Coordinators in jobs folder
            "**/pipelines/**/coordinator.xml",       # Coordinators in pipelines folder
            "**/processes/**/coordinator.xml",       # Coordinators in processes folder
            "**/etl/**/coordinator.xml",             # Coordinators in etl folder
            "**/data/**/coordinator.xml",            # Coordinators in data folder
            "**/ingestion/**/coordinator.xml",       # Coordinators in ingestion folder
            "**/processing/**/coordinator.xml",      # Coordinators in processing folder
            "**/batch/**/coordinator.xml",          # Coordinators in batch folder
            "**/streaming/**/coordinator.xml",      # Coordinators in streaming folder
            "**/realtime/**/coordinator.xml",       # Coordinators in realtime folder
            "**/scheduled/**/coordinator.xml",      # Coordinators in scheduled folder
            "**/automated/**/coordinator.xml",      # Coordinators in automated folder
            "**/manual/**/coordinator.xml",         # Coordinators in manual folder
            "**/adhoc/**/coordinator.xml",          # Coordinators in adhoc folder
            "**/temp/**/coordinator.xml",           # Coordinators in temp folder
            "**/test/**/coordinator.xml",           # Coordinators in test folder
            "**/dev/**/coordinator.xml",            # Coordinators in dev folder
            "**/prod/**/coordinator.xml",           # Coordinators in prod folder
            "**/staging/**/coordinator.xml",        # Coordinators in staging folder
            "**/qa/**/coordinator.xml"              # Coordinators in qa folder
        ]
        
        for pattern in patterns:
            found_files = list(repo_path.rglob(pattern))
            if found_files:
                print(f"    Pattern '{pattern}': Found {len(found_files)} files")
                coordinators.extend(found_files)
        
        unique_coordinators = list(set(coordinators))  # Remove duplicates
        print(f"    Total unique coordinators found: {len(unique_coordinators)}")
        
        return unique_coordinators
    
    def analyze_workflow(self, workflow_file, repo_name):
        """Analyze an Oozie workflow file"""
        try:
            tree = ET.parse(workflow_file)
            root = tree.getroot()
            
            # Extract workflow name
            workflow_name = root.get('name', workflow_file.stem)
            
            # Extract actions
            actions = self.extract_workflow_actions(root, workflow_file)
            
            # Determine processing order
            processing_order = self.determine_processing_order(actions)
            
            # Count technologies
            technologies = [action['technology'] for action in actions]
            tech_counts = Counter(technologies)
            
            pipeline_info = {
                'repo_name': repo_name,
                'pipeline_name': workflow_name,
                'pipeline_type': 'Workflow',
                'file_path': str(workflow_file.relative_to(workflow_file.parents[2])),
                'actions': actions,
                'processing_order': processing_order,
                'technologies': technologies,
                'technology_counts': dict(tech_counts),
                'total_actions': len(actions)
            }
            
            # Update global technology counts
            for tech in technologies:
                self.technology_counts[tech] += 1
            
            return pipeline_info
            
        except Exception as e:
            print(f"    ⚠️ Error analyzing workflow {workflow_file}: {e}")
            return None
    
    def analyze_coordinator(self, coordinator_file, repo_name):
        """Analyze an Oozie coordinator file"""
        try:
            tree = ET.parse(coordinator_file)
            root = tree.getroot()
            
            # Extract coordinator name
            coordinator_name = root.get('name', coordinator_file.stem)
            
            # Extract workflow reference
            workflow_ref = self.extract_workflow_reference(root)
            
            coordinator_info = {
                'repo_name': repo_name,
                'pipeline_name': coordinator_name,
                'pipeline_type': 'Coordinator',
                'file_path': str(coordinator_file.relative_to(coordinator_file.parents[2])),
                'workflow_reference': workflow_ref,
                'actions': [],
                'processing_order': [],
                'technologies': [],
                'technology_counts': {},
                'total_actions': 0
            }
            
            return coordinator_info
            
        except Exception as e:
            print(f"    ⚠️ Error analyzing coordinator {coordinator_file}: {e}")
            return None
    
    def extract_workflow_reference(self, root):
        """Extract workflow reference from coordinator"""
        try:
            # Look for workflow action
            workflow_elem = root.find('.//{uri:oozie:coordinator:0.2}workflow')
            if workflow_elem is not None:
                app_path = workflow_elem.find('{uri:oozie:coordinator:0.2}app-path')
                if app_path is not None:
                    return app_path.text
        except:
            pass
        
        # Fallback: look for any workflow reference
        for elem in root.iter():
            if 'workflow' in elem.tag.lower() and elem.text:
                return elem.text
        
        return "N/A"
    
    def extract_workflow_actions(self, root, workflow_file):
        """Extract actions from workflow"""
        actions = []
        
        # Handle different namespace patterns
        namespaces = [
            '{uri:oozie:workflow:0.5}',
            '{uri:oozie:workflow:0.2}',
            '{uri:oozie:workflow:0.1}',
            '{uri:oozie:workflow:0.3}',
            '{uri:oozie:workflow:0.4}',
            ''
        ]
        
        action_elements = []
        for ns in namespaces:
            action_elements = root.findall(f'.//{ns}action')
            if action_elements:
                print(f"    Found {len(action_elements)} actions with namespace: {ns}")
                break
        
        # If no actions found with namespaces, try without namespace
        if not action_elements:
            action_elements = root.findall('.//action')
            print(f"    Found {len(action_elements)} actions without namespace")
        
        for i, action_elem in enumerate(action_elements):
            action_info = self.parse_action(action_elem, i + 1, workflow_file)
            if action_info:
                actions.append(action_info)
                print(f"    Parsed action {i+1}: {action_info['action_name']} ({action_info['technology']})")
        
        return actions
    
    def parse_action(self, action_elem, order, workflow_file):
        """Parse individual action element"""
        try:
            action_name = action_elem.get('name', f'action_{order}')
            
            # Determine technology and script path
            technology = "Unknown"
            script_path = "N/A"
            
            # Debug: Print action element structure
            print(f"    Analyzing action: {action_name}")
            
            # Check for Spark actions with multiple namespace patterns
            spark_patterns = [
                './/{uri:oozie:spark-action:0.1}spark',
                './/{uri:oozie:spark-action:0.2}spark',
                './/{uri:oozie:spark-action:0.3}spark',
                './/spark'
            ]
            
            spark_elem = None
            for pattern in spark_patterns:
                spark_elem = action_elem.find(pattern)
                if spark_elem is not None:
                    break
            
            if spark_elem is not None:
                technology = "Spark"
                # Look for jar or script with multiple patterns
                jar_patterns = [
                    '{uri:oozie:spark-action:0.1}jar',
                    '{uri:oozie:spark-action:0.2}jar',
                    'jar'
                ]
                
                jar_elem = None
                for pattern in jar_patterns:
                    jar_elem = spark_elem.find(pattern)
                    if jar_elem is not None:
                        break
                
                if jar_elem is not None:
                    script_path = jar_elem.text or "N/A"
                else:
                    # Look for script element
                    script_patterns = [
                        '{uri:oozie:spark-action:0.1}script',
                        '{uri:oozie:spark-action:0.2}script',
                        'script'
                    ]
                    
                    script_elem = None
                    for pattern in script_patterns:
                        script_elem = spark_elem.find(pattern)
                        if script_elem is not None:
                            break
                    
                    if script_elem is not None:
                        script_path = script_elem.text or "N/A"
            
            # Check for Pig actions
            pig_elem = action_elem.find('.//pig')
            if pig_elem is not None:
                technology = "Pig"
                script_elem = pig_elem.find('script')
                if script_elem is not None:
                    script_path = script_elem.text or "N/A"
            
            # Check for Hive actions with multiple namespace patterns
            hive_patterns = [
                './/{uri:oozie:hive-action:0.2}hive',
                './/{uri:oozie:hive-action:0.1}hive',
                './/hive'
            ]
            
            hive_elem = None
            for pattern in hive_patterns:
                hive_elem = action_elem.find(pattern)
                if hive_elem is not None:
                    break
            
            if hive_elem is not None:
                technology = "Hive"
                script_patterns = [
                    '{uri:oozie:hive-action:0.2}script',
                    '{uri:oozie:hive-action:0.1}script',
                    'script'
                ]
                
                script_elem = None
                for pattern in script_patterns:
                    script_elem = hive_elem.find(pattern)
                    if script_elem is not None:
                        break
                
                if script_elem is not None:
                    script_path = script_elem.text or "N/A"
            
            # Check for Shell actions with multiple namespace patterns
            shell_patterns = [
                './/{uri:oozie:shell-action:0.1}shell',
                './/{uri:oozie:shell-action:0.2}shell',
                './/{uri:oozie:shell-action:0.3}shell',
                './/shell'
            ]
            
            shell_elem = None
            for pattern in shell_patterns:
                shell_elem = action_elem.find(pattern)
                if shell_elem is not None:
                    break
            
            if shell_elem is not None:
                technology = "Shell"
                exec_patterns = [
                    '{uri:oozie:shell-action:0.1}exec',
                    '{uri:oozie:shell-action:0.2}exec',
                    'exec'
                ]
                
                exec_elem = None
                for pattern in exec_patterns:
                    exec_elem = shell_elem.find(pattern)
                    if exec_elem is not None:
                        break
                
                if exec_elem is not None:
                    script_path = exec_elem.text or "N/A"
            
            # Check for Java actions with multiple namespace patterns
            java_patterns = [
                './/{uri:oozie:java-action:0.1}java',
                './/{uri:oozie:java-action:0.2}java',
                './/java'
            ]
            
            java_elem = None
            for pattern in java_patterns:
                java_elem = action_elem.find(pattern)
                if java_elem is not None:
                    break
            
            if java_elem is not None:
                technology = "Java"
                main_class_patterns = [
                    '{uri:oozie:java-action:0.1}main-class',
                    '{uri:oozie:java-action:0.2}main-class',
                    'main-class'
                ]
                
                main_class = None
                for pattern in main_class_patterns:
                    main_class = java_elem.find(pattern)
                    if main_class is not None:
                        break
                
                if main_class is not None:
                    script_path = main_class.text or "N/A"
            
            print(f"    Detected: {technology} - {script_path}")
            
            return {
                'action_name': action_name,
                'technology': technology,
                'script_path': script_path,
                'processing_order': order
            }
            
        except Exception as e:
            print(f"    ⚠️ Error parsing action: {e}")
            return None
    
    def determine_processing_order(self, actions):
        """Determine the processing order of actions"""
        if not actions:
            return []
        
        # Actions are already in order based on XML parsing
        return [f"{i+1}. {action['action_name']} ({action['technology']})" 
                for i, action in enumerate(actions)]
    
    def create_consolidated_excel(self, output_file):
        """Create consolidated Excel report"""
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        
        # Create sheets
        self.create_pipeline_overview_sheet(wb)
        self.create_action_details_sheet(wb)
        self.create_technology_summary_sheet(wb)
        self.create_repository_summary_sheet(wb)
        self.create_pipeline_mapping_sheet(wb)
        
        wb.save(output_file)
        print(f"\n📊 Consolidated Excel report created: {output_file}")
    
    def create_pipeline_overview_sheet(self, wb):
        """Create Pipeline Overview sheet"""
        ws = wb.create_sheet("Pipeline_Overview")
        
        # Add title
        ws['A1'] = "Hadoop Pipeline Overview - All Repositories"
        ws['A1'].font = Font(size=16, bold=True)
        
        headers = [
            "Repository", "Pipeline Name", "Pipeline Type", "File Path", 
            "Total Actions", "Technologies", "Processing Order"
        ]
        ws.append(headers)
        
        for pipeline in self.all_pipelines:
            row = [
                pipeline['repo_name'],
                pipeline['pipeline_name'],
                pipeline['pipeline_type'],
                pipeline['file_path'],
                pipeline['total_actions'],
                ', '.join(pipeline['technologies']),
                ' | '.join(pipeline['processing_order'])
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def create_action_details_sheet(self, wb):
        """Create Action Details sheet"""
        ws = wb.create_sheet("Action_Details")
        
        # Add title
        ws['A1'] = "Pipeline Action Details"
        ws['A1'].font = Font(size=16, bold=True)
        
        headers = [
            "Repository", "Pipeline Name", "Action Name", "Technology", 
            "Script Path", "Processing Order"
        ]
        ws.append(headers)
        
        for pipeline in self.all_pipelines:
            for action in pipeline['actions']:
                row = [
                    pipeline['repo_name'],
                    pipeline['pipeline_name'],
                    action['action_name'],
                    action['technology'],
                    action['script_path'],
                    action['processing_order']
                ]
                ws.append(row)
        
        self.format_sheet(ws)
    
    def create_technology_summary_sheet(self, wb):
        """Create Technology Summary sheet"""
        ws = wb.create_sheet("Technology_Summary")
        
        # Add title
        ws['A1'] = "Technology Usage Summary"
        ws['A1'].font = Font(size=16, bold=True)
        
        headers = ["Technology", "Total Count", "Percentage"]
        ws.append(headers)
        
        total_technologies = sum(self.technology_counts.values())
        
        for tech, count in self.technology_counts.most_common():
            percentage = (count / total_technologies * 100) if total_technologies > 0 else 0
            row = [tech, count, f"{percentage:.1f}%"]
            ws.append(row)
        
        # Add summary row
        ws.append([])
        ws.append(["TOTAL", total_technologies, "100.0%"])
        
        self.format_sheet(ws)
    
    def create_repository_summary_sheet(self, wb):
        """Create Repository Summary sheet"""
        ws = wb.create_sheet("Repository_Summary")
        
        # Add title
        ws['A1'] = "Repository Summary"
        ws['A1'].font = Font(size=16, bold=True)
        
        headers = [
            "Repository", "Total Pipelines", "Workflows", "Coordinators", 
            "Total Actions", "Technologies Used"
        ]
        ws.append(headers)
        
        for repo_name, stats in self.repo_summary.items():
            # Count technologies for this repo
            repo_technologies = []
            for pipeline in self.all_pipelines:
                if pipeline['repo_name'] == repo_name:
                    repo_technologies.extend(pipeline['technologies'])
            
            tech_counts = Counter(repo_technologies)
            tech_summary = ', '.join([f"{tech}({count})" for tech, count in tech_counts.items()])
            
            row = [
                repo_name,
                stats['total_pipelines'],
                stats['workflows'],
                stats['coordinators'],
                sum(len(p['actions']) for p in self.all_pipelines if p['repo_name'] == repo_name),
                tech_summary
            ]
            ws.append(row)
        
        # Add totals row
        total_pipelines = sum(stats['total_pipelines'] for stats in self.repo_summary.values())
        total_workflows = sum(stats['workflows'] for stats in self.repo_summary.values())
        total_coordinators = sum(stats['coordinators'] for stats in self.repo_summary.values())
        total_actions = sum(len(p['actions']) for p in self.all_pipelines)
        
        ws.append([])
        ws.append([
            "TOTAL",
            total_pipelines,
            total_workflows,
            total_coordinators,
            total_actions,
            f"{len(self.technology_counts)} different technologies"
        ])
        
        self.format_sheet(ws)
    
    def create_pipeline_mapping_sheet(self, wb):
        """Create Pipeline Mapping sheet - Azure Databricks to Hadoop workflows"""
        ws = wb.create_sheet("Pipeline_Mapping")
        
        # Add title
        ws['A1'] = "Azure Databricks to Hadoop Pipeline Mapping"
        ws['A1'].font = Font(size=16, bold=True)
        
        headers = [
            "S.No", "Project", "Azure Databricks Pipeline", "Hadoop Repository", 
            "Mapped Hadoop Workflow", "Mapping Status", "Confidence Level", "Notes"
        ]
        ws.append(headers)
        
        # Define the mapping from your table
        azure_pipelines = [
            # DATA INGESTION
            {"sno": 1, "project": "DATA INGESTION", "pipeline": "pl_dataingestion_abi_group1", "repo": "app-data-ingestion"},
            {"sno": 2, "project": "", "pipeline": "pl_dataingestion_abi_group2", "repo": "app-data-ingestion"},
            {"sno": 3, "project": "", "pipeline": "pl_dataingestion_big_tables", "repo": "app-data-ingestion"},
            {"sno": 4, "project": "", "pipeline": "pl_dataingestion_fnf", "repo": "app-data-ingestion"},
            
            # CDD
            {"sno": 5, "project": "CDD", "pipeline": "pl_cdd_es_prebdf", "repo": "app-cdd"},
            {"sno": 6, "project": "", "pipeline": "pl_cdd_bdf_download", "repo": "app-cdd"},
            {"sno": 7, "project": "", "pipeline": "pl_cdd_es_postbdf", "repo": "app-cdd"},
            {"sno": 8, "project": "", "pipeline": "pl_cdd_ie_prebdf", "repo": "app-cdd"},
            {"sno": 9, "project": "", "pipeline": "pl_cdd_ie_postbdf", "repo": "app-cdd"},
            {"sno": 10, "project": "", "pipeline": "pl_cdd_tu_prebdf", "repo": "app-cdd"},
            
            # GMRN
            {"sno": 11, "project": "GMRN", "pipeline": "pl_gmrn_ghic", "repo": "app-globalmrn"},
            {"sno": 12, "project": "", "pipeline": "pl_gmrn_merge", "repo": "app-globalmrn"},
            
            # LEAD REPOSITORY
            {"sno": 13, "project": "LEAD REPOSITORY", "pipeline": "pl_leadrepo_escan_ich_import", "repo": "app-lead-repository"},
            {"sno": 14, "project": "", "pipeline": "pl_leadrepo_escan_import_fc", "repo": "app-lead-repository"},
            {"sno": 15, "project": "", "pipeline": "pl_leadrepository_xref", "repo": "app-lead-repository"},
            
            # LSB
            {"sno": 16, "project": "LSB", "pipeline": "pl_leadservicebase", "repo": "app-lead-service"},
            
            # LEAD DISCOVERY
            {"sno": 17, "project": "LEAD DISCOVERY", "pipeline": "pl_leaddiscovery_globalmrn_assign", "repo": "app-lead-discovery"},
            {"sno": 18, "project": "", "pipeline": "pl_leaddiscovery_medicareleads", "repo": "app-lead-discovery"},
            {"sno": 19, "project": "", "pipeline": "pl_leaddiscovery_medicaidleads", "repo": "app-lead-discovery"},
            {"sno": 20, "project": "", "pipeline": "pl_leaddiscovery_lead_propagation", "repo": "app-lead-discovery"},
            {"sno": 21, "project": "", "pipeline": "pl_leaddiscovery_known_commercial", "repo": "app-lead-discovery"},
            {"sno": 22, "project": "", "pipeline": "pl_leaddiscovery_leadlookup_knowncommercial", "repo": "app-lead-discovery"},
            {"sno": 23, "project": "", "pipeline": "pl_leaddiscovery_leadverify", "repo": "app-lead-discovery"},
            {"sno": 24, "project": "", "pipeline": "pl_leaddiscovery_subdob", "repo": "app-lead-discovery"},
            
            # OPS HINTS
            {"sno": 25, "project": "OPS HINTS", "pipeline": "pl_ops_hints", "repo": "app-ops-hints"},
            
            # HELPERS
            {"sno": 26, "project": "Helpers", "pipeline": "pl_mbihelper", "repo": "app-coverage-helpers"}
        ]
        
        # Create mapping for each Azure pipeline
        for azure_pipeline in azure_pipelines:
            mapped_workflow = self.find_matching_hadoop_workflow(azure_pipeline["pipeline"], azure_pipeline["repo"])
            
            row = [
                azure_pipeline["sno"],
                azure_pipeline["project"],
                azure_pipeline["pipeline"],
                azure_pipeline["repo"],
                mapped_workflow["workflow_name"],
                mapped_workflow["status"],
                mapped_workflow["confidence"],
                mapped_workflow["notes"]
            ]
            ws.append(row)
        
        self.format_sheet(ws)
    
    def find_matching_hadoop_workflow(self, azure_pipeline_name, repo_name):
        """Find matching Hadoop workflow for Azure pipeline"""
        # Direct matches we know exist
        direct_matches = {
            "pl_dataingestion_big_tables": {
                "workflow_name": "big_tables_workflow.xml",
                "status": "✅ FOUND",
                "confidence": "High",
                "notes": "Direct match found in ingest_all folder"
            },
            "pl_dataingestion_fnf": {
                "workflow_name": "sqoop_fnf_workflow.xml", 
                "status": "✅ FOUND",
                "confidence": "High",
                "notes": "Direct match found in ingest_all folder"
            }
        }
        
        # Check if we have this repository analyzed
        if repo_name not in self.repo_summary:
            return {
                "workflow_name": "N/A",
                "status": "❌ REPO NOT ANALYZED",
                "confidence": "N/A",
                "notes": f"Repository {repo_name} not found in current analysis"
            }
        
        # Check for direct match
        if azure_pipeline_name in direct_matches:
            return direct_matches[azure_pipeline_name]
        
        # Look for similar workflows in the analyzed pipelines
        found_workflows = [p for p in self.all_pipelines if p['repo_name'] == repo_name]
        
        if not found_workflows:
            return {
                "workflow_name": "N/A",
                "status": "❌ NO WORKFLOWS FOUND",
                "confidence": "N/A", 
                "notes": f"No workflows found in {repo_name}"
            }
        
        # Try to find similar workflow names
        pipeline_keywords = azure_pipeline_name.lower().replace("pl_", "").split("_")
        
        for workflow in found_workflows:
            workflow_name = workflow['pipeline_name'].lower()
            
            # Check for keyword matches
            matches = sum(1 for keyword in pipeline_keywords if keyword in workflow_name)
            if matches >= 2:  # At least 2 keywords match
                return {
                    "workflow_name": workflow['pipeline_name'],
                    "status": "🔍 SIMILAR FOUND",
                    "confidence": "Medium",
                    "notes": f"Similar workflow found with {matches} keyword matches"
                }
        
        # If no similar match found, list available workflows
        available_workflows = [w['pipeline_name'] for w in found_workflows]
        return {
            "workflow_name": f"Available: {', '.join(available_workflows[:3])}{'...' if len(available_workflows) > 3 else ''}",
            "status": "❌ NO MATCH FOUND",
            "confidence": "Low",
            "notes": f"No matching workflow found. Available workflows: {len(available_workflows)}"
        }
    
    def format_sheet(self, ws):
        """Format Excel sheet with basic styling"""
        try:
            # Set column widths
            for col_idx in range(1, ws.max_column + 1):
                try:
                    column_letter = ws.cell(row=1, column=col_idx).column_letter
                    ws.column_dimensions[column_letter].width = 20
                except:
                    pass
        except Exception as e:
            print(f"    ⚠️ Warning: Could not format sheet: {e}")

def main():
    """Main function to run the Hadoop Pipeline Consolidator"""
    import sys
    
    print("=" * 80)
    print("🚀 HADOOP PIPELINE CONSOLIDATOR")
    print("=" * 80)
    print("Analyzes all Hadoop repositories and creates a consolidated pipeline report")
    print()
    
    # Get base path from command line arguments or prompt
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    else:
        base_path = input("Enter the path containing all Hadoop repositories: ").strip()
    
    if not base_path:
        print("Error: Repository path is required!")
        print("Usage: python hadoop_pipeline_consolidator.py <path_to_repositories>")
        print("Example: python hadoop_pipeline_consolidator.py OneDrive_1_7-25-2025/Hadoop")
        return
    
    if not os.path.exists(base_path):
        print(f"Error: Repository path does not exist: {base_path}")
        return
    
    # Initialize consolidator
    consolidator = HadoopPipelineConsolidator()
    
    # Find all Hadoop repositories
    print(f"\n🔍 Scanning for Hadoop repositories in: {base_path}")
    hadoop_repos = consolidator.find_all_hadoop_repos(base_path)
    
    if not hadoop_repos:
        print("❌ No Hadoop repositories found!")
        return
    
    print(f"✅ Found {len(hadoop_repos)} Hadoop repositories:")
    for repo in hadoop_repos:
        print(f"  - {repo.name}")
    
    # Analyze each repository
    print(f"\n📊 Analyzing all repositories...")
    total_stats = {
        'repositories': len(hadoop_repos),
        'pipelines': 0,
        'actions': 0,
        'technologies': 0
    }
    
    for repo in hadoop_repos:
        stats = consolidator.analyze_single_repository(repo)
        total_stats['pipelines'] += stats['total_pipelines']
    
    total_stats['actions'] = sum(len(p['actions']) for p in consolidator.all_pipelines)
    total_stats['technologies'] = len(consolidator.technology_counts)
    
    # Generate consolidated Excel report
    output_file = "HADOOP_PIPELINE_CONSOLIDATED_REPORT.xlsx"
    consolidator.create_consolidated_excel(output_file)
    
    # Print summary
    print(f"\n{'='*80}")
    print("🎯 ANALYSIS SUMMARY")
    print(f"{'='*80}")
    print(f"📁 Repositories Analyzed: {total_stats['repositories']}")
    print(f"🔄 Total Pipelines: {total_stats['pipelines']}")
    print(f"⚙️ Total Actions: {total_stats['actions']}")
    print(f"🛠️ Technologies Found: {total_stats['technologies']}")
    
    print(f"\n📊 Technology Breakdown:")
    for tech, count in consolidator.technology_counts.most_common():
        print(f"  {tech}: {count}")
    
    print(f"\n📄 Repository Breakdown:")
    for repo_name, count in consolidator.pipeline_counts.items():
        print(f"  {repo_name}: {count} pipelines")
    
    print(f"\n🎉 Analysis complete! Check the Excel file: {output_file}")

if __name__ == "__main__":
    main()
