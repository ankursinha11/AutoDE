#!/usr/bin/env python3
"""
Hadoop Repository Structure Analyzer
Analyzes multiple Hadoop repositories and categorizes Oozie workflows vs coordinators
Handles different repository structures (oozie/, workflows/, coordinators/)
"""

import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from collections import defaultdict
import re

class HadoopRepoStructureAnalyzer:
    def __init__(self):
        self.repos = []
        self.workflows = []
        self.coordinators = []
        self.repo_summary = defaultdict(lambda: {'workflows': 0, 'coordinators': 0, 'total_pipelines': 0})
        
    def find_repositories(self, base_path):
        """Find all app-* repositories"""
        base = Path(base_path)
        if not base.exists():
            print(f"❌ Base path does not exist: {base_path}")
            return []
            
        repos = []
        for item in base.iterdir():
            if item.is_dir() and item.name.startswith('app-'):
                repos.append(item)
                print(f"📁 Found repository: {item.name}")
        
        return repos
    
    def find_oozie_files(self, repo_path):
        """Find all Oozie workflow and coordinator files in a repository"""
        repo_name = repo_path.name
        workflows = []
        coordinators = []
        
        print(f"\n🔍 Analyzing repository: {repo_name}")
        
        # Pattern 1: Files in oozie/ subdirectories
        oozie_patterns = [
            "**/oozie/**/workflow.xml",
            "**/oozie/**/*workflow*.xml",
            "**/oozie/**/coordinator.xml",
            "**/workflows/**/workflow.xml",
            "**/workflows/**/*workflow*.xml",
            "**/workflows/**/coordinator.xml",
            "**/coordinators/**/coordinator.xml",
            "**/coordinators/**/workflow.xml"
        ]
        
        for pattern in oozie_patterns:
            for file_path in repo_path.rglob(pattern):
                if file_path.is_file():
                    file_type = self.determine_file_type(file_path)
                    if file_type == 'workflow':
                        workflows.append({
                            'repo': repo_name,
                            'file_path': str(file_path),
                            'relative_path': str(file_path.relative_to(repo_path)),
                            'type': 'workflow',
                            'location': self.get_location_type(file_path)
                        })
                    elif file_type == 'coordinator':
                        coordinators.append({
                            'repo': repo_name,
                            'file_path': str(file_path),
                            'relative_path': str(file_path.relative_to(repo_path)),
                            'type': 'coordinator',
                            'location': self.get_location_type(file_path)
                        })
        
        # Pattern 2: Files with _coord suffix
        coord_patterns = [
            "**/*_coord.xml",
            "**/*_coordinator.xml"
        ]
        
        for pattern in coord_patterns:
            for file_path in repo_path.rglob(pattern):
                if file_path.is_file() and self.is_coordinator_file(file_path):
                    coordinators.append({
                        'repo': repo_name,
                        'file_path': str(file_path),
                        'relative_path': str(file_path.relative_to(repo_path)),
                        'type': 'coordinator',
                        'location': 'coordinator_suffix'
                    })
        
        print(f"  📊 Found {len(workflows)} workflows, {len(coordinators)} coordinators")
        
        return workflows, coordinators
    
    def determine_file_type(self, file_path):
        """Determine if file is workflow or coordinator based on content"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Check for workflow elements
            if root.tag.endswith('workflow-app'):
                return 'workflow'
            elif root.tag.endswith('coordinator-app'):
                return 'coordinator'
            
            # Fallback: check for workflow/coordinator elements
            if root.find('.//{uri:oozie:workflow:0.5}workflow-app') is not None:
                return 'workflow'
            elif root.find('.//{uri:oozie:coordinator:0.2}coordinator-app') is not None:
                return 'coordinator'
            elif root.find('.//{uri:oozie:workflow:0.2}workflow-app') is not None:
                return 'workflow'
            elif root.find('.//{uri:oozie:coordinator:0.1}coordinator-app') is not None:
                return 'coordinator'
            
            return 'unknown'
        except Exception as e:
            print(f"    ⚠️  Error parsing {file_path}: {e}")
            return 'unknown'
    
    def is_coordinator_file(self, file_path):
        """Check if file with _coord suffix is actually a coordinator"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            return root.tag.endswith('coordinator-app') or 'coordinator' in root.tag.lower()
        except:
            return False
    
    def get_location_type(self, file_path):
        """Determine the location type of the file"""
        path_str = str(file_path)
        if '/oozie/' in path_str:
            return 'oozie_folder'
        elif '/workflows/' in path_str:
            return 'workflows_folder'
        elif '/coordinators/' in path_str:
            return 'coordinators_folder'
        else:
            return 'other'
    
    def extract_pipeline_info(self, file_path):
        """Extract pipeline name and other info from Oozie file"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Get name attribute
            name = root.get('name', 'Unknown')
            
            # Clean up name (remove variables)
            name = re.sub(r'\$\{.*?\}', '', name).strip()
            name = re.sub(r'\s+', ' ', name)
            
            return {
                'name': name,
                'file_type': 'workflow' if root.tag.endswith('workflow-app') else 'coordinator'
            }
        except Exception as e:
            return {
                'name': Path(file_path).stem,
                'file_type': 'unknown'
            }
    
    def analyze_repositories(self, base_path):
        """Main analysis function"""
        print("🚀 Starting Hadoop Repository Structure Analysis")
        print("=" * 60)
        
        # Find all repositories
        repos = self.find_repositories(base_path)
        if not repos:
            print("❌ No repositories found!")
            return
        
        print(f"\n📊 Found {len(repos)} repositories to analyze")
        
        all_workflows = []
        all_coordinators = []
        unique_workflow_names = set()
        unique_coordinator_names = set()
        
        # Analyze each repository
        for repo in repos:
            workflows, coordinators = self.find_oozie_files(repo)
            
            # Extract pipeline info and deduplicate
            for wf in workflows:
                info = self.extract_pipeline_info(wf['file_path'])
                wf.update(info)
                
                # Only add if we haven't seen this workflow name before
                if wf['name'] not in unique_workflow_names:
                    unique_workflow_names.add(wf['name'])
                    all_workflows.append(wf)
                else:
                    print(f"    ⚠️  Duplicate workflow skipped: {wf['name']}")
            
            for coord in coordinators:
                info = self.extract_pipeline_info(coord['file_path'])
                coord.update(info)
                
                # Only add if we haven't seen this coordinator name before
                if coord['name'] not in unique_coordinator_names:
                    unique_coordinator_names.add(coord['name'])
                    all_coordinators.append(coord)
                else:
                    print(f"    ⚠️  Duplicate coordinator skipped: {coord['name']}")
            
            # Update summary with unique counts
            self.repo_summary[repo.name]['workflows'] = len([w for w in all_workflows if w['repo'] == repo.name])
            self.repo_summary[repo.name]['coordinators'] = len([c for c in all_coordinators if c['repo'] == repo.name])
            self.repo_summary[repo.name]['total_pipelines'] = self.repo_summary[repo.name]['workflows'] + self.repo_summary[repo.name]['coordinators']
        
        self.workflows = all_workflows
        self.coordinators = all_coordinators
        
        print(f"\n✅ Analysis Complete!")
        print(f"🔄 Unique Workflows (Pipelines): {len(all_workflows)}")
        print(f"⏰ Unique Coordinators (Triggers): {len(all_coordinators)}")
        print(f"📊 Total Unique Items: {len(all_workflows) + len(all_coordinators)}")
    
    def generate_excel_report(self, output_file):
        """Generate comprehensive Excel report"""
        print(f"\n📊 Generating Excel report: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # 1. Repository Summary
            summary_data = []
            for repo, counts in self.repo_summary.items():
                summary_data.append({
                    'Repository': repo,
                    'Pipelines (Workflows)': counts['workflows'],
                    'Triggers (Coordinators)': counts['coordinators'],
                    'Total Items': counts['total_pipelines']
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Repository_Summary', index=False)
            
            # 2. PIPELINES ONLY (Workflows)
            if self.workflows:
                pipeline_data = []
                for wf in self.workflows:
                    pipeline_data.append({
                        'Repository': wf['repo'],
                        'Pipeline_Name': wf['name'],
                        'File_Path': wf['relative_path'],
                        'Location_Type': wf['location'],
                        'Type': 'PIPELINE (Workflow)'
                    })
                
                pipelines_df = pd.DataFrame(pipeline_data)
                pipelines_df.to_excel(writer, sheet_name='PIPELINES_ONLY', index=False)
            
            # 3. TRIGGERS ONLY (Coordinators)
            if self.coordinators:
                trigger_data = []
                for coord in self.coordinators:
                    trigger_data.append({
                        'Repository': coord['repo'],
                        'Trigger_Name': coord['name'],
                        'File_Path': coord['relative_path'],
                        'Location_Type': coord['location'],
                        'Type': 'TRIGGER (Coordinator)'
                    })
                
                triggers_df = pd.DataFrame(trigger_data)
                triggers_df.to_excel(writer, sheet_name='TRIGGERS_ONLY', index=False)
            
            # 4. Combined View (for reference)
            combined_data = []
            
            for wf in self.workflows:
                combined_data.append({
                    'Repository': wf['repo'],
                    'Name': wf['name'],
                    'Type': 'PIPELINE',
                    'Location': wf['location'],
                    'File_Path': wf['relative_path']
                })
            
            for coord in self.coordinators:
                combined_data.append({
                    'Repository': coord['repo'],
                    'Name': coord['name'],
                    'Type': 'TRIGGER',
                    'Location': coord['location'],
                    'File_Path': coord['relative_path']
                })
            
            combined_df = pd.DataFrame(combined_data)
            combined_df.to_excel(writer, sheet_name='Combined_View', index=False)
            
            # 5. Location Analysis
            location_data = defaultdict(lambda: {'pipelines': 0, 'triggers': 0})
            
            for wf in self.workflows:
                location_data[wf['location']]['pipelines'] += 1
            
            for coord in self.coordinators:
                location_data[coord['location']]['triggers'] += 1
            
            location_summary = []
            for location, counts in location_data.items():
                location_summary.append({
                    'Location_Type': location,
                    'Pipelines': counts['pipelines'],
                    'Triggers': counts['triggers'],
                    'Total': counts['pipelines'] + counts['triggers']
                })
            
            location_df = pd.DataFrame(location_summary)
            location_df.to_excel(writer, sheet_name='Location_Analysis', index=False)
        
        print(f"✅ Excel report generated: {output_file}")
    
    def print_summary(self):
        """Print analysis summary"""
        print("\n" + "=" * 60)
        print("📊 ANALYSIS SUMMARY")
        print("=" * 60)
        
        print(f"\n🏢 REPOSITORIES ({len(self.repo_summary)}):")
        for repo, counts in self.repo_summary.items():
            print(f"  📁 {repo}: {counts['workflows']} pipelines, {counts['coordinators']} triggers")
        
        print(f"\n📈 TOTALS:")
        print(f"  🔄 Pipelines (Workflows): {len(self.workflows)}")
        print(f"  ⏰ Triggers (Coordinators): {len(self.coordinators)}")
        print(f"  📊 Total Items: {len(self.workflows) + len(self.coordinators)}")
        
        print(f"\n📍 LOCATION BREAKDOWN:")
        location_counts = defaultdict(int)
        for wf in self.workflows:
            location_counts[f"{wf['location']} (pipelines)"] += 1
        for coord in self.coordinators:
            location_counts[f"{coord['location']} (triggers)"] += 1
        
        for location, count in location_counts.items():
            print(f"  📂 {location}: {count}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python hadoop_repo_structure_analyzer.py <base_path>")
        print("Example: python hadoop_repo_structure_analyzer.py /path/to/hadoop/repos")
        sys.exit(1)
    
    base_path = sys.argv[1]
    
    analyzer = HadoopRepoStructureAnalyzer()
    analyzer.analyze_repositories(base_path)
    analyzer.print_summary()
    
    # Generate Excel report
    output_file = "HADOOP_REPO_STRUCTURE_ANALYSIS.xlsx"
    analyzer.generate_excel_report(output_file)
    
    print(f"\n🎉 Analysis complete! Check {output_file} for detailed results.")

if __name__ == "__main__":
    main()
