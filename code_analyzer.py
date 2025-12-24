"""
Code Understanding Agent
========================
Uses LLM to understand code logic, extract transformations, and generate
natural language explanations.

Features:
- Column transformation extraction with formulas
- Business logic step extraction
- Data flow analysis (input → output)
- Dependency identification
- Natural language explanations
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from loguru import logger
from pathlib import Path


@dataclass
class ColumnTransformation:
    """A column transformation"""
    source_columns: List[str]
    target_column: str
    transformation_logic: str
    transformation_type: str  # "direct", "calculated", "conditional", etc.
    code_snippet: str
    line_numbers: List[int]
    confidence: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "source_columns": self.source_columns,
            "target_column": self.target_column,
            "transformation_logic": self.transformation_logic,
            "transformation_type": self.transformation_type,
            "code_snippet": self.code_snippet,
            "line_numbers": self.line_numbers,
            "confidence": self.confidence
        }


@dataclass
class CodeAnalysisResult:
    """Result from code analysis"""
    file_path: str
    file_type: str
    summary: str
    business_logic_steps: List[str]
    column_transformations: List[ColumnTransformation]
    input_tables: List[str]
    output_tables: List[str]
    dependencies: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "file_path": self.file_path,
            "file_type": self.file_type,
            "summary": self.summary,
            "business_logic_steps": self.business_logic_steps,
            "column_transformations": [ct.to_dict() for ct in self.column_transformations],
            "input_tables": self.input_tables,
            "output_tables": self.output_tables,
            "dependencies": self.dependencies,
            "metadata": self.metadata
        }


class CodeAnalyzer:
    """
    Analyzes code to extract logic and transformations

    Attributes:
        ai_analyzer: AI analyzer for LLM-based understanding
        file_reader: File reader for loading content
    """

    def __init__(self, ai_analyzer, file_reader):
        """
        Initialize code analyzer

        Args:
            ai_analyzer: AI analyzer (for OpenAI calls)
            file_reader: File reader instance
        """
        self.ai_analyzer = ai_analyzer
        self.file_reader = file_reader

        logger.info("Initialized CodeAnalyzer")

    def analyze_file(
        self,
        file_path: str,
        analysis_type: str = "full"
    ) -> Optional[CodeAnalysisResult]:
        """
        Analyze a code file

        Args:
            file_path: Path to file
            analysis_type: "full", "transformations_only", "summary_only"

        Returns:
            CodeAnalysisResult or None
        """
        # Read file
        file_content = self.file_reader.read_file(file_path)
        if not file_content:
            logger.error(f"Could not read file: {file_path}")
            return None

        logger.info(f"Analyzing file: {file_path} ({file_content.file_type})")

        # Route based on file type
        if file_content.file_type in ["sql", "hive"]:
            return self._analyze_sql(file_content, analysis_type)
        elif file_content.file_type == "python":
            return self._analyze_python(file_content, analysis_type)
        elif file_content.file_type == "pig":
            return self._analyze_pig(file_content, analysis_type)
        elif file_content.file_type == "scala":
            return self._analyze_scala(file_content, analysis_type)
        else:
            return self._analyze_generic(file_content, analysis_type)

    def _analyze_sql(
        self,
        file_content,
        analysis_type: str
    ) -> CodeAnalysisResult:
        """Analyze SQL/Hive file"""

        code = file_content.content

        # Create analysis prompt
        prompt = f"""Analyze this SQL/Hive code and extract:

1. Summary: Brief description of what this code does
2. Business Logic Steps: Step-by-step what happens (bullet points)
3. Column Transformations: For each output column, identify:
   - Source columns used
   - Transformation logic (formula/calculation)
   - Transformation type (direct, calculated, conditional, aggregate)
4. Input Tables: List all input tables/sources
5. Output Tables: List all output tables/targets
6. Dependencies: Any external dependencies (UDFs, functions, other scripts)

SQL CODE:
```sql
{code}
```

Return your analysis in this JSON format:
{{
    "summary": "Brief summary",
    "business_logic_steps": ["Step 1", "Step 2", ...],
    "column_transformations": [
        {{
            "source_columns": ["col1", "col2"],
            "target_column": "output_col",
            "transformation_logic": "Formula or description",
            "transformation_type": "calculated",
            "code_snippet": "relevant SQL snippet",
            "line_numbers": [10, 11, 12]
        }}
    ],
    "input_tables": ["table1", "table2"],
    "output_tables": ["output_table"],
    "dependencies": ["dependency1"]
}}
"""

        try:
            # Call AI analyzer
            result = self.ai_analyzer.analyze_code(
                code=code,
                context=prompt,
                analysis_type="sql_analysis"
            )

            # Parse result
            return self._parse_ai_result(file_content, result)

        except Exception as e:
            logger.error(f"Error analyzing SQL file: {e}")
            return self._create_fallback_result(file_content)

    def _analyze_python(
        self,
        file_content,
        analysis_type: str
    ) -> CodeAnalysisResult:
        """Analyze Python file"""

        code = file_content.content

        prompt = f"""Analyze this Python/PySpark code and extract:

1. Summary: What does this code do?
2. Business Logic Steps: Step-by-step execution flow
3. DataFrame Transformations: For each column transformation:
   - Source columns
   - Target column
   - Transformation logic
   - Type (select, withColumn, join, aggregate, etc.)
4. Input Sources: DataFrames read, tables loaded
5. Output Targets: DataFrames written, tables saved
6. Dependencies: Imports, external functions

PYTHON CODE:
```python
{code}
```

Return JSON in the same format as SQL analysis.
"""

        try:
            result = self.ai_analyzer.analyze_code(
                code=code,
                context=prompt,
                analysis_type="python_analysis"
            )

            return self._parse_ai_result(file_content, result)

        except Exception as e:
            logger.error(f"Error analyzing Python file: {e}")
            return self._create_fallback_result(file_content)

    def _analyze_pig(
        self,
        file_content,
        analysis_type: str
    ) -> CodeAnalysisResult:
        """Analyze Pig script"""

        code = file_content.content

        prompt = f"""Analyze this Pig Latin script and extract:

1. Summary: What does this script do?
2. Business Logic Steps: Step-by-step data transformations
3. Field Transformations: For each field in FOREACH GENERATE:
   - Source fields
   - Target field
   - Transformation (UDF, function, expression)
4. Input Relations: LOAD statements
5. Output Relations: STORE statements
6. Dependencies: UDFs, external JARs

PIG SCRIPT:
```pig
{code}
```

Return JSON in the same format.
"""

        try:
            result = self.ai_analyzer.analyze_code(
                code=code,
                context=prompt,
                analysis_type="pig_analysis"
            )

            return self._parse_ai_result(file_content, result)

        except Exception as e:
            logger.error(f"Error analyzing Pig file: {e}")
            return self._create_fallback_result(file_content)

    def _analyze_scala(
        self,
        file_content,
        analysis_type: str
    ) -> CodeAnalysisResult:
        """Analyze Scala/Spark file"""

        code = file_content.content

        prompt = f"""Analyze this Scala Spark code and extract:

1. Summary: Purpose of this code
2. Business Logic Steps: Transformation pipeline
3. Column Transformations: For each column operation:
   - Source columns
   - Target column
   - Transformation logic
4. Input DataFrames: Data sources
5. Output DataFrames: Data targets
6. Dependencies: Imports, external libraries

SCALA CODE:
```scala
{code}
```

Return JSON in the same format.
"""

        try:
            result = self.ai_analyzer.analyze_code(
                code=code,
                context=prompt,
                analysis_type="scala_analysis"
            )

            return self._parse_ai_result(file_content, result)

        except Exception as e:
            logger.error(f"Error analyzing Scala file: {e}")
            return self._create_fallback_result(file_content)

    def _analyze_generic(
        self,
        file_content,
        analysis_type: str
    ) -> CodeAnalysisResult:
        """Generic code analysis"""

        code = file_content.content

        prompt = f"""Analyze this code file and extract:

1. Summary: What does this code do?
2. Key Operations: Main operations performed
3. Inputs: Data sources or inputs
4. Outputs: Data targets or outputs
5. Dependencies: External dependencies

CODE:
```
{code}
```

Provide a brief analysis.
"""

        try:
            result = self.ai_analyzer.analyze_code(
                code=code,
                context=prompt,
                analysis_type="generic_analysis"
            )

            return self._parse_ai_result(file_content, result)

        except Exception as e:
            logger.error(f"Error analyzing file: {e}")
            return self._create_fallback_result(file_content)

    def _parse_ai_result(
        self,
        file_content,
        ai_result: Dict[str, Any]
    ) -> CodeAnalysisResult:
        """
        Parse AI analysis result into CodeAnalysisResult

        Args:
            file_content: Original file content
            ai_result: Result from AI analyzer

        Returns:
            CodeAnalysisResult
        """
        # Extract column transformations
        transformations = []
        for trans_dict in ai_result.get("column_transformations", []):
            transformations.append(ColumnTransformation(
                source_columns=trans_dict.get("source_columns", []),
                target_column=trans_dict.get("target_column", ""),
                transformation_logic=trans_dict.get("transformation_logic", ""),
                transformation_type=trans_dict.get("transformation_type", "unknown"),
                code_snippet=trans_dict.get("code_snippet", ""),
                line_numbers=trans_dict.get("line_numbers", [])
            ))

        return CodeAnalysisResult(
            file_path=file_content.file_path,
            file_type=file_content.file_type,
            summary=ai_result.get("summary", ""),
            business_logic_steps=ai_result.get("business_logic_steps", []),
            column_transformations=transformations,
            input_tables=ai_result.get("input_tables", []),
            output_tables=ai_result.get("output_tables", []),
            dependencies=ai_result.get("dependencies", [])
        )

    def _create_fallback_result(self, file_content) -> CodeAnalysisResult:
        """Create fallback result when AI analysis fails"""
        return CodeAnalysisResult(
            file_path=file_content.file_path,
            file_type=file_content.file_type,
            summary=f"Analysis unavailable for {Path(file_content.file_path).name}",
            business_logic_steps=[],
            column_transformations=[],
            input_tables=[],
            output_tables=[],
            dependencies=[]
        )

    def extract_column_transformation(
        self,
        file_path: str,
        column_name: str
    ) -> Optional[ColumnTransformation]:
        """
        Extract specific column transformation

        Args:
            file_path: Path to file
            column_name: Column to find

        Returns:
            ColumnTransformation or None
        """
        analysis = self.analyze_file(file_path, analysis_type="full")
        if not analysis:
            return None

        # Find matching transformation
        for trans in analysis.column_transformations:
            if trans.target_column.lower() == column_name.lower():
                return trans

        return None

    def compare_logic(
        self,
        file1_path: str,
        file2_path: str
    ) -> Dict[str, Any]:
        """
        Compare logic between two files

        Args:
            file1_path: First file
            file2_path: Second file

        Returns:
            Comparison results
        """
        # Analyze both files
        analysis1 = self.analyze_file(file1_path)
        analysis2 = self.analyze_file(file2_path)

        if not analysis1 or not analysis2:
            return {"error": "Could not analyze one or both files"}

        # Compare transformations
        trans1_map = {t.target_column: t for t in analysis1.column_transformations}
        trans2_map = {t.target_column: t for t in analysis2.column_transformations}

        common_columns = set(trans1_map.keys()) & set(trans2_map.keys())
        only_in_file1 = set(trans1_map.keys()) - set(trans2_map.keys())
        only_in_file2 = set(trans2_map.keys()) - set(trans1_map.keys())

        # Compare logic for common columns
        differences = []
        for col in common_columns:
            if trans1_map[col].transformation_logic != trans2_map[col].transformation_logic:
                differences.append({
                    "column": col,
                    "file1_logic": trans1_map[col].transformation_logic,
                    "file2_logic": trans2_map[col].transformation_logic
                })

        return {
            "file1": file1_path,
            "file2": file2_path,
            "common_columns": list(common_columns),
            "only_in_file1": list(only_in_file1),
            "only_in_file2": list(only_in_file2),
            "logic_differences": differences,
            "match_percentage": len(common_columns) / max(len(trans1_map), len(trans2_map)) * 100
        }
