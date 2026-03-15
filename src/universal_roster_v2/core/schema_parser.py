"""Schema parser for standalone Universal Roster V2."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from universal_roster_v2.config import SCHEMAS_DIR


@dataclass
class FieldSchema:
    name: str
    title: str
    description: str
    type: List[str]
    format: Optional[str] = None
    pattern: Optional[str] = None
    enum: List[str] = field(default_factory=list)
    system_required: bool = False
    entity: Optional[str] = None
    entity_key: Optional[str] = None
    error_message: Optional[str] = None
    npi_api_check: bool = False
    is_array: bool = False
    object_array_grouping: Optional[Dict] = None
    object_grouping: Optional[Dict] = None
    value_array_grouping: Optional[Dict] = None
    conditionally_required: Optional[Dict] = None
    dependency: Optional[List[Dict]] = None

    def to_dict(self) -> Dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


class SystemSchemaParser:
    """Parses practitioner/facility schema JSON into indexed metadata."""

    def __init__(self, practitioner_path: str | Path | None = None, facility_path: str | Path | None = None):
        self.practitioner_path = Path(practitioner_path or (SCHEMAS_DIR / "practitioner-roster-system-fields.json"))
        self.facility_path = Path(facility_path or (SCHEMAS_DIR / "facility-roster-system-fields.json"))

        self.practitioner_fields: Dict[str, FieldSchema] = {}
        self.facility_fields: Dict[str, FieldSchema] = {}
        self.enum_lookup: Dict[str, List[str]] = {}
        self.pattern_lookup: Dict[str, str] = {}
        self.entity_lookup: Dict[str, Dict[str, str]] = {}
        self.required_fields: Dict[str, List[str]] = {"practitioner": [], "facility": []}

    def parse_all(self) -> Dict:
        self.practitioner_fields = {}
        self.facility_fields = {}
        self.enum_lookup = {}
        self.pattern_lookup = {}
        self.entity_lookup = {}
        self.required_fields = {"practitioner": [], "facility": []}

        self._parse_schema_file(self.practitioner_path, target="practitioner")
        self._parse_schema_file(self.facility_path, target="facility")
        self._build_indexes()

        return {
            "practitioner_field_count": len(self.practitioner_fields),
            "facility_field_count": len(self.facility_fields),
            "enum_fields": len(self.enum_lookup),
            "pattern_fields": len(self.pattern_lookup),
            "entities": list(self.entity_lookup.keys()),
            "practitioner_required": self.required_fields["practitioner"],
            "facility_required": self.required_fields["facility"],
        }

    def _parse_schema_file(self, path: Path, target: str) -> None:
        if not path.exists():
            raise FileNotFoundError(f"Schema not found: {path}")

        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        properties = data.get("properties") or {}
        for name, schema in properties.items():
            parsed = self._parse_field(name=name, schema=schema)
            if target == "practitioner":
                self.practitioner_fields[name] = parsed
            else:
                self.facility_fields[name] = parsed

    def _parse_field(self, name: str, schema: Dict) -> FieldSchema:
        metadata = schema.get("metadata") or {}
        return FieldSchema(
            name=name,
            title=schema.get("title", name),
            description=schema.get("description", ""),
            type=list(schema.get("type", ["string"])),
            format=schema.get("format"),
            pattern=schema.get("pattern"),
            enum=[e for e in (schema.get("enum") or []) if e is not None],
            system_required=bool(metadata.get("systemRequired", False)),
            entity=metadata.get("entity"),
            entity_key=metadata.get("entityKey"),
            error_message=metadata.get("errorMessage"),
            npi_api_check=bool(metadata.get("npiApiCheck", False)),
            is_array=bool(metadata.get("isArray", False)),
            object_array_grouping=metadata.get("objectArrayGrouping"),
            object_grouping=metadata.get("objectGrouping"),
            value_array_grouping=metadata.get("valueArrayGrouping"),
            conditionally_required=metadata.get("conditionallyRequired"),
            dependency=metadata.get("dependency"),
        )

    def _build_indexes(self) -> None:
        for roster_type, fields in (("practitioner", self.practitioner_fields), ("facility", self.facility_fields)):
            for name, field in fields.items():
                if field.enum:
                    self.enum_lookup[name] = list(field.enum)
                if field.pattern:
                    self.pattern_lookup[name] = field.pattern
                if field.system_required:
                    self.required_fields[roster_type].append(name)
                if field.entity:
                    self.entity_lookup.setdefault(field.entity, {})[name] = field.entity_key or name

    def get_field(self, field_name: str, roster_type: str = "practitioner") -> Optional[FieldSchema]:
        if roster_type == "practitioner":
            return self.practitioner_fields.get(field_name)
        return self.facility_fields.get(field_name)
