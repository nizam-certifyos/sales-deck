"""Schema registry helpers driven by local schema JSON files."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from universal_roster_v2.core.schema_parser import FieldSchema, SystemSchemaParser


class SchemaRegistry:
    """Adapter around system schema parser with convenience metadata helpers."""

    def __init__(self, parser: Optional[SystemSchemaParser] = None):
        self.parser = parser or SystemSchemaParser()
        if not self.parser.practitioner_fields:
            self.parser.parse_all()

    def list_fields(self, roster_type: str) -> List[str]:
        fields = self.parser.practitioner_fields if roster_type == "practitioner" else self.parser.facility_fields
        return sorted(fields.keys())

    def get_field(self, field_name: str, roster_type: str) -> Optional[FieldSchema]:
        return self.parser.get_field(field_name=field_name, roster_type=roster_type)

    def is_valid_field(self, field_name: str, roster_type: str) -> bool:
        if not field_name:
            return False
        return self.get_field(field_name=field_name, roster_type=roster_type) is not None

    def field_metadata(self, field_name: str, roster_type: str) -> Optional[Dict]:
        field = self.get_field(field_name=field_name, roster_type=roster_type)
        if not field:
            return None
        return {
            "name": field.name,
            "title": field.title,
            "description": field.description,
            "format": field.format,
            "pattern": field.pattern,
            "enum": list(field.enum or []),
            "required": bool(field.system_required),
            "entity": field.entity,
            "entity_key": field.entity_key,
            "npi_api_check": bool(field.npi_api_check),
            "object_array_grouping": field.object_array_grouping,
            "object_grouping": field.object_grouping,
            "value_array_grouping": field.value_array_grouping,
            "conditionally_required": field.conditionally_required,
            "dependency": field.dependency,
        }

    def required_fields(self, roster_type: str) -> List[str]:
        return list(self.parser.required_fields.get(roster_type, []))

    def validate_mapping_targets(self, mappings: List[Dict], roster_type: str) -> Tuple[List[Dict], List[Dict]]:
        valid: List[Dict] = []
        invalid: List[Dict] = []
        for mapping in mappings:
            target = str(mapping.get("target_field", "") or "").strip()
            if target and self.is_valid_field(field_name=target, roster_type=roster_type):
                valid.append(mapping)
            else:
                invalid.append(mapping)
        return valid, invalid

    def fields_prompt_block(self, roster_type: str, max_items: int = 500) -> str:
        names = self.list_fields(roster_type)[:max_items]
        lines: List[str] = []
        for name in names:
            meta = self.field_metadata(name, roster_type) or {}
            required = " [REQUIRED]" if meta.get("required") else ""
            enum_count = len(meta.get("enum") or [])
            enum_hint = f" enum={enum_count}" if enum_count else ""
            fmt_hint = f" format={meta.get('format')}" if meta.get("format") else ""
            pattern_hint = " pattern" if meta.get("pattern") else ""
            lines.append(f"- {name}: {meta.get('title') or name}{fmt_hint}{enum_hint}{pattern_hint}{required}")
        return "\n".join(lines)

    def template_summary(self, roster_type: str, mapped_fields: List[str]) -> Dict:
        seen = []
        for field in mapped_fields:
            if field and field not in seen:
                seen.append(field)
        required = set(self.required_fields(roster_type))
        missing_required = sorted(required.difference(seen))

        return {
            "roster_type": roster_type,
            "mapped_field_count": len(seen),
            "required_total": len(required),
            "missing_required_fields": missing_required,
            "mapped_fields": seen,
        }
