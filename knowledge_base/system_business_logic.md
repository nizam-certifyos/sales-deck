# CertifyOS System Business Logic — Complete Reference

**Source:** core-data-access-layer-main + api-layer-main repos
**Purpose:** This is the AI's understanding of how the entire system works

---

## 1. ENTITY MODEL

### Core vs Tenant
- **Core entities** = global, cross-tenant (CorePractitioner, CoreFacility, Group, CoreLocation)
- **Tenant entities** = tenant-scoped relationships (TenantPractitioner, TenantGroup, etc.)
- A provider (Core) can belong to multiple tenants (health plans)

### Key Identifiers
- **certifyId** = system-generated unique ID for CorePractitioner
- **tenantPractitionerId** = links practitioner to a specific tenant
- **NPI** = crosswalk (dedup key) for practitioners
- **TIN:NPI** = crosswalk for groups
- **SHA256(address)** = crosswalk for locations

### Entity Hierarchy
```
Tenant
 └─ TenantGroup (via Group = TIN:NPI)
      └─ TenantGroupPractitioner
      │    └─ GroupPractitionerLocation (has isPrimary, PRI/PRA)
      │    └─ TenantGroupPractitionerNetwork (has effectiveDate/terminationDate)
      │    └─ TenantGroupPractitionerSpecialty
      └─ TenantGroupLocation (via CoreLocation = name_SHA256(address))
      │    └─ TenantGroupLocationNetwork
      │    └─ TenantGroupLocationSpecialty
      └─ TenantGroupNetwork (via Network = name)
           └─ TenantGroupNetworkPlan (via Plan = name+type)
```

---

## 2. HOW A CSV ROW BECOMES DATABASE RECORDS

### Phase 1: Supervisor entities (if supervisingPractitionerNpi present)
### Phase 2: Core entities
- CorePractitioner (UPSERT by NPI crosswalk)
- Group (UPSERT by TIN:NPI crosswalk)
- CoreLocation (UPSERT by name+address hash crosswalk)
- CoreEntityAddress (UPSERT by address hash)
- Network (UPSERT by name within tenant)
- Plan (UPSERT by name+type within payer+tenant)

### Phase 3: Primary relationships
- TenantPractitioner, TenantGroup, TenantGroupPractitioner
- TenantGroupNetwork, TenantGroupLocation

### Phase 4: Location relationships
- GroupPractitionerLocation (this is where PRI/PRA lives)
- TenantGroupPractitionerNetwork
- TenantGroupLocationPractitionerNetwork

### Phase 5: Specialty lookup
- NuccSpecialty (FIND by taxonomy code)
- Specialty (FIND)
- TenantSpecialty (FIND)

### Phase 6: Specialty relationships (cascade)
### Phase 7: ProviderRoster tracking record

---

## 3. FIELD MAPPING: How schema metadata drives entity mapping

Each field in roster-system-fields.json has metadata:
```json
"practitionerNpi": {
  "metadata": {
    "entity": "CorePractitioner",      // → goes to this entity
    "entityKey": "npi",                // → as this field name
    "systemRequired": true,
    "npiApiCheck": true
  }
}
```

The EntityMappingService groups CSV columns by their metadata.entity, creating one map per entity. Special groupings:
- **entityInstanceGrouping**: Multiple groups → separate entity instances (e.g., Specialty 1, Specialty 2)
- **valueArrayGrouping**: Multiple columns → array in one field
- **objectArrayGrouping**: Multiple columns → array of objects

---

## 4. CROSSWALK DEDUPLICATION

| Entity | Crosswalk Formula | Example |
|---|---|---|
| CorePractitioner | NPI | `1508360298` |
| Group | TIN:NPI | `461314303:1457695413` |
| CoreLocation | URL_ENCODE(NORMALIZE(name))_SHA256(addr) | `PLANO_OFFICE_a3f2b1...` |
| CoreEntityAddress | SHA256(line1\|line2\|city\|state\|zip\|type) | `f7c2d8...` |
| CoreFacility | NPI or externalId (tenant-configurable) | `1457695413` |

UPSERT_DATA: If crosswalk exists → UPDATE. If not → CREATE.

---

## 5. LOCATION LOGIC

### Address Types
- **service** = practice location (gets linked to CoreLocation via LocationEntityAddress)
- **mailing, billing, remittance** = only linked to Group (via GroupEntityAddress)
- LocationEntityAddress has entityFilter: ONLY addressType="service"

### PRI vs PRA
- **PRI** = Primary location (isPrimary=true on GroupPractitionerLocation)
- **PRA** = Practice/Additional location (isPrimary=false)
- Determined by the groupPractitionerLocationType field in the roster

### Location Matching
Two addresses = same location if SHA256(normalized address fields) matches.
Normalization: strip non-alphanumeric → collapse underscores → UPPERCASE

---

## 6. TERMINATION LOGIC

### Hierarchy (cascades DOWN)
```
Terminate from Group → kills ALL networks + ALL locations + ALL specialties + ALL plans
Terminate from Network → kills ALL locations under that network + specialties + plans
Terminate from Location → kills ONLY that location + its specialties
```

### How it works
1. Resolve NPI → certifyId → tenantPractitionerId
2. Fetch all existing relationships
3. Find the specific relationship to terminate
4. UPSERT_DATA with terminationDate on the relationship record
5. Cascade terminationDate to child relationships

### All termination date fields → standardized to "terminationDate"
- groupPractitionerNetworkTermEffectiveDate → terminationDate
- practitionerLocationTermEffectiveDate → terminationDate
- groupPractitionerEndDate → terminationDate
- locationTerminationDate → terminationDate
- networkTermDate → terminationDate

---

## 7. DYNAMIC STATUS

Relationship status is computed, not stored:
- terminationDate in past → "inactive"
- terminationDate in future → "active"
- effectiveDate today or past → "active"
- effectiveDate in future → "pending"
- Default → "active"

---

## 8. VALIDATION FLOW

### Pre-ingestion (during VALIDATION phase)
1. JSON schema validation (type, pattern, required, enum, format)
2. NPI NPPES check (if npiApiCheck=true in metadata)
3. Action type validation (conditional required fields per transaction type)
4. Business validation (group must have TIN+NPI+Name if new; location must have full address if new)
5. Specialty validation (taxonomy code must exist for tenant)

### Record Pass/Fail
- ANY validation error → record status = FAILED
- No errors → record status = COMPLETED (sync) or PENDING (async)

---

## 9. SPECIALTY LOGIC

### Lookup chain: NuccSpecialty → Specialty → TenantSpecialty
### isPrimary determination:
- Practitioner: first specialty (index 0) = primary
- Facility: only if key contains "primary"

### Specialty cascade:
INDEXED (per-practitioner): TenantPractitionerSpecialty → TenantGroupPractitionerSpecialty → GroupPractitionerLocationSpecialty
GROUP (per-group): TenantGroupSpecialty → TenantGroupLocationSpecialty → TenantGroupNetworkSpecialty

---

## 10. FIELD PROPAGATION

Values cascade through the entity hierarchy:
- effectiveDate/terminationDate propagate DOWN from TenantGroupPractitioner to all child relationships (LAST_NON_NULL)
- isPrimary propagates DOWN from TenantSpecialty to all specialty relationships (FIRST_NON_NULL)
- practitionerRolesMap propagates from CorePractitioner to GroupPractitionerLocation

---

## 11. ROSTER LIFECYCLE

```
PENDING → VALIDATION_IN_PROGRESS → VALIDATED / VALIDATION_FAILED
                                        ↓ (user approves)
                                     APPROVED → PRE_PROCESSING → IN_PROGRESS → COMPLETED / PARTIAL_IMPORTED / FAILED
```

---

## 12. TEMPLATE SYSTEM

- Templates define which columns appear, how they map, which are required
- Schema = system fields (roster-system-fields.json) + template-specific fields + tenant overrides
- Plan-specific schemas exist: humana-practitioner, humana-facility, zing-roster
- FieldMatchingService suggests CSV header → system field mappings

---

## 13. KEY INSIGHT FOR AI

When processing a roster file, the AI must understand:
1. Each CSV column maps to a specific ENTITY via metadata.entity + metadata.entityKey
2. A single CSV row creates 10-20+ database records across the entity hierarchy
3. Deduplication is via crosswalks — same NPI = same practitioner, same TIN:NPI = same group
4. Address matching is hash-based — exact match required after normalization
5. Transaction type changes WHICH relationships are created/updated/terminated
6. Required fields differ per transaction type AND per file type (practitioner vs facility)
7. Terminations cascade DOWN the hierarchy
8. Status is dynamically computed from effectiveDate and terminationDate
9. Specialties have their own cascade chain separate from the main hierarchy
10. The system NEVER deletes — it sets terminationDate (soft delete)
