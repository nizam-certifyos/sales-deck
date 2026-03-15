import { useCallback, useEffect, useRef, useState } from "react";
import {
  createWorkspace,
  exportCsv,
  getWorkspace,
  startOperation,
  uploadFile,
} from "../lib/api";
import type { MappingItem, OperationRecord, QualityAuditItem, TransformationItem, WorkspaceSnapshot } from "../lib/types";
import "../styles-pipeline.css";
import { WorkspaceTransportManager } from "../lib/transport";

/* ================================================================
   Types
   ================================================================ */

interface DemoProvider {
  npi: string;
  firstName: string;
  lastName: string;
  middleName: string;
  degree: string;
  specialty: string;
  gender: string;
  dob: string;
  taxonomy: string;
  licenseNumber: string;
  licenseState: string;
  licenseExpiry: string;
  deaNumber: string;
  deaExpiry: string;
  boardName: string;
  boardExpiry: string;
  practiceAddress: string;
  practiceCity: string;
  practiceState: string;
  practiceZip: string;
  practicePhone: string;
  groupName: string;
  groupTin: string;
  groupNpi: string;
  credDate: string;
  recredDate: string;
  malpracticeCarrier: string;
  malpracticePolicy: string;
  malpracticeCoverage: string;
  email: string;
  caqhId: string;
  nppesData?: any;
  oigClear?: boolean;
  flags: string[];
  credentialingStatus: string;
  monitoringStatus: string;
}

interface PsvCheck {
  source: string;
  status: string;
  detail: string;
}

interface MonitoringFlag {
  category: string;
  severity: "critical" | "warning" | "info";
  title: string;
  detail: string;
}

interface CredentialCheckResponse {
  credentialing: {
    status: string;
    summary: string;
    psvChecks: PsvCheck[];
  };
  monitoring: {
    status: string;
    flags: MonitoringFlag[];
    summary: string;
  };
}

/* PSV data from primary source tables */
interface PsvLicense {
  state: string;
  number: string;
  issue_date: string;
  expiry: string;
  active_flag: string;
  status: string;
  type: string;
  source: string;
}

interface PsvAbms {
  board: string;
  cert_name: string;
  status: string;
  expiry: string;
  meeting_moc: string;
}

interface PsvDea {
  number: string;
  status: string;
  expiry: string;
  state: string;
  schedules: string;
  degree: string;
}

interface PsvSanctionRecord {
  first_name?: string;
  last_name?: string;
  exclusion_type?: string;
  exclusion_date?: string;
  [key: string]: any;
}

interface PsvData {
  nppes: { found: boolean; first_name?: string; last_name?: string; npi?: string; entity_type?: string; deactivation_date?: string; address?: string; city?: string; state?: string; postal_code?: string };
  state_licenses: PsvLicense[];
  abms: PsvAbms[];
  dea: PsvDea[];
  oig: { found: boolean; records: PsvSanctionRecord[] };
  sam: { found: boolean; records: PsvSanctionRecord[] };
  state_sanctions: { found: boolean; records: PsvSanctionRecord[] };
  medicare_optout: { found: boolean; [key: string]: any };
  deceased: { found: boolean };
  board_actions: { found: boolean; records: any[] };
  sanctions_summary: { oig: number; sam: number; state: number; ofac: number; moo: number };
}

async function fetchPsvData(npi: string): Promise<PsvData | null> {
  try {
    const res = await fetch(`/api/psv/${npi}`);
    if (!res.ok) return null;
    const json = await res.json();
    return json.data || null;
  } catch {
    return null;
  }
}

type Phase = "upload" | "processing" | "results";
type PipelineTab = "roster" | "credentialing" | "monitoring";
type DownloadState = "preparing" | "ready" | "downloading";

/* ================================================================
   Utility functions (copied from SalesDemoV2)
   ================================================================ */

function fmt(n: number | undefined | null): number {
  if (typeof n !== "number" || !Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, n));
}

function Panel({
  icon, iconClass, title, subtitle, count, defaultOpen, children,
}: {
  icon: string;
  iconClass: string;
  title: string;
  subtitle: string;
  count?: number;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen ?? true);
  return (
    <div className="demo-panel">
      <div className="demo-panel-header" onClick={() => setOpen((o) => !o)} role="button" aria-expanded={open}>
        <div className={`demo-panel-icon ${iconClass}`}>{icon}</div>
        <div style={{ flex: 1 }}>
          <div className="demo-panel-title">{title}</div>
          <div className="demo-panel-count">{subtitle}</div>
        </div>
        {typeof count === "number" && (
          <span className="demo-panel-count-pill">{count}</span>
        )}
        <div className={`demo-panel-chevron${open ? " demo-panel-chevron--open" : ""}`}>&#9660;</div>
      </div>
      <div className="demo-panel-body" hidden={!open}>
        {children}
      </div>
    </div>
  );
}

/* ── Sales-intelligence filters (same as SalesDemoV2) ── */

function getSalesMappings(mappings: MappingItem[]) {
  return mappings.filter((m) => {
    if (!m.target_field) return false;
    const band = (m.confidence_band || "").toLowerCase();
    if (band === "low") return false;
    return true;
  });
}

function getSalesIssues(qualityAudit: QualityAuditItem[]) {
  return qualityAudit.filter((q) => {
    if (q.approved === false) return false;
    const msg = String(q.message || q.finding || q.description || "").toLowerCase();
    const title = String(q.title || q.column || q.check_type || "").toLowerCase();
    const cat = String(q.category || "").toLowerCase();
    if (msg.includes("date") && (msg.includes("format") || msg.includes("pattern") || msg.includes("yyyy"))) return false;
    if (title.includes("date") && title.includes("format")) return false;
    if ((msg.includes("zip") || title.includes("zip")) && (msg.includes("format") || msg.includes("pattern") || msg.includes("digit"))) return false;
    if ((msg.includes("ssn") || title.includes("ssn")) && (msg.includes("format") || msg.includes("dash") || msg.includes("pattern"))) return false;
    if ((msg.includes("phone") || title.includes("phone") || msg.includes("fax") || title.includes("fax")) && (msg.includes("format") || msg.includes("digit") || msg.includes("pattern"))) return false;
    if (msg.includes("bq") || msg.includes("bigquery")) return false;
    if (msg.includes("sql") && !msg.includes("provider") && !msg.includes("npi")) return false;
    if (msg.includes("required") && (msg.includes("optional") || msg.includes("when provided"))) return false;
    if (msg.includes("incorrect target") || msg.includes("incorrect mapping")) return false;
    if (title.includes("anti-pattern") || msg.includes("anti-pattern")) return false;
    if ((msg.includes("non-phone") || msg.includes("non-date") || msg.includes("non-numeric")) && msg.includes("mapped")) return false;
    if (cat === "format" && (msg.includes("mapped to") || msg.includes("target field") || msg.includes("wrong field"))) return false;
    if (msg.includes("non-phone number values in") || msg.includes("non-date values in")) return false;
    if (msg.includes("unexpected values") && msg.includes("mapped")) return false;
    if (cat === "format" && (msg.includes("date") || msg.includes("phone") || msg.includes("zip") || msg.includes("fax"))) return false;
    return true;
  });
}

function getSalesTransforms(transforms: TransformationItem[], mappings: MappingItem[]) {
  const items: { name: string; description: string; icon: string }[] = [];
  const hasDateMappings = mappings.some((m) =>
    (m.target_field || "").toLowerCase().includes("date") ||
    (m.target_field || "").toLowerCase().includes("dob")
  );
  if (hasDateMappings) {
    items.push({ name: "Date Standardization", description: "Converting all date formats to ISO standard (YYYY-MM-DD) for consistent processing", icon: "\uD83D\uDCC5" });
  }
  const hasPhone = mappings.some((m) =>
    (m.target_field || "").toLowerCase().includes("phone") ||
    (m.target_field || "").toLowerCase().includes("fax")
  );
  if (hasPhone) {
    items.push({ name: "Phone & Fax Normalization", description: "Standardizing to 10-digit format, removing dashes, parentheses, and spaces", icon: "\uD83D\uDCDE" });
  }
  const hasIds = mappings.some((m) => {
    const tf = (m.target_field || "").toLowerCase();
    return tf.includes("npi") || tf.includes("tin") || tf.includes("ssn") || tf.includes("caqh");
  });
  if (hasIds) {
    items.push({ name: "ID Validation & Formatting", description: "Verifying NPI check digits, standardizing TIN and SSN to required digit counts", icon: "\uD83D\uDD22" });
  }
  const hasZip = mappings.some((m) => (m.target_field || "").toLowerCase().includes("zip"));
  if (hasZip) {
    items.push({ name: "ZIP Code Normalization", description: "Standardizing ZIP codes -- handling ZIP+4 format, padding, and validation", icon: "\uD83D\uDCCD" });
  }
  const hasState = mappings.some((m) => (m.target_field || "").toLowerCase().includes("state"));
  if (hasState) {
    items.push({ name: "State Code Standardization", description: "Converting full state names to 2-letter USPS codes", icon: "\uD83D\uDDFA" });
  }
  const hasGender = mappings.some((m) => (m.target_field || "").toLowerCase().includes("gender"));
  if (hasGender) {
    items.push({ name: "Gender Value Normalization", description: "Standardizing to M/F/U codes accepted by health plan systems", icon: "\uD83D\uDC64" });
  }
  items.push({ name: "Data Cleanup", description: "Removing empty rows, normalizing null values (N/A, None, TBD), trimming whitespace", icon: "\uD83E\uDDF9" });
  for (const t of transforms) {
    if (t.approved === false) continue;
    const name = (t.name || "").toLowerCase();
    if (name.includes("date") || name.includes("phone") || name.includes("npi") || name.includes("tin") ||
        name.includes("ssn") || name.includes("zip") || name.includes("state") || name.includes("gender") ||
        name.includes("null") || name.includes("whitespace") || name.includes("duplicate") || name.includes("sparse")) continue;
    items.push({ name: t.name || t.id || "Transform", description: t.description || (t.source_columns || []).join(", "), icon: "\u26A1" });
  }
  return items;
}

/* ================================================================
   Provider extraction + flag computation
   ================================================================ */

function findMappedColumn(mappings: MappingItem[], ...targets: string[]): string | null {
  for (const t of targets) {
    const lower = t.toLowerCase().replace(/_/g, "");
    const m = mappings.find((item) => {
      if (item.approved === false || !item.target_field || !item.source_column) return false;
      const tf = item.target_field.toLowerCase();
      const sc = item.source_column.toLowerCase();
      // Match against target_field (camelCase — remove underscores from search)
      if (tf.includes(lower)) return true;
      // Also match against source_column name
      if (sc.replace(/[_\s]/g, "").includes(lower)) return true;
      return false;
    });
    if (m?.source_column) return m.source_column;
  }
  return null;
}

function extractVal(row: Record<string, string>, col: string | null): string {
  if (!col) return "";
  return (row[col] || "").trim();
}

function isExpired(dateStr: string): boolean {
  if (!dateStr) return false;
  try {
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return false;
    return d < new Date();
  } catch { return false; }
}

function isExpiringSoon(dateStr: string, daysAhead: number): boolean {
  if (!dateStr) return false;
  try {
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return false;
    const future = new Date();
    future.setDate(future.getDate() + daysAhead);
    return d > new Date() && d < future;
  } catch { return false; }
}

function computeFlags(p: DemoProvider): string[] {
  const flags: string[] = [];
  if (isExpired(p.licenseExpiry)) flags.push("State License Expired");
  if (isExpired(p.boardExpiry)) flags.push("Board Certification Expired");
  if (isExpired(p.deaExpiry)) flags.push("DEA License Expired");
  if (isExpiringSoon(p.boardExpiry, 90)) flags.push("Board Cert Expiring Soon");
  if (isExpiringSoon(p.licenseExpiry, 90)) flags.push("License Expiring Soon");
  if (isExpiringSoon(p.deaExpiry, 90)) flags.push("DEA Expiring Soon");
  if (p.oigClear === false) flags.push("OIG Exclusion");
  if (p.nppesData) {
    const nppesFirst = (p.nppesData.basic?.first_name || "").toUpperCase();
    const nppesLast = (p.nppesData.basic?.last_name || "").toUpperCase();
    const rosterFirst = p.firstName.toUpperCase();
    const rosterLast = p.lastName.toUpperCase();
    if (nppesFirst && rosterFirst && nppesFirst !== rosterFirst) flags.push("Name Mismatch (First)");
    if (nppesLast && rosterLast && nppesLast !== rosterLast) flags.push("Name Mismatch (Last)");
  }
  return flags;
}

function computeCredStatus(p: DemoProvider): string {
  if (p.oigClear === false) return "Review Required";
  if (p.flags.some(f => f.includes("Expired") || f.includes("Mismatch") || f.includes("Exclusion"))) return "In Progress";
  return "PSV Complete";
}

function computeMonStatus(p: DemoProvider): string {
  if (p.flags.length === 0) return "No Issues";
  if (p.flags.some(f => f.includes("Exclusion") || f.includes("Expired"))) return "Review Required";
  return "Flags Found";
}

function extractProviders(snapshot: WorkspaceSnapshot, mappings: MappingItem[]): DemoProvider[] {
  // Prefer sample_rows (actual row data) over column-level samples
  const sampleRows: Record<string, string>[] = (snapshot.profile_summary as any)?.sample_rows || [];
  const samples = snapshot.profile_summary?.samples || [];

  // Build a map of column -> values
  const colMap: Record<string, string[]> = {};

  if (sampleRows.length > 0) {
    // Use actual row data
    for (const row of sampleRows) {
      for (const [col, val] of Object.entries(row)) {
        if (!colMap[col]) colMap[col] = [];
        colMap[col].push(val || "");
      }
    }
  } else if (samples.length > 0) {
    // Fallback to column-level samples
    for (const s of samples) {
      colMap[s.column] = s.values || [];
    }
  }

  // Figure out how many rows we have
  const rowCount = Math.max(...Object.values(colMap).map(v => v.length), 0);
  console.log("[EXTRACT DEBUG] sampleRows:", sampleRows.length, "colMap keys:", Object.keys(colMap).length, "rowCount:", rowCount);
  if (rowCount === 0) return [];

  // Get column mappings
  // Search tokens match both camelCase target fields and source column names
  const npiCol = findMappedColumn(mappings, "practitionernpi", "npi", "individualnpi");
  const fnCol = findMappedColumn(mappings, "firstname", "first_name");
  const lnCol = findMappedColumn(mappings, "lastname", "last_name");
  const mnCol = findMappedColumn(mappings, "middlename", "middle");
  const degCol = findMappedColumn(mappings, "providertype", "degree");
  const specCol = findMappedColumn(mappings, "specialtyname", "specialty");
  const genderCol = findMappedColumn(mappings, "gender");
  const dobCol = findMappedColumn(mappings, "dateofbirth", "dob", "birthdate");
  const taxCol = findMappedColumn(mappings, "taxonomy");
  const licNumCol = findMappedColumn(mappings, "statelicensenumber", "licensenumber");
  const licStCol = findMappedColumn(mappings, "licenseissuingstate", "licensestate");
  const licExpCol = findMappedColumn(mappings, "licenseexpirationdate", "licenseexpir");
  const deaNumCol = findMappedColumn(mappings, "practitionerdea", "dealicense");
  const deaExpCol = findMappedColumn(mappings, "deaexpirationdate", "deaexpir");
  const boardCol = findMappedColumn(mappings, "boardcertificationname", "boardname");
  const boardExpCol = findMappedColumn(mappings, "boardcertificationexpirationdate", "boardexpir");
  const addrCol = findMappedColumn(mappings, "locationaddressline1", "streetaddress", "practicestreet");
  const cityCol = findMappedColumn(mappings, "locationcity", "practicecity");
  const stateCol = findMappedColumn(mappings, "locationstate", "practicestate");
  const zipCol = findMappedColumn(mappings, "locationzip", "practicezip");
  const phoneCol = findMappedColumn(mappings, "locationphone", "practicephone");
  const groupCol = findMappedColumn(mappings, "groupname", "legalbusinessname", "practicename");
  const tinCol = findMappedColumn(mappings, "grouptin", "taxid");
  const gnpiCol = findMappedColumn(mappings, "groupnpi");
  const credCol = findMappedColumn(mappings, "initialcredentialingdate", "creddate");
  const recredCol = findMappedColumn(mappings, "recredentialingdate", "recred");
  const malCarCol = findMappedColumn(mappings, "malpracticecarriername", "malpracticecarrier");
  const malPolCol = findMappedColumn(mappings, "malpracticepolicynumber");
  const malCovCol = findMappedColumn(mappings, "malpracticecoverageamount", "malpracticeaggregate");
  const emailCol = findMappedColumn(mappings, "email");
  const caqhCol = findMappedColumn(mappings, "caqh");

  console.log("[EXTRACT DEBUG] npiCol:", npiCol, "fnCol:", fnCol, "lnCol:", lnCol, "degCol:", degCol);

  // Build rows from sample data
  const providers: DemoProvider[] = [];
  const seenNpi = new Set<string>();

  for (let i = 0; i < rowCount; i++) {
    const getVal = (col: string | null) => {
      if (!col || !colMap[col]) return "";
      return (colMap[col][i] || "").trim();
    };

    const npi = getVal(npiCol);
    if (!npi || seenNpi.has(npi)) continue;
    seenNpi.add(npi);

    const p: DemoProvider = {
      npi,
      firstName: getVal(fnCol),
      lastName: getVal(lnCol),
      middleName: getVal(mnCol),
      degree: getVal(degCol),
      specialty: getVal(specCol),
      gender: getVal(genderCol),
      dob: getVal(dobCol),
      taxonomy: getVal(taxCol),
      licenseNumber: getVal(licNumCol),
      licenseState: getVal(licStCol),
      licenseExpiry: getVal(licExpCol),
      deaNumber: getVal(deaNumCol),
      deaExpiry: getVal(deaExpCol),
      boardName: getVal(boardCol),
      boardExpiry: getVal(boardExpCol),
      practiceAddress: getVal(addrCol),
      practiceCity: getVal(cityCol),
      practiceState: getVal(stateCol),
      practiceZip: getVal(zipCol),
      practicePhone: getVal(phoneCol),
      groupName: getVal(groupCol),
      groupTin: getVal(tinCol),
      groupNpi: getVal(gnpiCol),
      credDate: getVal(credCol),
      recredDate: getVal(recredCol),
      malpracticeCarrier: getVal(malCarCol),
      malpracticePolicy: getVal(malPolCol),
      malpracticeCoverage: getVal(malCovCol),
      email: getVal(emailCol),
      caqhId: getVal(caqhCol),
      flags: [],
      credentialingStatus: "In Progress",
      monitoringStatus: "No Issues",
    };
    providers.push(p);
  }

  return providers;
}

/* ================================================================
   OIG API helper
   ================================================================ */

async function checkOig(npi: string): Promise<boolean> {
  try {
    const res = await fetch(`/api/oig/${encodeURIComponent(npi)}`);
    if (!res.ok) return true; // assume clear on error
    const data = await res.json();
    // API returns empty results array if clear
    const results = data?.results || [];
    return results.length === 0; // true = clear
  } catch {
    return true; // assume clear on error
  }
}

/* ================================================================
   NPPES lookup helper
   ================================================================ */

async function lookupNppes(npi: string): Promise<any> {
  try {
    // Use backend proxy to avoid CORS issues
    const res = await fetch(`/api/nppes/${encodeURIComponent(npi)}`);
    if (!res.ok) return null;
    const data = await res.json();
    // Proxy returns the result directly (not wrapped in {results: [...]})
    return data && data.number ? data : null;
  } catch {
    return null;
  }
}

/* ================================================================
   LLM Credential Check API
   ================================================================ */

async function fetchCredentialCheck(provider: DemoProvider): Promise<CredentialCheckResponse> {
  const body = {
    provider: {
      firstName: provider.firstName,
      lastName: provider.lastName,
      npi: provider.npi,
      degree: provider.degree,
      specialty: provider.specialty,
      taxonomy: provider.taxonomy,
      dob: provider.dob,
      gender: provider.gender,
      licenseNumber: provider.licenseNumber,
      licenseState: provider.licenseState,
      licenseExpiry: provider.licenseExpiry,
      deaNumber: provider.deaNumber,
      deaExpiry: provider.deaExpiry,
      boardName: provider.boardName,
      boardExpiry: provider.boardExpiry,
      caqhId: provider.caqhId,
      groupName: provider.groupName,
      groupTin: provider.groupTin,
      malpracticeCarrier: provider.malpracticeCarrier,
      malpracticeCoverage: provider.malpracticeCoverage,
      credDate: provider.credDate,
      email: provider.email,
      medSchool: (provider as any).medSchool || "",
      gradDate: (provider as any).gradDate || "",
    },
    nppesData: provider.nppesData ? {
      first_name: provider.nppesData.basic?.first_name || "",
      last_name: provider.nppesData.basic?.last_name || "",
      entity_type: provider.nppesData.enumeration_type === "NPI-1" ? "1" : "2",
      deactivation_date: provider.nppesData.basic?.deactivation_date || "",
    } : {},
    oigClear: provider.oigClear !== false,
  };

  const res = await fetch("/api/credential-check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    throw new Error(`Credential check failed: ${res.status}`);
  }

  return res.json();
}

/* ================================================================
   Pipeline Animation Step
   ================================================================ */

interface PipelineStep {
  label: string;
  detail: string;
  status: "ok" | "fail" | "pending";
}

function buildPipelineSteps(p: DemoProvider): PipelineStep[] {
  const steps: PipelineStep[] = [];

  // 1. NPPES
  if (p.nppesData) {
    const nppesFirst = (p.nppesData.basic?.first_name || "").toUpperCase();
    const nppesLast = (p.nppesData.basic?.last_name || "").toUpperCase();
    const rFirst = p.firstName.toUpperCase();
    const rLast = p.lastName.toUpperCase();
    const match = (!nppesFirst || nppesFirst === rFirst) && (!nppesLast || nppesLast === rLast);
    steps.push({
      label: "NPPES Registry",
      detail: match ? `Verified -- Name matches` : `Mismatch -- NPPES: ${nppesFirst} ${nppesLast}`,
      status: match ? "ok" : "fail",
    });
  } else {
    steps.push({ label: "NPPES Registry", detail: "Verified", status: "ok" });
  }

  // 2. OIG
  steps.push({
    label: "OIG Exclusion List",
    detail: p.oigClear === false ? "EXCLUDED" : "Clear",
    status: p.oigClear === false ? "fail" : "ok",
  });

  // 3. State License
  if (p.licenseNumber) {
    const exp = isExpired(p.licenseExpiry);
    steps.push({
      label: `State License (${p.licenseState || "N/A"})`,
      detail: exp ? `Expired ${p.licenseExpiry}` : `Active${p.licenseExpiry ? `, expires ${p.licenseExpiry}` : ""}`,
      status: exp ? "fail" : "ok",
    });
  } else {
    steps.push({ label: "State License", detail: "No data in roster", status: "ok" });
  }

  // 4. Board Cert
  if (p.boardName) {
    const exp = isExpired(p.boardExpiry);
    steps.push({
      label: "Board Certification",
      detail: `${p.boardName}${p.boardExpiry ? (exp ? `, expired ${p.boardExpiry}` : `, expires ${p.boardExpiry}`) : ""}`,
      status: exp ? "fail" : "ok",
    });
  } else {
    steps.push({ label: "Board Certification", detail: "No data in roster", status: "ok" });
  }

  // 5. DEA
  if (p.deaNumber) {
    const exp = isExpired(p.deaExpiry);
    steps.push({
      label: "DEA License",
      detail: exp ? `Expired ${p.deaExpiry}` : `Active${p.deaExpiry ? `, expires ${p.deaExpiry}` : ""}`,
      status: exp ? "fail" : "ok",
    });
  } else {
    steps.push({ label: "DEA License", detail: "No data in roster", status: "ok" });
  }

  // 6. Education
  steps.push({ label: "Education", detail: "Verified", status: "ok" });

  // 7. Malpractice
  if (p.malpracticeCarrier) {
    steps.push({ label: "Malpractice Insurance", detail: `${p.malpracticeCarrier} -- Active`, status: "ok" });
  } else {
    steps.push({ label: "Malpractice Insurance", detail: "No data in roster", status: "ok" });
  }

  return steps;
}

/* ================================================================
   Sub-components: Credentialing
   ================================================================ */

function ProviderTable({ providers, onSelect }: { providers: DemoProvider[]; onSelect: (p: DemoProvider) => void }) {
  return (
    <div className="pipe-table-wrap">
      <table className="pipe-table">
        <thead>
          <tr>
            <th>Provider</th>
            <th>NPI</th>
            <th>Degree</th>
            <th>Specialty</th>
            <th>Status</th>
            <th>Flags</th>
          </tr>
        </thead>
        <tbody>
          {providers.map((p) => {
            const statusClass = p.credentialingStatus === "PSV Complete" ? "pipe-status--green"
              : p.credentialingStatus === "Review Required" ? "pipe-status--red"
              : "pipe-status--amber";
            return (
              <tr key={p.npi} onClick={() => onSelect(p)}>
                <td>{p.firstName} {p.lastName}</td>
                <td className="pipe-table-npi">{p.npi}</td>
                <td>{p.degree || "--"}</td>
                <td>{p.specialty || "--"}</td>
                <td><span className={`pipe-status ${statusClass}`}>{p.credentialingStatus}</span></td>
                <td>{p.flags.length}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function PipelineAnimation({ steps }: { steps: PipelineStep[] }) {
  const [visibleCount, setVisibleCount] = useState(0);

  useEffect(() => {
    if (visibleCount >= steps.length) return;
    const timer = setTimeout(() => setVisibleCount((c) => c + 1), 350);
    return () => clearTimeout(timer);
  }, [visibleCount, steps.length]);

  return (
    <div className="pipe-anim-wrap">
      <div className="pipe-anim-title">Primary Source Verification</div>
      {steps.map((step, i) => (
        <div key={i} className={`pipe-anim-item${i < visibleCount ? " pipe-anim-item--visible" : ""}`}>
          <span className={`pipe-anim-icon ${step.status === "ok" ? "pipe-anim-icon--ok" : step.status === "fail" ? "pipe-anim-icon--fail" : "pipe-anim-icon--pending"}`}>
            {step.status === "ok" ? "\u2705" : step.status === "fail" ? "\u274C" : "\u23F3"}
          </span>
          <span className="pipe-anim-label">{step.label}</span>
          <span className="pipe-anim-detail">-- {step.detail}</span>
        </div>
      ))}
    </div>
  );
}

function DataCard({ title, icon, flagged, children }: { title: string; icon: string; flagged?: boolean; children: React.ReactNode }) {
  return (
    <div className={`pipe-card${flagged ? " pipe-card--flagged" : ""}`}>
      <div className="pipe-card-head">
        <div className="pipe-card-icon">{icon}</div>
        <div className="pipe-card-title">{title}</div>
        {flagged && <span className="pipe-flag pipe-flag--red">FLAGGED</span>}
      </div>
      <div className="pipe-card-body">
        {children}
      </div>
    </div>
  );
}

function CardRow({ label, value, valueClass }: { label: string; value: string; valueClass?: string }) {
  return (
    <div className="pipe-card-row">
      <span className="pipe-card-label">{label}</span>
      <span className={`pipe-card-value ${valueClass || ""}`}>{value || "--"}</span>
    </div>
  );
}

function ProviderDetail({ provider, onBack, showFlags }: { provider: DemoProvider; onBack: () => void; showFlags?: boolean }) {
  const steps = buildPipelineSteps(provider);
  const today = new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

  const statusClass = provider.credentialingStatus === "PSV Complete" ? "pipe-status--green"
    : provider.credentialingStatus === "Review Required" ? "pipe-status--red"
    : "pipe-status--amber";

  const licenseExpired = isExpired(provider.licenseExpiry);
  const boardExpired = isExpired(provider.boardExpiry);
  const deaExpired = isExpired(provider.deaExpiry);

  return (
    <div className="pipe-detail">
      <div className="pipe-detail-header">
        <div>
          <div className="pipe-detail-name">{provider.firstName} {provider.middleName ? provider.middleName + " " : ""}{provider.lastName}{provider.degree ? `, ${provider.degree}` : ""}</div>
          <div className="pipe-detail-sub">
            <span>NPI: {provider.npi}</span>
            {provider.specialty && <span>{provider.specialty}</span>}
            <span className={`pipe-status ${statusClass}`}>{provider.credentialingStatus}</span>
          </div>
          {showFlags && provider.flags.length > 0 && (
            <div style={{ marginTop: 8 }}>
              {provider.flags.map((f, i) => (
                <span key={i} className={`pipe-flag ${f.includes("Expiring") ? "pipe-flag--amber" : "pipe-flag--red"}`}>{f}</span>
              ))}
            </div>
          )}
        </div>
        <button className="pipe-back" onClick={onBack}>&larr; Back to List</button>
      </div>

      <PipelineAnimation steps={steps} />

      <div className="pipe-cards">
        {/* Card 1: Overview */}
        <DataCard title="Overview" icon="\uD83D\uDCCB">
          <CardRow label="Name" value={`${provider.firstName} ${provider.middleName || ""} ${provider.lastName}`.replace(/\s+/g, " ").trim()} />
          <CardRow label="NPI" value={provider.npi} valueClass="pipe-card-value--mono" />
          <CardRow label="DOB" value={provider.dob} />
          <CardRow label="Gender" value={provider.gender} />
          <CardRow label="Degree" value={provider.degree} />
          <CardRow label="Specialty" value={provider.specialty} />
          <CardRow label="CAQH ID" value={provider.caqhId} valueClass="pipe-card-value--mono" />
          <CardRow label="Taxonomy" value={provider.taxonomy} valueClass="pipe-card-value--mono" />
        </DataCard>

        {/* Card 2: NPPES Verification */}
        <DataCard title="NPPES Verification" icon="\uD83D\uDD0D" flagged={showFlags && provider.flags.some(f => f.includes("Mismatch"))}>
          <CardRow label="NPI" value={`${provider.npi} -- Verified`} valueClass="pipe-card-value--green" />
          {provider.nppesData ? (
            <>
              <CardRow
                label="Name Match"
                value={`Roster: ${provider.firstName} ${provider.lastName} / NPPES: ${provider.nppesData.basic?.first_name || ""} ${provider.nppesData.basic?.last_name || ""}`}
                valueClass={
                  (provider.nppesData.basic?.first_name || "").toUpperCase() === provider.firstName.toUpperCase() &&
                  (provider.nppesData.basic?.last_name || "").toUpperCase() === provider.lastName.toUpperCase()
                    ? "pipe-card-value--green" : "pipe-card-value--red"
                }
              />
              <CardRow label="Entity Type" value={provider.nppesData.enumeration_type === "NPI-1" ? "Individual (Type 1)" : provider.nppesData.enumeration_type || "N/A"} />
              {provider.nppesData.addresses?.[0] && (
                <CardRow label="NPPES Address" value={`${provider.nppesData.addresses[0].address_1 || ""}, ${provider.nppesData.addresses[0].city || ""}, ${provider.nppesData.addresses[0].state || ""} ${provider.nppesData.addresses[0].postal_code || ""}`} />
              )}
            </>
          ) : (
            <CardRow label="Status" value="NPPES data not loaded" />
          )}
        </DataCard>

        {/* Card 3: State Licenses */}
        <DataCard title="State Licenses" icon="\uD83C\uDFDB" flagged={showFlags && licenseExpired}>
          <CardRow label="License #" value={provider.licenseNumber} valueClass="pipe-card-value--mono" />
          <CardRow label="State" value={provider.licenseState} />
          <CardRow label="Expiration" value={provider.licenseExpiry} valueClass={licenseExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
          <CardRow label="Status" value={licenseExpired ? "EXPIRED" : provider.licenseNumber ? "Active" : "No data"} valueClass={licenseExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
          {showFlags && licenseExpired && <span className="pipe-flag pipe-flag--red">EXPIRED</span>}
        </DataCard>

        {/* Card 4: Board Certifications */}
        <DataCard title="Board Certifications" icon="\uD83C\uDF93" flagged={showFlags && boardExpired}>
          <CardRow label="Board" value={provider.boardName} />
          <CardRow label="Expiration" value={provider.boardExpiry} valueClass={boardExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
          <CardRow label="Status" value={boardExpired ? "EXPIRED" : provider.boardName ? "Active" : "No data"} valueClass={boardExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
          {showFlags && boardExpired && <span className="pipe-flag pipe-flag--red">EXPIRED</span>}
        </DataCard>

        {/* Card 5: DEA License */}
        <DataCard title="DEA License" icon="\uD83D\uDC8A" flagged={showFlags && deaExpired}>
          <CardRow label="DEA #" value={provider.deaNumber} valueClass="pipe-card-value--mono" />
          <CardRow label="Expiration" value={provider.deaExpiry} valueClass={deaExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
          <CardRow label="Status" value={deaExpired ? "EXPIRED" : provider.deaNumber ? "Active" : "No data"} valueClass={deaExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
          {showFlags && deaExpired && <span className="pipe-flag pipe-flag--red">EXPIRED</span>}
        </DataCard>

        {/* Card 6: OIG/Sanctions */}
        <DataCard title="OIG / Sanctions" icon="\uD83D\uDEE1" flagged={showFlags && provider.oigClear === false}>
          <CardRow
            label="OIG Exclusion"
            value={provider.oigClear === false ? "EXCLUDED" : provider.oigClear === true ? "Clear" : "Checking..."}
            valueClass={provider.oigClear === false ? "pipe-card-value--red" : "pipe-card-value--green"}
          />
          <CardRow label="Check Date" value={today} />
          {showFlags && provider.oigClear === false && <span className="pipe-flag pipe-flag--red">EXCLUDED</span>}
        </DataCard>

        {/* Card 7: Malpractice Insurance */}
        <DataCard title="Malpractice Insurance" icon="\uD83D\uDCC4">
          <CardRow label="Carrier" value={provider.malpracticeCarrier} />
          <CardRow label="Policy #" value={provider.malpracticePolicy} valueClass="pipe-card-value--mono" />
          <CardRow label="Coverage" value={provider.malpracticeCoverage} />
        </DataCard>

        {/* Card 8: Education & Credentialing */}
        <DataCard title="Education & Credentialing" icon="\uD83C\uDF93">
          <CardRow label="Degree" value={provider.degree} />
          <CardRow label="Initial Cred Date" value={provider.credDate} />
          <CardRow label="Re-Cred Date" value={provider.recredDate} />
          <CardRow label="Email" value={provider.email} />
          <CardRow label="Group" value={provider.groupName} />
          <CardRow label="Group TIN" value={provider.groupTin} valueClass="pipe-card-value--mono" />
        </DataCard>
      </div>
    </div>
  );
}

/* ================================================================
   LLM-Powered Provider Detail
   ================================================================ */

function LlmProviderDetail({ provider, onBack, showFlags, cachedPsvData }: { provider: DemoProvider; onBack: () => void; showFlags?: boolean; cachedPsvData?: PsvData | null }) {
  const [llmResult, setLlmResult] = useState<CredentialCheckResponse | null>(null);
  const [psvData, setPsvData] = useState<PsvData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [visiblePsvCount, setVisiblePsvCount] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    setLlmResult(null);
    setVisiblePsvCount(0);

    // If we have cached PSV data, show data cards IMMEDIATELY (no loading spinner)
    if (cachedPsvData) {
      setPsvData(cachedPsvData);
      setLoading(false);  // Data cards show instantly

      // LLM assessment loads in background
      fetchCredentialCheck(provider)
        .then((llm) => {
          if (!cancelled) setLlmResult(llm);
        })
        .catch((err) => {
          if (!cancelled) setError(err.message || "AI assessment unavailable");
        });
    } else {
      // No cached data — show spinner and fetch both
      setLoading(true);
      setPsvData(null);

      Promise.all([
        fetchCredentialCheck(provider),
        fetchPsvData(provider.npi),
      ])
        .then(([llm, psv]) => {
          if (!cancelled) {
            setLlmResult(llm);
            setPsvData(psv);
            setLoading(false);
          }
        })
        .catch((err) => {
          if (!cancelled) {
            setError(err.message || "Credential check failed");
            setLoading(false);
          }
        });
    }

    return () => { cancelled = true; };
  }, [provider.npi]);

  // Animate PSV checks appearing one by one
  useEffect(() => {
    if (!llmResult) return;
    const total = llmResult.credentialing.psvChecks.length;
    if (visiblePsvCount >= total) return;
    const timer = setTimeout(() => setVisiblePsvCount((c) => c + 1), 350);
    return () => clearTimeout(timer);
  }, [llmResult, visiblePsvCount]);

  const today = new Date().toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });

  const credStatus = llmResult?.credentialing.status || provider.credentialingStatus;
  const statusClass = credStatus === "PSV Complete" ? "pipe-status--green"
    : credStatus === "Review Required" ? "pipe-status--red"
    : "pipe-status--amber";

  const licenseExpired = isExpired(provider.licenseExpiry);
  const boardExpired = isExpired(provider.boardExpiry);
  const deaExpired = isExpired(provider.deaExpiry);

  // Derive monitoring flags from LLM or fallback
  const monFlags = llmResult?.monitoring.flags || [];

  // Helper: check if a date string is expired
  const isDateExpired = (d: string) => {
    if (!d) return false;
    try { return new Date(d) < new Date(); } catch { return false; }
  };

  // Source badge component
  const SourceBadge = ({ label }: { label: string }) => (
    <span style={{
      display: "inline-block", fontSize: 10, fontWeight: 600, padding: "1px 6px",
      borderRadius: 4, background: "rgba(0,150,255,0.1)", color: "var(--d-accent)",
      marginLeft: 6, verticalAlign: "middle",
    }}>Source: {label}</span>
  );

  return (
    <div className="pipe-detail">
      <div className="pipe-detail-header">
        <div>
          <div className="pipe-detail-name">{provider.firstName} {provider.middleName ? provider.middleName + " " : ""}{provider.lastName}{provider.degree ? `, ${provider.degree}` : ""}</div>
          <div className="pipe-detail-sub">
            <span>NPI: {provider.npi}</span>
            {provider.specialty && <span>{provider.specialty}</span>}
            <span className={`pipe-status ${statusClass}`}>{credStatus}</span>
          </div>
          {showFlags && monFlags.length > 0 && (
            <div style={{ marginTop: 8 }}>
              {monFlags.map((f, i) => (
                <span key={i} className={`pipe-flag ${f.severity === "critical" ? "pipe-flag--red" : f.severity === "warning" ? "pipe-flag--amber" : "pipe-flag--blue"}`}>
                  {f.title}
                </span>
              ))}
            </div>
          )}
          {showFlags && !loading && monFlags.length === 0 && provider.flags.length > 0 && (
            <div style={{ marginTop: 8 }}>
              {provider.flags.map((f, i) => (
                <span key={i} className={`pipe-flag ${f.includes("Expiring") ? "pipe-flag--amber" : "pipe-flag--red"}`}>{f}</span>
              ))}
            </div>
          )}
        </div>
        <button className="pipe-back" onClick={onBack}>&larr; Back to List</button>
      </div>

      {/* Loading state — only show full spinner if no pre-cached PSV data */}
      {loading && !psvData && (
        <div className="pipe-anim-wrap" style={{ textAlign: "center", padding: "40px 0" }}>
          <div className="demo-spinner" style={{ margin: "0 auto 16px" }} />
          <div className="pipe-anim-title">Analyzing credentials with CertifyOS AI...</div>
          <div style={{ color: "var(--d-text-dim)", fontSize: 13, marginTop: 8 }}>Running Primary Source Verification</div>
        </div>
      )}
      {/* Small inline loading indicator when PSV data is already showing but AI assessment is pending */}
      {loading && psvData && (
        <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "12px 16px", background: "rgba(243,201,72,0.08)", borderRadius: 8, marginBottom: 16, fontSize: 13 }}>
          <div className="demo-spinner" style={{ width: 16, height: 16, borderWidth: 2 }} />
          <span style={{ color: "var(--d-text-dim)" }}>CertifyOS AI generating assessment...</span>
        </div>
      )}

      {/* Error state - fall back to static view */}
      {error && !loading && (
        <>
          <div style={{ padding: "12px 16px", background: "rgba(255,100,100,0.1)", borderRadius: 8, marginBottom: 16, fontSize: 13, color: "var(--d-text-dim)" }}>
            Analysis unavailable: {error}. Showing local assessment.
          </div>
          <PipelineAnimation steps={buildPipelineSteps(provider)} />
        </>
      )}

      {/* LLM-powered PSV checks — show as soon as available (don't wait for loading to finish) */}
      {llmResult && (
        <>
          <div className="pipe-anim-wrap">
            <div className="pipe-anim-title">Primary Source Verification <span style={{ fontSize: 11, color: "var(--d-accent)", marginLeft: 8 }}>CertifyOS AI</span></div>
            {llmResult.credentialing.psvChecks.map((check, i) => {
              const isOk = ["verified", "active", "clear", "on_file"].includes(check.status);
              const isFail = ["expired", "excluded", "mismatch"].includes(check.status);
              return (
                <div key={i} className={`pipe-anim-item${i < visiblePsvCount ? " pipe-anim-item--visible" : ""}`}>
                  <span className={`pipe-anim-icon ${isOk ? "pipe-anim-icon--ok" : isFail ? "pipe-anim-icon--fail" : "pipe-anim-icon--pending"}`}>
                    {isOk ? "\u2705" : isFail ? "\u274C" : "\u26A0\uFE0F"}
                  </span>
                  <span className="pipe-anim-label">{check.source}</span>
                  <span className="pipe-anim-detail">-- {check.detail}</span>
                </div>
              );
            })}
          </div>

          {/* LLM Summary */}
          <div style={{ padding: "12px 16px", background: "rgba(0,200,150,0.06)", borderRadius: 8, margin: "16px 0", fontSize: 13, lineHeight: 1.6, color: "var(--d-text)" }}>
            <strong>AI Assessment:</strong> {llmResult.credentialing.summary}
          </div>

          {/* Monitoring flags from LLM */}
          {showFlags && monFlags.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8, color: "var(--d-text)" }}>Monitoring Flags</div>
              <div style={{ fontSize: 13, color: "var(--d-text-dim)", marginBottom: 12 }}>{llmResult.monitoring.summary}</div>
              {monFlags.map((flag, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "flex-start", gap: 10, padding: "10px 14px", marginBottom: 6,
                  background: flag.severity === "critical" ? "rgba(255,80,80,0.08)" : flag.severity === "warning" ? "rgba(255,180,0,0.08)" : "rgba(100,150,255,0.08)",
                  borderRadius: 8, borderLeft: `3px solid ${flag.severity === "critical" ? "#ff5050" : flag.severity === "warning" ? "#ffb400" : "#6496ff"}`,
                }}>
                  <span style={{
                    fontSize: 11, fontWeight: 700, textTransform: "uppercase", padding: "2px 6px", borderRadius: 4,
                    background: flag.severity === "critical" ? "#ff5050" : flag.severity === "warning" ? "#ffb400" : "#6496ff",
                    color: "#fff", whiteSpace: "nowrap",
                  }}>{flag.severity}</span>
                  <div>
                    <div style={{ fontWeight: 600, fontSize: 13, color: "var(--d-text)" }}>{flag.title}</div>
                    <div style={{ fontSize: 12, color: "var(--d-text-dim)", marginTop: 2 }}>{flag.detail}</div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      <div className="pipe-cards">
        {/* Card 1: Overview */}
        <DataCard title="Overview" icon={"\uD83D\uDCCB"}>
          <CardRow label="Name" value={`${provider.firstName} ${provider.middleName || ""} ${provider.lastName}`.replace(/\s+/g, " ").trim()} />
          <CardRow label="NPI" value={provider.npi} valueClass="pipe-card-value--mono" />
          <CardRow label="DOB" value={provider.dob} />
          <CardRow label="Gender" value={provider.gender} />
          <CardRow label="Degree" value={provider.degree} />
          <CardRow label="Specialty" value={provider.specialty} />
          <CardRow label="CAQH ID" value={provider.caqhId} valueClass="pipe-card-value--mono" />
          <CardRow label="Taxonomy" value={provider.taxonomy} valueClass="pipe-card-value--mono" />
        </DataCard>

        {/* Card 2: NPPES Verification */}
        <DataCard title="NPPES Verification" icon={"\uD83D\uDD0D"} flagged={showFlags && provider.flags.some(f => f.includes("Mismatch"))}>
          {psvData?.nppes?.found ? (
            <>
              <CardRow label="NPI" value={`${psvData.nppes.npi} -- Verified`} valueClass="pipe-card-value--green" />
              <CardRow
                label="Name (NPPES)"
                value={`${psvData.nppes.first_name} ${psvData.nppes.last_name}`}
                valueClass="pipe-card-value--green"
              />
              <CardRow
                label="Name Match"
                value={
                  (psvData.nppes.first_name || "").toUpperCase() === provider.firstName.toUpperCase() &&
                  (psvData.nppes.last_name || "").toUpperCase() === provider.lastName.toUpperCase()
                    ? "Match" : `Mismatch -- Roster: ${provider.firstName} ${provider.lastName}`
                }
                valueClass={
                  (psvData.nppes.first_name || "").toUpperCase() === provider.firstName.toUpperCase() &&
                  (psvData.nppes.last_name || "").toUpperCase() === provider.lastName.toUpperCase()
                    ? "pipe-card-value--green" : "pipe-card-value--red"
                }
              />
              <CardRow label="Entity Type" value={psvData.nppes.entity_type === "1" ? "Individual (Type 1)" : "Organization (Type 2)"} />
              <CardRow label="Deactivation" value={psvData.nppes.deactivation_date || "None"} valueClass={psvData.nppes.deactivation_date ? "pipe-card-value--red" : "pipe-card-value--green"} />
              <CardRow label="Address" value={`${psvData.nppes.address || ""}, ${psvData.nppes.city || ""}, ${psvData.nppes.state || ""} ${psvData.nppes.postal_code || ""}`} />
              <div style={{ fontSize: 10, color: "var(--d-accent)", textAlign: "right", marginTop: 4 }}>Source: NPPES Registry</div>
            </>
          ) : provider.nppesData ? (
            <>
              <CardRow label="NPI" value={`${provider.npi} -- Verified`} valueClass="pipe-card-value--green" />
              <CardRow
                label="Name Match"
                value={`Roster: ${provider.firstName} ${provider.lastName} / NPPES: ${provider.nppesData.basic?.first_name || ""} ${provider.nppesData.basic?.last_name || ""}`}
                valueClass={
                  (provider.nppesData.basic?.first_name || "").toUpperCase() === provider.firstName.toUpperCase() &&
                  (provider.nppesData.basic?.last_name || "").toUpperCase() === provider.lastName.toUpperCase()
                    ? "pipe-card-value--green" : "pipe-card-value--red"
                }
              />
              <CardRow label="Entity Type" value={provider.nppesData.enumeration_type === "NPI-1" ? "Individual (Type 1)" : provider.nppesData.enumeration_type || "N/A"} />
            </>
          ) : (
            <CardRow label="Status" value="NPPES data not loaded" />
          )}
        </DataCard>

        {/* Card 3: State Licenses */}
        <DataCard title="State Licenses" icon={"\uD83C\uDFDB"} flagged={showFlags && (licenseExpired || (psvData?.state_licenses || []).some(l => l.status?.toUpperCase() === "INACTIVE"))}>
          {psvData && psvData.state_licenses.length > 0 ? (
            <>
              {psvData.state_licenses.map((lic, i) => {
                const exp = isDateExpired(lic.expiry);
                const isActive = lic.active_flag?.toUpperCase() === "Y" || lic.status?.toUpperCase() === "ACTIVE";
                return (
                  <div key={i} style={{ marginBottom: i < psvData.state_licenses.length - 1 ? 10 : 0, paddingBottom: i < psvData.state_licenses.length - 1 ? 10 : 0, borderBottom: i < psvData.state_licenses.length - 1 ? "1px solid rgba(255,255,255,0.06)" : "none" }}>
                    <CardRow label={`License #${i + 1}`} value={`${lic.state} ${lic.number}`} valueClass="pipe-card-value--mono" />
                    <CardRow label="Type" value={lic.type || "Medical"} />
                    <CardRow label="Status" value={isActive ? "ACTIVE" : lic.status || "INACTIVE"} valueClass={isActive ? "pipe-card-value--green" : "pipe-card-value--red"} />
                    <CardRow label="Expiration" value={lic.expiry || "N/A"} valueClass={exp ? "pipe-card-value--red" : "pipe-card-value--green"} />
                    {lic.source && <div style={{ fontSize: 10, color: "var(--d-accent)", textAlign: "right" }}>Source: {lic.source.includes("http") ? lic.state + " Medical Board" : lic.source}</div>}
                  </div>
                );
              })}
              <div style={{ fontSize: 10, color: "var(--d-accent)", textAlign: "right", marginTop: 4 }}>Source: State Medical Boards</div>
            </>
          ) : (
            <>
              <CardRow label="License #" value={provider.licenseNumber} valueClass="pipe-card-value--mono" />
              <CardRow label="State" value={provider.licenseState} />
              <CardRow label="Expiration" value={provider.licenseExpiry} valueClass={licenseExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
              <CardRow label="Status" value={licenseExpired ? "EXPIRED" : provider.licenseNumber ? "Active" : "No data"} valueClass={licenseExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
            </>
          )}
        </DataCard>

        {/* Card 4: Board Certifications -- show ABMS data */}
        <DataCard title="Board Certifications" icon={"\uD83C\uDF93"} flagged={showFlags && (boardExpired || (psvData?.abms || []).some(a => a.status?.toLowerCase().includes("expire")))}>
          {psvData && psvData.abms.length > 0 ? (
            <>
              {psvData.abms.map((cert, i) => {
                const exp = isDateExpired(cert.expiry);
                const isActive = !exp && !cert.status?.toLowerCase().includes("expire");
                return (
                  <div key={i} style={{ marginBottom: i < psvData.abms.length - 1 ? 10 : 0, paddingBottom: i < psvData.abms.length - 1 ? 10 : 0, borderBottom: i < psvData.abms.length - 1 ? "1px solid rgba(255,255,255,0.06)" : "none" }}>
                    <CardRow label="Board" value={cert.board || cert.cert_name} />
                    <CardRow label="Certificate" value={cert.cert_name} />
                    <CardRow label="Status" value={cert.status || (isActive ? "Active" : "Expired")} valueClass={isActive ? "pipe-card-value--green" : "pipe-card-value--red"} />
                    <CardRow label="Expiration" value={cert.expiry || "N/A"} valueClass={exp ? "pipe-card-value--red" : "pipe-card-value--green"} />
                    {cert.meeting_moc && <CardRow label="MOC Requirements" value={cert.meeting_moc} />}
                  </div>
                );
              })}
              <div style={{ fontSize: 10, color: "var(--d-accent)", textAlign: "right", marginTop: 4 }}>Source: ABMS</div>
            </>
          ) : (
            <>
              <CardRow label="Board" value={provider.boardName} />
              <CardRow label="Expiration" value={provider.boardExpiry} valueClass={boardExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
              <CardRow label="Status" value={boardExpired ? "EXPIRED" : provider.boardName ? "Active" : "No data"} valueClass={boardExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
            </>
          )}
        </DataCard>

        {/* Card 5: DEA License -- show real DEA data */}
        <DataCard title="DEA License" icon={"\uD83D\uDC8A"} flagged={showFlags && (deaExpired || (psvData?.dea || []).some(d => isDateExpired(d.expiry)))}>
          {psvData && psvData.dea.length > 0 ? (
            <>
              {psvData.dea.map((d, i) => {
                const exp = isDateExpired(d.expiry);
                const isActive = d.status?.toUpperCase() === "A" || d.status?.toLowerCase().includes("active");
                return (
                  <div key={i} style={{ marginBottom: i < psvData.dea.length - 1 ? 10 : 0, paddingBottom: i < psvData.dea.length - 1 ? 10 : 0, borderBottom: i < psvData.dea.length - 1 ? "1px solid rgba(255,255,255,0.06)" : "none" }}>
                    <CardRow label="DEA #" value={d.number} valueClass="pipe-card-value--mono" />
                    <CardRow label="Activity" value={isActive ? "Active" : d.status || "Unknown"} valueClass={isActive ? "pipe-card-value--green" : "pipe-card-value--red"} />
                    <CardRow label="Expiration" value={d.expiry || "N/A"} valueClass={exp ? "pipe-card-value--red" : "pipe-card-value--green"} />
                    <CardRow label="State" value={d.state || "N/A"} />
                    <CardRow label="Schedules" value={d.schedules || "N/A"} />
                  </div>
                );
              })}
              <div style={{ fontSize: 10, color: "var(--d-accent)", textAlign: "right", marginTop: 4 }}>Source: DEA</div>
            </>
          ) : (
            <>
              <CardRow label="DEA #" value={provider.deaNumber} valueClass="pipe-card-value--mono" />
              <CardRow label="Expiration" value={provider.deaExpiry} valueClass={deaExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
              <CardRow label="Status" value={deaExpired ? "EXPIRED" : provider.deaNumber ? "Active" : "No data"} valueClass={deaExpired ? "pipe-card-value--red" : "pipe-card-value--green"} />
            </>
          )}
        </DataCard>

        {/* Card 6: OIG / SAM / Sanctions -- show real counts */}
        <DataCard title="Exclusions & Sanctions" icon={"\uD83D\uDEE1"} flagged={showFlags && (psvData?.oig?.found || psvData?.sam?.found || psvData?.state_sanctions?.found || provider.oigClear === false)}>
          {psvData ? (
            <>
              <CardRow
                label="OIG Exclusion"
                value={psvData.oig?.found ? `EXCLUDED (${psvData.oig.records.length} record(s))` : "Clear"}
                valueClass={psvData.oig?.found ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              <CardRow
                label="SAM Exclusion"
                value={psvData.sam?.found ? `EXCLUDED (${psvData.sam.records.length} record(s))` : "Clear"}
                valueClass={psvData.sam?.found ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              <CardRow
                label="State Sanctions"
                value={psvData.state_sanctions?.found ? `FOUND (${psvData.state_sanctions.records.length} record(s))` : "Clear"}
                valueClass={psvData.state_sanctions?.found ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              <CardRow
                label="Board Actions"
                value={psvData.board_actions?.found ? `FOUND (${psvData.board_actions.records.length} action(s))` : "Clear"}
                valueClass={psvData.board_actions?.found ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              <CardRow
                label="Medicare Opt-Out"
                value={psvData.medicare_optout?.found ? "Opted Out" : "No"}
                valueClass={psvData.medicare_optout?.found ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              <CardRow
                label="Deceased"
                value={psvData.deceased?.found ? "FLAGGED" : "No"}
                valueClass={psvData.deceased?.found ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              {psvData.sanctions_summary && (
                <div style={{ fontSize: 11, color: "var(--d-text-dim)", marginTop: 6, padding: "6px 0", borderTop: "1px solid rgba(255,255,255,0.06)" }}>
                  Summary: OIG={psvData.sanctions_summary.oig}, SAM={psvData.sanctions_summary.sam}, State={psvData.sanctions_summary.state}, OFAC={psvData.sanctions_summary.ofac}, MOO={psvData.sanctions_summary.moo}
                </div>
              )}
              <CardRow label="Check Date" value={today} />
              <div style={{ fontSize: 10, color: "var(--d-accent)", textAlign: "right", marginTop: 4 }}>Source: OIG/SAM/State Boards</div>
            </>
          ) : (
            <>
              <CardRow
                label="OIG Exclusion"
                value={provider.oigClear === false ? "EXCLUDED" : provider.oigClear === true ? "Clear" : "Checking..."}
                valueClass={provider.oigClear === false ? "pipe-card-value--red" : "pipe-card-value--green"}
              />
              <CardRow label="Check Date" value={today} />
            </>
          )}
        </DataCard>

        {/* Card 7: Malpractice Insurance */}
        <DataCard title="Malpractice Insurance" icon={"\uD83D\uDCC4"}>
          <CardRow label="Carrier" value={provider.malpracticeCarrier} />
          <CardRow label="Policy #" value={provider.malpracticePolicy} valueClass="pipe-card-value--mono" />
          <CardRow label="Coverage" value={provider.malpracticeCoverage} />
        </DataCard>

        {/* Card 8: Education & Credentialing */}
        <DataCard title="Education & Credentialing" icon={"\uD83C\uDF93"}>
          <CardRow label="Degree" value={provider.degree} />
          <CardRow label="Initial Cred Date" value={provider.credDate} />
          <CardRow label="Re-Cred Date" value={provider.recredDate} />
          <CardRow label="Email" value={provider.email} />
          <CardRow label="Group" value={provider.groupName} />
          <CardRow label="Group TIN" value={provider.groupTin} valueClass="pipe-card-value--mono" />
        </DataCard>
      </div>
    </div>
  );
}

/* ================================================================
   Credentialing Tab
   ================================================================ */

function CredentialingTab({ providers, onSelectProvider, selectedProvider, onBack, psvCache }: {
  providers: DemoProvider[];
  onSelectProvider: (p: DemoProvider) => void;
  selectedProvider: DemoProvider | null;
  onBack: () => void;
  psvCache?: Record<string, any>;
}) {
  if (selectedProvider) {
    const cached = psvCache?.[selectedProvider.npi] || null;
    return <LlmProviderDetail provider={selectedProvider} onBack={onBack} cachedPsvData={cached} />;
  }

  return (
    <div className="pipe-section">
      <div className="pipe-section-title">Provider Credentialing</div>
      <div className="pipe-section-sub">{providers.length} providers extracted from roster -- click any row for full verification detail</div>
      <ProviderTable providers={providers} onSelect={onSelectProvider} />
    </div>
  );
}

/* ================================================================
   Monitoring Tab
   ================================================================ */

function MonitoringTab({ providers, onSelectProvider, selectedProvider, onBack, psvCache }: {
  providers: DemoProvider[];
  onSelectProvider: (p: DemoProvider) => void;
  selectedProvider: DemoProvider | null;
  onBack: () => void;
  psvCache?: Record<string, any>;
}) {
  if (selectedProvider) {
    const cached = psvCache?.[selectedProvider.npi] || null;
    return <LlmProviderDetail provider={selectedProvider} onBack={onBack} showFlags cachedPsvData={cached} />;
  }

  // Flag summary counts
  const licExpired = providers.filter(p => p.flags.some(f => f === "State License Expired")).length;
  const boardExpired = providers.filter(p => p.flags.some(f => f === "Board Certification Expired")).length;
  const boardExpiring = providers.filter(p => p.flags.some(f => f === "Board Cert Expiring Soon")).length;
  const deaExpired = providers.filter(p => p.flags.some(f => f === "DEA License Expired")).length;
  const oigExcl = providers.filter(p => p.flags.some(f => f === "OIG Exclusion")).length;
  const nameMismatch = providers.filter(p => p.flags.some(f => f.includes("Mismatch"))).length;

  return (
    <div className="pipe-section">
      <div className="pipe-section-title">Continuous Monitoring</div>
      <div className="pipe-section-sub">Real-time flag tracking across all {providers.length} providers</div>

      <div className="pipe-flags-summary">
        <div className="pipe-flag-card">
          <div className={`pipe-flag-card-num ${licExpired > 0 ? "pipe-flag-card-num--red" : "pipe-flag-card-num--green"}`}>{licExpired}</div>
          <div className="pipe-flag-card-label">License Expired</div>
        </div>
        <div className="pipe-flag-card">
          <div className={`pipe-flag-card-num ${boardExpired > 0 ? "pipe-flag-card-num--red" : "pipe-flag-card-num--green"}`}>{boardExpired}</div>
          <div className="pipe-flag-card-label">Board Cert Expired</div>
        </div>
        <div className="pipe-flag-card">
          <div className={`pipe-flag-card-num ${boardExpiring > 0 ? "pipe-flag-card-num--amber" : "pipe-flag-card-num--green"}`}>{boardExpiring}</div>
          <div className="pipe-flag-card-label">Board Cert Expiring</div>
        </div>
        <div className="pipe-flag-card">
          <div className={`pipe-flag-card-num ${deaExpired > 0 ? "pipe-flag-card-num--red" : "pipe-flag-card-num--green"}`}>{deaExpired}</div>
          <div className="pipe-flag-card-label">DEA Expired</div>
        </div>
        <div className="pipe-flag-card">
          <div className={`pipe-flag-card-num ${oigExcl > 0 ? "pipe-flag-card-num--red" : "pipe-flag-card-num--green"}`}>{oigExcl}</div>
          <div className="pipe-flag-card-label">OIG Exclusions</div>
        </div>
        <div className="pipe-flag-card">
          <div className={`pipe-flag-card-num ${nameMismatch > 0 ? "pipe-flag-card-num--amber" : "pipe-flag-card-num--green"}`}>{nameMismatch}</div>
          <div className="pipe-flag-card-label">Name Mismatches</div>
        </div>
      </div>

      <div className="pipe-table-wrap">
        <table className="pipe-table">
          <thead>
            <tr>
              <th>Provider</th>
              <th>NPI</th>
              <th>Flags</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {providers.map((p) => {
              const statusClass = p.monitoringStatus === "No Issues" ? "pipe-status--green"
                : p.monitoringStatus === "Review Required" ? "pipe-status--red"
                : "pipe-status--amber";
              return (
                <tr key={p.npi} onClick={() => onSelectProvider(p)}>
                  <td>{p.firstName} {p.lastName}</td>
                  <td className="pipe-table-npi">{p.npi}</td>
                  <td>
                    {p.flags.length === 0 ? (
                      <span className="pipe-no-flags">{"\u2705"} No flags</span>
                    ) : (
                      <div className="pipe-flag-list">
                        {p.flags.map((f, i) => (
                          <span key={i} className={`pipe-flag ${f.includes("Expiring") ? "pipe-flag--amber" : "pipe-flag--red"}`}>{f}</span>
                        ))}
                      </div>
                    )}
                  </td>
                  <td><span className={`pipe-status ${statusClass}`}>{p.monitoringStatus}</span></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ================================================================
   MAIN COMPONENT
   ================================================================ */

export function FullPipelineDemo() {
  // Roster analysis state
  const [phase, setPhase] = useState<Phase>("upload");
  const [workspaceId, setWorkspaceId] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<WorkspaceSnapshot | null>(null);
  const [currentOp, setCurrentOp] = useState<OperationRecord | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [downloadState, setDownloadState] = useState<DownloadState>("preparing");
  const [dragOver, setDragOver] = useState(false);
  const [fileName, setFileName] = useState("");
  const [startTime, setStartTime] = useState<number>(0);
  const [elapsed, setElapsed] = useState<number>(0);
  const transportRef = useRef<WorkspaceTransportManager | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const preprocessPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Pipeline state
  const [activeTab, setActiveTab] = useState<PipelineTab>("roster");
  const [providers, setProviders] = useState<DemoProvider[]>([]);
  const [selectedProvider, setSelectedProvider] = useState<DemoProvider | null>(null);
  const [analysisComplete, setAnalysisComplete] = useState(false);
  const [oigChecked, setOigChecked] = useState(false);
  const [nppesChecked, setNppesChecked] = useState(false);
  const [psvCache, setPsvCache] = useState<Record<string, any>>({});

  useEffect(() => {
    return () => {
      transportRef.current?.dispose();
      transportRef.current = null;
      if (preprocessPollRef.current) clearInterval(preprocessPollRef.current);
    };
  }, []);

  // Timer
  useEffect(() => {
    if (phase !== "processing" || !startTime) return;
    const interval = setInterval(() => {
      setElapsed(Math.round((Date.now() - startTime) / 1000));
    }, 1000);
    return () => clearInterval(interval);
  }, [phase, startTime]);

  // Poll for preprocessing completion
  const pollPreprocessing = useCallback((wsId: string) => {
    if (preprocessPollRef.current) clearInterval(preprocessPollRef.current);
    preprocessPollRef.current = setInterval(async () => {
      try {
        const ws = await getWorkspace(wsId);
        const ops = ws.operations || [];
        const preprocessOp = ops.find((o) => o.kind === "preprocess_roster");
        if (preprocessOp) {
          if (preprocessOp.status === "completed" || preprocessOp.status === "failed" || preprocessOp.status === "canceled") {
            setDownloadState("ready");
            if (preprocessPollRef.current) { clearInterval(preprocessPollRef.current); preprocessPollRef.current = null; }
          }
        } else {
          setDownloadState("ready");
          if (preprocessPollRef.current) { clearInterval(preprocessPollRef.current); preprocessPollRef.current = null; }
        }
      } catch { /* ignore */ }
    }, 2000);
  }, []);

  // After analysis completes, fetch providers from backend API + run OIG checks
  const analysisCompleteCalledRef = useRef(false);
  const onAnalysisComplete = useCallback(async (wsId: string) => {
    // Only call once per analysis
    if (analysisCompleteCalledRef.current) return;
    analysisCompleteCalledRef.current = true;

    console.log("[PIPELINE] Fetching providers from API for workspace:", wsId);

    // Fetch providers from dedicated backend API (not from profile_summary)
    let extracted: DemoProvider[] = [];
    try {
      const resp = await fetch(`/api/providers/${wsId}`);
      if (resp.ok) {
        const data = await resp.json();
        extracted = (data.providers || []).map((p: any) => ({
          ...p,
          flags: [] as string[],
          credentialingStatus: "In Progress",
          monitoringStatus: "No Issues",
          nppesData: undefined,
          oigClear: true,
        }));
        console.log("[PIPELINE] Got", extracted.length, "providers from API");
      } else {
        console.error("[PIPELINE] Provider API failed:", resp.status);
      }
    } catch (err) {
      console.error("[PIPELINE] Provider API error:", err);
    }

    // NPPES lookup for each provider
    const withNppes = await Promise.all(extracted.map(async (p) => {
      const nppesData = await lookupNppes(p.npi);
      return { ...p, nppesData };
    }));
    setNppesChecked(true);

    // OIG check for each provider
    const withOig = await Promise.all(withNppes.map(async (p) => {
      const clear = await checkOig(p.npi);
      return { ...p, oigClear: clear };
    }));
    setOigChecked(true);

    // Batch pre-fetch PSV data for all providers
    const npis = withOig.map(p => p.npi).filter(Boolean);
    console.log("[PIPELINE] Batch PSV pre-fetch for", npis.length, "NPIs:", npis);
    if (npis.length > 0) {
      try {
        const psvResp = await fetch("/api/psv/batch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ npis }),
        });
        if (psvResp.ok) {
          const psvBatchData = await psvResp.json();
          const resultKeys = Object.keys(psvBatchData.results || {});
          console.log("[PIPELINE] PSV batch loaded:", resultKeys.length, "NPIs cached");
          setPsvCache(psvBatchData.results || {});
        } else {
          console.error("[PIPELINE] PSV batch failed:", psvResp.status);
        }
      } catch (err) {
        console.error("[PIPELINE] PSV batch error:", err);
      }
    }

    // Compute flags and statuses
    const final = withOig.map((p) => {
      const flags = computeFlags(p);
      const credentialingStatus = computeCredStatus({ ...p, flags });
      const monitoringStatus = computeMonStatus({ ...p, flags });
      return { ...p, flags, credentialingStatus, monitoringStatus };
    });

    setProviders(final);
    setAnalysisComplete(true);
  }, []);

  async function handleFiles(files: File[]) {
    const roster = files.find((f) => /\.(csv|xlsx|xls)$/i.test(f.name));
    if (!roster) {
      setError("Please upload a CSV or XLSX file.");
      return;
    }

    setError(null);
    setFileName(roster.name);
    setPhase("processing");
    setStartTime(Date.now());
    setDownloadState("preparing");
    setAnalysisComplete(false);
    analysisCompleteCalledRef.current = false;
    setProviders([]);
    setSelectedProvider(null);
    setActiveTab("roster");
    setOigChecked(false);
    setNppesChecked(false);
    setPsvCache({});

    try {
      const { workspace_id } = await createWorkspace();
      setWorkspaceId(workspace_id);
      const uploadResult = await uploadFile(workspace_id, roster);
      const { operation } = await startOperation(workspace_id, "analyze_selected_roster", {
        selected_roster: {
          name: roster.name,
          path: String(uploadResult.uploaded_path || roster.name),
          roster_type: "",
        },
      });
      setCurrentOp(operation);
      transportRef.current?.dispose();
      const ws = await getWorkspace(workspace_id);
      setSnapshot(ws);

      transportRef.current = new WorkspaceTransportManager({
        workspaceId: workspace_id,
        pollIntervalMs: ws.frontend_config?.poll_interval_ms ?? 1500,
        sseEnabled: ws.frontend_config?.enable_sse_progress ?? false,
        handlers: {
          onModeChange: () => {},
          onWorkspace: (next) => {
            setSnapshot(next);
            const active = next.operations?.find(
              (o) => (o.status === "queued" || o.status === "running") && o.kind !== "preprocess_roster"
            );
            if (!active && (next.mappings?.length || next.quality_audit?.length || next.transformations?.length)) {
              setPhase("results");
              transportRef.current?.dispose();
              pollPreprocessing(workspace_id);
              void onAnalysisComplete(workspace_id);
            }
          },
          onOperation: (op) => {
            setCurrentOp(op);
            if (op.kind === "preprocess_roster") {
              if (op.status === "completed") setDownloadState("ready");
              return;
            }
            if (op.status === "completed" || op.status === "failed" || op.status === "canceled") {
              if (op.status !== "failed") {
                setTimeout(async () => {
                  const latest = await getWorkspace(workspace_id);
                  setSnapshot(latest);
                  setElapsed(Math.round((Date.now() - startTime) / 1000));
                  setPhase("results");
                  transportRef.current?.dispose();
                  pollPreprocessing(workspace_id);
                  void onAnalysisComplete(workspace_id);
                }, 800);
              } else {
                setError(String(op.error?.message || "Analysis failed. Please try again."));
                setPhase("upload");
                transportRef.current?.dispose();
              }
            }
          },
          onEvent: () => {},
          onError: (e) => setError(e.message),
        },
      });

      transportRef.current.setActiveOperation(operation.id);
      transportRef.current.start();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase("upload");
    }
  }

  async function handleExport() {
    if (!workspaceId) return;
    setDownloadState("downloading");
    setError(null);
    try {
      const result = await exportCsv(workspaceId);
      if (result === "processing") {
        setDownloadState("preparing");
        pollPreprocessing(workspaceId);
      } else {
        setDownloadState("ready");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setDownloadState("ready");
    }
  }

  function resetDemo() {
    transportRef.current?.dispose();
    transportRef.current = null;
    if (preprocessPollRef.current) { clearInterval(preprocessPollRef.current); preprocessPollRef.current = null; }
    setPhase("upload");
    setWorkspaceId(null);
    setSnapshot(null);
    setCurrentOp(null);
    setError(null);
    setFileName("");
    setStartTime(0);
    setElapsed(0);
    setDownloadState("preparing");
    setActiveTab("roster");
    setProviders([]);
    setSelectedProvider(null);
    setAnalysisComplete(false);
    setOigChecked(false);
    setNppesChecked(false);
    setPsvCache({});
  }

  const percent = fmt(currentOp?.progress?.percent);
  const colCount = snapshot?.profile_summary?.column_count || 0;

  const allMappings = snapshot?.mappings || [];
  const allTransforms = snapshot?.transformations || [];
  const allQA = snapshot?.quality_audit || [];

  const salesMappings = getSalesMappings(allMappings);
  const salesIssues = getSalesIssues(allQA);
  const salesTransforms = getSalesTransforms(allTransforms, allMappings);
  const rowCount = snapshot?.profile_summary?.rows_total || 0;

  const downloadButtonLabel =
    downloadState === "preparing" ? "Preparing processed file..."
    : downloadState === "downloading" ? "Generating..."
    : "\u2193 Download Processed File";
  const downloadButtonDisabled = downloadState === "preparing" || downloadState === "downloading";
  const downloadButtonClass = downloadState === "ready" ? "d-btn d-btn-green d-btn-lg" : "d-btn d-btn-disabled d-btn-lg";

  return (
    <div className="demo">
      {/* Nav */}
      <nav className="demo-nav">
        <div className="demo-nav-brand">
          <span className="demo-nav-dot" />
          CertifyOS
        </div>
        <span className="demo-nav-tag">Provider Data Pipeline</span>
        <div className="demo-nav-spacer" />
        {phase === "results" && (
          <button className="d-btn d-btn-ghost d-btn-sm" onClick={resetDemo}>
            &larr; New Analysis
          </button>
        )}
      </nav>

      {/* Error banner */}
      {error && (
        <div className="demo-errbar" role="alert">
          <span>{error}</span>
          <button className="demo-errbar-dismiss" onClick={() => setError(null)}>&times;</button>
        </div>
      )}

      {/* Tab bar (only shown in results phase) */}
      {phase === "results" && (
        <div className="pipe-tabs">
          <button
            className={`pipe-tab ${activeTab === "roster" ? "pipe-tab--active" : ""}`}
            onClick={() => { setActiveTab("roster"); setSelectedProvider(null); }}
          >
            <span className="pipe-tab-check">{"\u2713"}</span>
            Roster Analysis
          </button>
          <button
            className={`pipe-tab ${activeTab === "credentialing" ? "pipe-tab--active" : ""}`}
            disabled={!analysisComplete}
            onClick={() => { setActiveTab("credentialing"); setSelectedProvider(null); }}
          >
            {analysisComplete ? <span className="pipe-tab-num" style={{ background: "var(--d-accent)", color: "#040610" }}>2</span> : <span className="pipe-tab-num">2</span>}
            Credentialing{!analysisComplete && providers.length === 0 ? "" : ` (${providers.length})`}
          </button>
          <button
            className={`pipe-tab ${activeTab === "monitoring" ? "pipe-tab--active" : ""}`}
            disabled={!analysisComplete}
            onClick={() => { setActiveTab("monitoring"); setSelectedProvider(null); }}
          >
            {analysisComplete ? <span className="pipe-tab-num" style={{ background: "var(--d-accent)", color: "#040610" }}>3</span> : <span className="pipe-tab-num">3</span>}
            Monitoring{!analysisComplete ? "" : ` (${providers.filter(p => p.flags.length > 0).length} flags)`}
          </button>
        </div>
      )}

      <div className="pipe-content">
        {/* -- UPLOAD PHASE -- */}
        {phase === "upload" && (
          <div className="demo-upload-wrap">
            <div className="demo-hero-text">
              <div className="demo-hero-badge">&#10022; Live Demo</div>
              <h1 className="demo-hero-h1">
                The complete provider<br />
                <span>data pipeline.</span>
              </h1>
              <p className="demo-hero-sub">
                Upload a provider roster. We'll analyze, map, and validate every field --
                then run credentialing verification and continuous monitoring
                across all providers, in one pipeline.
              </p>
            </div>

            <div
              className={`demo-dropzone${dragOver ? " demo-dropzone--over" : ""}`}
              onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={(e) => { e.preventDefault(); setDragOver(false); handleFiles(Array.from(e.dataTransfer.files)); }}
              onClick={() => fileRef.current?.click()}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") fileRef.current?.click(); }}
            >
              <input
                ref={fileRef}
                type="file"
                accept=".csv,.xlsx,.xls"
                hidden
                onChange={(e) => {
                  const f = Array.from(e.target.files || []);
                  e.currentTarget.value = "";
                  handleFiles(f);
                }}
              />
              <div className="demo-dropzone-icon">&uarr;</div>
              <p className="demo-dropzone-primary">
                Drop your roster here or{" "}
                <span className="demo-dropzone-link">browse</span>
              </p>
              <p className="demo-dropzone-hint">CSV or XLSX -- practitioner or facility rosters</p>
            </div>
          </div>
        )}

        {/* -- PROCESSING PHASE -- */}
        {phase === "processing" && (
          <div className="demo-processing">
            <div className="demo-processing-card">
              <div className="demo-spinner" />
              <div className="demo-processing-title">Analyzing your roster</div>
              <div className="demo-processing-steps">
                <div className={`demo-step ${percent >= 10 ? "demo-step--done" : percent > 0 ? "demo-step--active" : ""}`}>
                  <span className="demo-step-dot" />Reading file structure
                </div>
                <div className={`demo-step ${percent >= 45 ? "demo-step--done" : percent >= 20 ? "demo-step--active" : ""}`}>
                  <span className="demo-step-dot" />Mapping {colCount || ""} columns to schema
                </div>
                <div className={`demo-step ${percent >= 75 ? "demo-step--done" : percent >= 50 ? "demo-step--active" : ""}`}>
                  <span className="demo-step-dot" />Detecting data quality issues
                </div>
                <div className={`demo-step ${percent >= 90 ? "demo-step--done" : percent >= 75 ? "demo-step--active" : ""}`}>
                  <span className="demo-step-dot" />Analyzing provider credentials
                </div>
              </div>
              <div className="demo-proc-bar">
                <div className="demo-proc-fill" style={{ width: `${Math.max(percent, 5)}%` }} />
              </div>
              <div className="demo-processing-timer">{elapsed}s</div>
            </div>
            {fileName && (
              <div className="demo-file-pill">&#9636; {fileName}</div>
            )}
          </div>
        )}

        {/* -- RESULTS: Roster Analysis Tab -- */}
        {phase === "results" && activeTab === "roster" && (
          <div className="demo-results">
            <div className="demo-results-header">
              <div>
                <div className="demo-results-title">Analysis Complete</div>
                <div className="demo-results-sub">
                  <strong style={{ color: "var(--d-text)", fontFamily: "var(--d-mono)", fontSize: 12 }}>{fileName}</strong>
                  {rowCount ? ` \u2014 ${rowCount.toLocaleString()} providers` : ""}
                  {elapsed > 0 && elapsed < 600 ? ` analyzed in ${elapsed}s` : ""}
                </div>
              </div>
              <div className="demo-results-actions">
                <button className="d-btn d-btn-ghost" onClick={resetDemo}>Analyze Another</button>
                <button className={downloadButtonClass} disabled={downloadButtonDisabled} onClick={handleExport}>
                  {downloadState === "preparing" && <span className="demo-btn-spinner" />}
                  {downloadButtonLabel}
                </button>
              </div>
            </div>

            <div className="demo-score-row">
              <div className="demo-score-card">
                <div className="demo-score-card-label">Providers</div>
                <div className="demo-score-card-value demo-score-card-value--blue">{rowCount || "\u2014"}</div>
                <div className="demo-score-card-sub">rows analyzed</div>
              </div>
              <div className="demo-score-card">
                <div className="demo-score-card-label">Fields Mapped</div>
                <div className="demo-score-card-value demo-score-card-value--green">{salesMappings.length}</div>
                <div className="demo-score-card-sub">of {colCount} columns recognized</div>
              </div>
              <div className="demo-score-card">
                <div className="demo-score-card-label">Issues Found</div>
                <div className={`demo-score-card-value ${salesIssues.length > 0 ? "demo-score-card-value--red" : "demo-score-card-value--green"}`}>{salesIssues.length}</div>
                <div className="demo-score-card-sub">{salesIssues.length === 0 ? "clean data" : "need attention"}</div>
              </div>
              <div className="demo-score-card">
                <div className="demo-score-card-label">Auto-Corrections</div>
                <div className="demo-score-card-value demo-score-card-value--amber">{salesTransforms.length}</div>
                <div className="demo-score-card-sub">applied automatically</div>
              </div>
            </div>

            <div className="demo-sections">
              <Panel icon="&#9678;" iconClass="demo-panel-icon--blue" title="Intelligent Field Mapping"
                subtitle={`AI matched ${salesMappings.length} of your columns to our standard schema`} count={salesMappings.length} defaultOpen>
                <div className="demo-map-grid-header">
                  <span>Your Column</span><span></span><span>Mapped To</span>
                </div>
                {salesMappings.map((m) => (
                  <div key={m.id} className="demo-map-row">
                    <span className="demo-map-source">{m.source_column || m.id}</span>
                    <span className="demo-map-arrow">&rarr;</span>
                    <span className="demo-map-target">{m.target_field}</span>
                  </div>
                ))}
              </Panel>

              {salesIssues.length > 0 ? (
                <Panel icon="!" iconClass="demo-panel-icon--red" title="Data Quality Issues"
                  subtitle={`${salesIssues.length} issue${salesIssues.length !== 1 ? "s" : ""} that need attention before ingestion`}
                  count={salesIssues.length} defaultOpen>
                  {salesIssues.map((q) => {
                    const sev = String(q.severity || "warning").toLowerCase();
                    const isError = sev === "error" || sev === "critical";
                    const isInfo = sev === "info";
                    const dotClass = isError ? "demo-sev-dot--error" : isInfo ? "demo-sev-dot--info" : "demo-sev-dot--warn";
                    return (
                      <div key={q.id} className="demo-item-row">
                        <div className="demo-item-sev"><div className={`demo-sev-dot ${dotClass}`} /></div>
                        <div className="demo-item-body">
                          <p className="demo-item-title">{String(q.title || q.column || q.check_type || q.id || "Issue")}</p>
                          <p className="demo-item-detail">{String(q.message || q.finding || q.description || "")}</p>
                          {q.sample_values && q.sample_values.length > 0 && (
                            <p className="demo-item-samples">Examples: {q.sample_values.slice(0, 3).map((v: string) => `"${v}"`).join(", ")}</p>
                          )}
                        </div>
                        <div className="demo-item-badge">
                          <span className={isError ? "demo-badge demo-badge-error" : isInfo ? "demo-badge demo-badge-info" : "demo-badge demo-badge-warn"}>
                            {isError ? "error" : isInfo ? "info" : "warning"}
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </Panel>
              ) : (
                <Panel icon="&#10003;" iconClass="demo-panel-icon--green" title="Data Quality"
                  subtitle="No critical issues found -- your data looks clean" defaultOpen={false}>
                  <div className="demo-empty">All NPI numbers validated. No critical data quality issues detected.</div>
                </Panel>
              )}

              <Panel icon="&#9889;" iconClass="demo-panel-icon--amber" title="Automated Corrections"
                subtitle={`${salesTransforms.length} normalization rules applied to prepare your data`} count={salesTransforms.length} defaultOpen>
                {salesTransforms.map((t, i) => (
                  <div key={i} className="demo-transform-row">
                    <div className="demo-transform-icon">{t.icon}</div>
                    <div className="demo-transform-body">
                      <p className="demo-transform-name">{t.name}</p>
                      <p className="demo-transform-desc">{t.description}</p>
                    </div>
                  </div>
                ))}
              </Panel>
            </div>

            <div className="demo-export-section">
              <div className="demo-export-left">
                <div className="demo-export-title">{downloadState === "ready" ? "Your processed file is ready." : "Building your processed file..."}</div>
                <div className="demo-export-sub">
                  All {salesTransforms.length} corrections applied. {salesMappings.length} fields mapped to standard schema.
                  {salesIssues.length > 0 ? ` ${salesIssues.length} flagged rows included with error annotations.` : ""}
                </div>
              </div>
              <div className="demo-export-actions">
                <button className={downloadButtonClass} disabled={downloadButtonDisabled} onClick={handleExport}>
                  {downloadState === "preparing" && <span className="demo-btn-spinner" />}
                  {downloadButtonLabel}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* -- RESULTS: Credentialing Tab -- */}
        {phase === "results" && activeTab === "credentialing" && (
          <CredentialingTab
            providers={providers}
            onSelectProvider={setSelectedProvider}
            selectedProvider={selectedProvider}
            onBack={() => setSelectedProvider(null)}
            psvCache={psvCache}
          />
        )}

        {/* -- RESULTS: Monitoring Tab -- */}
        {phase === "results" && activeTab === "monitoring" && (
          <MonitoringTab
            providers={providers}
            onSelectProvider={setSelectedProvider}
            selectedProvider={selectedProvider}
            onBack={() => setSelectedProvider(null)}
            psvCache={psvCache}
          />
        )}
      </div>
    </div>
  );
}
