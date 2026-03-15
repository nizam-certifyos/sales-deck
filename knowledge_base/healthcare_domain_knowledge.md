# COMPREHENSIVE HEALTHCARE CREDENTIALING, MONITORING, AND PAYOR ENROLLMENT KNOWLEDGE BASE

**Purpose:** Training reference for an AI model acting as an expert roster analyst.
**Compiled:** March 2026

---

# SECTION 1: HEALTHCARE IDENTIFIERS

## 1.1 NPI (National Provider Identifier)

### What It Is
The NPI is a unique, permanent, 10-digit numeric identifier assigned to healthcare providers in the United States under HIPAA. It replaced the prior system of multiple payer-specific identifiers. The NPI is mandatory for all HIPAA-covered transactions (claims, eligibility checks, referrals, prior authorizations).

### Issuing Body
- Centers for Medicare & Medicaid Services (CMS)
- Administered through the National Plan and Provider Enumeration System (NPPES)

### Types
- **Type 1 (Individual):** Assigned to individual providers (physicians, nurses, dentists, therapists, etc.). Sole proprietors get a Type 1 NPI. A Type 1 NPI stays with the individual for life, regardless of employer or location changes.
- **Type 2 (Organization):** Assigned to organizational providers (hospitals, group practices, clinics, home health agencies, nursing facilities, DME suppliers, etc.). Organizations can have subparts, each receiving its own NPI.

### Format and Structure
- Exactly 10 numeric digits (0-9 only, no letters or special characters)
- Positions 1-9: Assigned identifier
- Position 10: Check digit, calculated using the Luhn algorithm
- The NPI is prefixed with "80840" (the health industry number) for check digit calculation purposes, making it a 15-digit number (80840 + 9-digit identifier) used for the Luhn validation

### Luhn Check Digit Algorithm for NPI
1. Start with the 9-digit NPI identifier (positions 1-9), and prepend the constant "80840" to create a 14-digit number
2. Beginning from the rightmost digit of the 14-digit number, double every other digit (the rightmost, third from right, fifth from right, etc.)
3. If doubling results in a number greater than 9, subtract 9 (equivalent to summing the two digits of the product)
4. Sum all the digits (both doubled and non-doubled)
5. The check digit is the amount needed to bring the total to the next multiple of 10: check_digit = (10 - (sum mod 10)) mod 10
6. This check digit becomes position 10 of the NPI

**Example:** For NPI 123456789X where X is the check digit:
- Prepend 80840: 80840123456789
- Apply Luhn algorithm to determine X
- The valid NPI would be 1234567893 (example)

### NPPES Registry
- Free public lookup: https://npiregistry.cms.hhs.gov/
- API available for bulk queries
- Data includes: NPI number, provider name, type (1 or 2), taxonomy codes, practice address, mailing address, phone/fax, authorized official (for Type 2), enumeration date
- NPPES downloadable file available monthly (full replacement file)
- NPPES API endpoint: https://npiregistry.cms.hhs.gov/api/

### Deactivation and Reactivation
- NPIs can be deactivated by CMS for: death of provider, fraudulent application, provider request, organizational dissolution
- Deactivated NPIs are never reassigned to another provider
- A deactivated NPI can be reactivated by the provider (if alive) or the organization
- Using a deactivated NPI on claims will cause denials

### Common Data Quality Issues
- Confusing Type 1 and Type 2 (billing under individual NPI vs group NPI)
- NPI not updated with address changes in NPPES
- Deactivated NPIs still being used on rosters
- Sole proprietors sometimes incorrectly assigned Type 2
- Multiple NPIs for the same individual (should only have one Type 1)
- Taxonomy codes not matching actual specialty practiced
- NPI transposition errors (digits swapped)

---

## 1.2 TIN/EIN (Tax Identification Number / Employer Identification Number)

### What It Is
A TIN is a generic term for the number used for tax reporting. In healthcare, TIN usually refers to either:
- **EIN (Employer Identification Number):** For organizations and group practices
- **SSN (Social Security Number):** For individual/sole proprietors

### Issuing Body
- Internal Revenue Service (IRS)

### Format
- 9 digits in XX-XXXXXXX format (2 digits, hyphen, 7 digits)
- The first two digits (prefix) historically indicated the IRS campus that issued the EIN; since 2001 they indicate the Internet processing center

### EIN Prefix Ranges (Historical Campus Assignment)
- 01-06: Northeast
- 10-16: Southeast
- 20-27: Midwest
- 30-38: Southwest
- 40-48: West
- 50-68: Small Business/Self-Employed
- 71-77: Large Business
- 80-88: Tax-Exempt/Government
- 90-98: International

### Validation Rules
- Must be exactly 9 digits
- Cannot be all zeros: 000000000
- Cannot be all the same digit: 111111111, 222222222, etc.
- Cannot start with 00
- The IRS has never issued EINs starting with 07, 08, 09, 17, 18, 19, 28, 29, 49, 69, 70, 78, 79, 89
- Should not match known test/invalid TINs: 123456789, etc.

### Healthcare Context
- Group practices bill under a single TIN (EIN)
- Individual providers billing as sole proprietors may use SSN as TIN
- The TIN/NPI combination is critical for claims adjudication
- A single TIN can have multiple NPIs associated (group with many providers)
- A single NPI can be associated with multiple TINs (provider working at multiple groups)
- W-9 form is the standard mechanism for collecting TIN
- Payor contracts are typically tied to TIN

### Common Data Quality Issues
- TIN/EIN transposition with NPI (mixing up 9 vs 10 digits)
- Using individual SSN when group EIN should be used (or vice versa)
- Outdated TINs after practice ownership changes
- Mismatched TIN/NPI on claims causing denials
- Same provider showing under different TINs across rosters

---

## 1.3 SSN (Social Security Number)

### What It Is
A 9-digit number issued to U.S. citizens, permanent residents, and temporary working residents. Used as a national identification number for taxation and other purposes.

### Issuing Body
- Social Security Administration (SSA)

### Format: Area-Group-Serial (Historical)
- **Pre-June 2011 (Geographic Assignment):**
  - Positions 1-3: Area Number (geographic, based on ZIP code of application)
  - Positions 4-5: Group Number (issued in a specific order within each area)
  - Positions 6-9: Serial Number (sequential within each group)
  - Format: AAA-GG-SSSS

- **Post-June 2011 (Randomization):**
  - SSA implemented random assignment
  - Area numbers no longer geographically significant
  - Previously unissued area numbers may now be assigned (except 000, 666, 900-999)

### Invalid SSN Patterns
- Area number 000: Never issued
- Area number 666: Never issued
- Area numbers 900-999: Reserved for ITINs (Individual Taxpayer Identification Numbers) — format 9XX-XX-XXXX
- Group number 00: Never issued
- Serial number 0000: Never issued
- Known test/advertising SSNs: 078-05-1120 (Woolworth wallet), 219-09-9999
- All same digits: 111-11-1111, 222-22-2222, etc.

### ITIN (Individual Taxpayer Identification Number)
- Format: 9XX-XX-XXXX (always starts with 9)
- Issued by IRS for tax filing by individuals who cannot obtain an SSN
- Fourth and fifth digits historically in ranges 70-88, 90-92, 94-99; expanded in 2011

### Healthcare Context
- Used as TIN for sole proprietor providers
- Required on credentialing applications
- Should NEVER be transmitted on roster files (PHI/PII risk)
- Medicare historically used SSN as HICN (replaced by MBI)
- Many credentialing applications collect SSN for background check purposes

### Common Data Quality Issues
- SSN appearing in roster files (security violation)
- SSN used where EIN should be
- Invalid SSN patterns not caught at data entry
- SSN confused with ITIN

---

## 1.4 DEA (Drug Enforcement Administration) Number

### What It Is
A DEA registration number is issued to healthcare providers authorized to prescribe, dispense, or handle controlled substances. It serves as both an identifier and proof of authorization to handle controlled substances under the Controlled Substances Act.

### Issuing Body
- U.S. Drug Enforcement Administration (DOJ)

### Who Needs One
- Physicians (MD, DO)
- Dentists (DDS, DMD)
- Podiatrists (DPM)
- Veterinarians (DVM)
- Nurse Practitioners (NP/APRN) — in states where they have prescriptive authority
- Physician Assistants (PA) — with delegated authority
- Optometrists (OD) — in some states
- Hospitals and clinics (institutional registrations)
- Pharmacies
- Manufacturers and distributors of controlled substances
- Researchers using controlled substances

### Format
The DEA number consists of 2 letters followed by 7 digits: **AANNNNNNC**

**First Letter (Registrant Type):**
- **A** — Deprecated (formerly used for older registrations; some still active)
- **B** — Hospital/Clinic
- **C** — Practitioner (physician, dentist, veterinarian)
- **D** — Teaching institution
- **E** — Manufacturer
- **F** — Distributor
- **G** — Researcher
- **H** — Analytical lab
- **J** — Importer
- **K** — Exporter
- **L** — Reverse distributor
- **M** — Mid-level practitioner (NP, PA, CRNA, CNM, optometrist where authorized)
- **P** — Narcotic Treatment Program
- **R** — Narcotic Treatment Program (additional)
- **S** — Narcotic Treatment Program (additional)
- **T** — Narcotic Treatment Program (additional)
- **X** — Suboxone/Buprenorphine prescriber (DATA 2000 waiver; note: as of the Consolidated Appropriations Act 2023, the X-waiver requirement was eliminated)

**Second Letter:**
- First letter of the registrant's last name (for individuals)
- First letter of the business name (for organizations)

**Digits 1-6:** Assigned sequentially

**Digit 7 (Check Digit):** Calculated by algorithm

### DEA Check Digit Algorithm
1. Add the 1st, 3rd, and 5th digits: sum1 = d1 + d3 + d5
2. Add the 2nd, 4th, and 6th digits, then multiply by 2: sum2 = (d2 + d4 + d6) * 2
3. Add sum1 + sum2 = total
4. The check digit is the last digit (ones place) of the total

**Example:** DEA number AB1234563
- sum1 = 1 + 3 + 5 = 9
- sum2 = (2 + 4 + 6) * 2 = 24
- total = 9 + 24 = 33
- Check digit = 3 (last digit of 33) -- Valid

### Controlled Substance Schedules
- **Schedule I:** High abuse potential, no accepted medical use (heroin, LSD, marijuana at federal level, ecstasy)
- **Schedule II:** High abuse potential with accepted medical use (oxycodone, fentanyl, Adderall, Ritalin, morphine, methadone)
- **Schedule III:** Moderate abuse potential (Tylenol with codeine, ketamine, anabolic steroids, testosterone)
- **Schedule IV:** Low abuse potential (Xanax, Valium, Ambien, tramadol)
- **Schedule V:** Lowest abuse potential (cough syrups with codeine, Lyrica, Lomotil)

### Multiple Registrations
- Providers can hold multiple DEA numbers
- Separate registration required for each state of practice
- Separate registration for each principal place of business
- Hospital-based practitioners may practice under the hospital's DEA
- A provider can have separate registrations for different activities (practitioner + researcher)

### State-Specific Requirements
- Many states require their own Controlled Dangerous Substance (CDS) license in addition to federal DEA
- State CDS numbers are separate identifiers with state-specific formats
- Some states have additional prescribing restrictions beyond federal schedules

### Common Data Quality Issues
- Expired DEA registrations on roster files
- DEA number not matching provider name (second letter should match last name initial)
- Using institutional DEA when individual DEA is needed
- Missing DEA for prescribing providers
- X-waiver DEA numbers still being tracked separately post-2023 policy change
- Multiple DEA numbers creating confusion about which is current/primary

---

## 1.5 CAQH ProView ID

### What It Is
CAQH (Council for Affordable Quality Health Care) ProView is an online provider data collection platform that serves as a universal credentialing database. The CAQH ProView ID is a unique identifier assigned to each provider profile.

### Format
- 7-8 digit numeric identifier
- Assigned sequentially upon registration
- Sometimes called "CAQH Provider ID" or "CAQH Number"

### What Data It Contains
The CAQH ProView profile collects comprehensive provider information:
- **Personal Information:** Legal name, date of birth, SSN, gender, contact info
- **Practice Locations:** All addresses, phone numbers, office hours, accessibility, languages
- **Education:** Medical/professional school, graduation dates
- **Training:** Internship, residency, fellowship details
- **Licenses:** All state licenses with numbers, issue/expiration dates
- **Board Certifications:** Board name, specialty, certification dates
- **DEA Registration:** Numbers, states, expiration dates
- **Hospital Affiliations:** Privileges, admitting arrangements
- **Malpractice Insurance:** Carrier, policy numbers, coverage amounts, claims history
- **Work History:** Chronological employment history (5+ years typically)
- **Professional References**
- **Disclosure Questions:** Malpractice history, disciplinary actions, criminal history, substance abuse, physical/mental health, loss of privileges
- **Supporting Documents:** Uploaded copies of licenses, certifications, DEA certificates, malpractice face sheets, W-9, board certification certificates

### Attestation Cycle
- Providers must re-attest (review and confirm all information is current) every 120 days (approximately quarterly)
- Non-attestation results in the profile being marked as "not attested" — health plans cannot use non-attested data for credentialing
- Providers receive reminders before attestation deadline
- Full re-attestation requires reviewing all sections and confirming accuracy
- Supporting documents must be kept current (replacing expired licenses, etc.)

### Health Plan Usage
- Over 1,000 health plans and healthcare organizations participate
- Health plans use CAQH data for initial credentialing and recredentialing
- Replaces paper applications for most commercial payers
- Providers authorize specific organizations to view their data
- Health plans can request additional data elements beyond the standard CAQH dataset
- CAQH ProView is free for providers; health plans pay for access

### Common Data Quality Issues
- Lapsed attestation (over 120 days)
- Expired supporting documents
- Incomplete profiles blocking credentialing
- Provider not authorizing the correct health plans
- Addresses not matching between CAQH and roster submissions
- Multiple CAQH IDs for the same provider (duplicates)
- Missing or outdated malpractice insurance information

---

## 1.6 CLIA (Clinical Laboratory Improvement Amendments) Number

### What It Is
A CLIA certificate number identifies a laboratory that has been certified to perform testing on human specimens under the Clinical Laboratory Improvement Amendments of 1988. Any facility performing even basic lab tests (including physician office labs performing waived tests) must have a CLIA certificate.

### Issuing Body
- Centers for Medicare & Medicaid Services (CMS)
- State Survey Agencies administer the program

### Certificate Types
1. **Certificate of Waiver (CoW):** For labs performing only waived tests (simple, low-risk tests like rapid strep, urine dipstick, glucose monitoring). Most common type (~75% of CLIA labs).
2. **Certificate of Provider-Performed Microscopy Procedures (PPMP):** For moderate complexity microscopy tests performed by physicians during patient visits (wet preps, KOH preps, fern tests).
3. **Certificate of Registration:** Temporary certificate allowing lab to begin testing while applying for Certificate of Compliance or Accreditation.
4. **Certificate of Compliance:** For labs performing moderate and/or high complexity testing, inspected by state agencies under CMS.
5. **Certificate of Accreditation:** For labs performing moderate and/or high complexity testing, inspected by CMS-approved accrediting organizations (CAP, COLA, Joint Commission, AABB, ASHI, A2LA).

### Format
- 10-digit alphanumeric identifier
- Format: SSXNNNNNN (varies slightly)
- First 2 characters often indicate the state
- The remaining characters are sequentially assigned
- Example: 05D0000001

### Test Complexity Categories
- **Waived Tests:** ~130+ tests designated by FDA as simple with low risk of error (e.g., rapid strep, urine pregnancy, blood glucose)
- **Moderate Complexity:** Most automated testing (routine chemistry, hematology, basic microbiology cultures)
- **High Complexity:** Manual procedures requiring significant training (Pap smears, cytogenetics, complex molecular testing)

### Regulatory Requirements
- Labs must have a qualified Laboratory Director
- Personnel must meet education/training requirements based on test complexity
- Quality control and proficiency testing required
- Inspection every 2 years (Certificate of Compliance/Accreditation)
- Application via CMS-116 form
- Fees based on certificate type and test volume

### Healthcare Context
- Required on Medicare/Medicaid claims for lab services
- Listed on provider rosters for practices performing in-office lab tests
- CLIA number ties to a specific physical location (not transferable)
- Multiple CLIA certificates possible for multi-site organizations

---

## 1.7 Medicaid Provider ID

### What It Is
A state-issued identifier assigned to providers enrolled in a state's Medicaid program. Unlike NPI (which is national), Medicaid IDs are state-specific.

### Issuing Body
- Individual state Medicaid agencies (each state has its own)

### Format
- Varies significantly by state
- Some states use numeric only, others alphanumeric
- Length varies from 7 to 13+ characters depending on state
- No national standard format
- Examples:
  - California (Medi-Cal): 9 digits
  - New York: 8 digits
  - Texas: 9 digits
  - Florida: up to 9 alphanumeric characters

### Healthcare Context
- A provider must enroll separately in each state's Medicaid program where they serve Medicaid beneficiaries
- The Medicaid ID is state-specific — a provider in 3 states will have 3 different Medicaid IDs
- Required on Medicaid claims in addition to NPI
- States may require Medicaid enrollment even for providers in managed Medicaid plans
- Some states have separate Medicaid IDs for each practice location
- Medicaid managed care organizations (MCOs) may also assign their own provider IDs

### Common Data Quality Issues
- Confusing Medicaid ID with Medicare ID or NPI
- Outdated Medicaid IDs after re-enrollment
- Missing Medicaid enrollment for required states
- Format inconsistencies across different state systems

---

## 1.8 Medicare Beneficiary Identifier (MBI)

### What It Is
The MBI is the current Medicare identification number for beneficiaries, replacing the Health Insurance Claim Number (HICN) which was SSN-based. The transition was mandated by the Medicare Access and CHIP Reauthorization Act (MACRA) of 2015 to combat identity theft.

### Format (11 Characters)
The MBI is exactly 11 characters with the following position-by-position rules:

| Position | Type | Allowed Values |
|----------|------|----------------|
| 1 | Numeric (N) | 1-9 (not 0) |
| 2 | Alpha-only (C) | A-Z excluding S,L,O,I,B,Z |
| 3 | Alphanumeric (AN) | 0-9 or A-Z excluding S,L,O,I,B,Z |
| 4 | Numeric (N) | 0-9 |
| 5 | Alpha-only (C) | A-Z excluding S,L,O,I,B,Z |
| 6 | Alphanumeric (AN) | 0-9 or A-Z excluding S,L,O,I,B,Z |
| 7 | Numeric (N) | 0-9 |
| 8 | Alpha-only (C) | A-Z excluding S,L,O,I,B,Z |
| 9 | Alpha-only (C) | A-Z excluding S,L,O,I,B,Z |
| 10 | Numeric (N) | 0-9 |
| 11 | Numeric (N) | 0-9 |

**Pattern:** N-C-AN-N-C-AN-N-C-C-N-N

**Excluded Letters:** S, L, O, I, B, Z (to avoid visual confusion with numbers 5, 1, 0, 1, 8, 2)

**Key Properties:**
- Non-intelligent: no embedded meaning in the characters
- Randomly generated
- Unique per beneficiary
- Automatically converts lowercase to uppercase
- No special characters or spaces

### Historical HICN Format (Legacy, Pre-2020)
- Based on SSN + suffix (e.g., 123-45-6789A)
- Suffix indicated relationship to wage earner:
  - A: Primary wage earner
  - B: Aged wife
  - D: Aged widow
  - Various other suffixes for dependents/survivors
- **Deprecated:** All claims must now use MBI

### Healthcare Context for Roster Analysis
- MBI is a beneficiary identifier, not a provider identifier
- Relevant when dealing with Medicare Advantage rosters or member files
- Important to distinguish from provider identifiers (NPI, Medicare Provider ID)

---

## 1.9 ECFMG (Educational Commission for Foreign Medical Graduates)

### What It Is
ECFMG certification is required for International Medical Graduates (IMGs) — physicians who graduated from medical schools outside the United States and Canada — to enter ACGME-accredited residency or fellowship programs in the U.S.

### Certification Requirements
1. **Medical School Eligibility:** School must be listed in the World Directory of Medical Schools with an ECFMG sponsor note
2. **USMLE Exams:** Must pass Step 1 and Step 2 CK (Clinical Knowledge)
3. **Clinical/Communication Skills:** Must satisfy requirements through an ECFMG Pathway (includes Occupational English Test) or previously passed USMLE Step 2 CS
4. **Medical Diploma Verification:** Final diploma verified directly with the issuing institution
5. **Minimum Education:** At least 4 credit years at an eligible medical school

### ECFMG Certificate Number
- Assigned upon certification
- Used throughout residency application process
- Verified by hospitals and health plans during credentialing

### Verification Process
- ECFMG provides primary source verification of IMG credentials
- Hospitals and health plans query ECFMG directly to verify certification status
- ECFMG also offers a Status Verification service

### Healthcare Context
- Critical for credentialing IMGs
- Verifies that foreign medical education meets U.S. standards
- Required before an IMG can take USMLE Step 3
- Required for state medical licensure for IMGs
- Note: As of July 2025, Canadian medical school graduates are classified as IMGs for U.S. GME purposes

---

## 1.10 State License Numbers

### What They Are
State medical and professional license numbers are issued by state licensing boards authorizing a provider to practice their profession within that state. A license is required in every state where a provider practices.

### Format
- **Varies significantly by state and profession**
- No national standard format
- May be numeric only, alpha-numeric, or include prefixes indicating license type
- Examples:
  - California Medical Board: Letter prefix + 5-6 digits (e.g., A12345, G12345)
  - New York: 6 digits (e.g., 123456)
  - Texas Medical Board: Letter prefix + numbers (e.g., M1234)
  - Florida: ME + 5 digits for MD (e.g., ME12345), OS + digits for DO
  - Massachusetts: Numeric only

### License Types (Common)
- **MD License:** Allopathic physician
- **DO License:** Osteopathic physician
- **DDS/DMD License:** Dentist
- **NP/APRN License:** Nurse practitioner / advanced practice registered nurse
- **PA License:** Physician assistant
- **RN License:** Registered nurse
- **DPM License:** Podiatrist
- **DC License:** Chiropractor
- **OD License:** Optometrist
- **Psychology License:** Psychologist (PhD, PsyD)
- **LCSW/LPC/LMFT License:** Licensed clinical social worker / professional counselor / marriage and family therapist

### Multi-State Licensing
- Providers practicing in multiple states need a license in each state
- **Interstate Medical Licensure Compact (IMLC):**
  - Expedited licensing for qualified physicians
  - 40+ member states and territories (as of 2025)
  - Physicians must designate a state of principal license (SPL)
  - Eligibility: MD/DO with board certification, no disciplinary actions, no criminal history
  - Creates an expedited pathway but still issues individual state licenses
- **Nurse Licensure Compact (NLC):**
  - Allows RNs and LPNs to practice in all compact states with one multistate license
  - 40+ member states
  - Primary state of residence issues the multistate license
- **Psychology Interjurisdictional Compact (PSYPACT):**
  - Allows psychologists to practice telepsychology and temporary in-person across compact states

### License Verification
- Primary source: State licensing board websites
- Most states offer online license lookup/verification
- Information available: License number, issue date, expiration date, status (active, inactive, expired, suspended, revoked), disciplinary actions
- AMA Physician Masterfile also contains license information
- Federation of State Medical Boards (FSMB) DocInfo provides centralized lookup

### Common Data Quality Issues
- Expired licenses on roster files
- License number format varying between submissions
- Missing state licenses for states where provider practices
- Inactive vs. active status not properly tracked
- Compact licenses confused with individual state licenses
- Wrong license type captured (e.g., capturing RN license instead of APRN license)

---

## 1.11 UPIN (Unique Physician Identification Number)

### What It Is
The UPIN was a legacy 6-character alphanumeric identifier assigned to physicians by CMS for Medicare claims. It was replaced by the NPI, with the NPI becoming mandatory on May 23, 2008.

### Format
- 6 characters: 1 letter followed by 5 digits (e.g., A12345)

### Current Status
- **Fully deprecated**
- No longer accepted on Medicare claims
- UPIN-to-NPI crosswalk files were historically available for transition
- Some legacy systems may still contain UPIN fields
- Occasionally encountered in historical data and old roster formats

---

## 1.12 Taxonomy Codes (NUCC Health Care Provider Taxonomy)

### What They Are
Taxonomy codes are standardized codes that classify healthcare providers by type, classification, and area of specialization. They are maintained by the National Uniform Claim Committee (NUCC) and required on NPI applications and claims.

### Format
- 10-character alphanumeric code
- Structure: LLLLLLLLLL (10 positions, mix of letters and numbers)
- The code structure has a hierarchical pattern:
  - Characters represent increasingly specific classification levels
  - Example: 207R00000X (Internal Medicine physician)

### Hierarchical Structure
1. **Provider Type (Top Level):**
   - Individual/Group provider categories
   - Non-individual (organizational) categories

2. **Classification (Second Level):**
   - Major discipline area within the provider type
   - Example: "Allopathic & Osteopathic Physicians" is a provider type; "Internal Medicine" is a classification

3. **Area of Specialization (Third Level):**
   - Subspecialty within the classification
   - Example: Under Internal Medicine → Cardiovascular Disease, Gastroenterology, etc.

### Number of Codes
- Approximately 800+ active taxonomy codes
- Updated periodically (current version as of 2025: Version 25.x)
- New specializations added as healthcare evolves

### Major Categories
**Individual Provider Types:**
- Allopathic & Osteopathic Physicians (207-series)
- Dental Providers (122-series)
- Behavioral Health & Social Service Providers (101-104 series)
- Nursing Service Providers (163-series)
- Physician Assistants & Advanced Practice Nursing (363-series)
- Podiatric Medicine & Surgery (213-series)
- Chiropractic Providers (111-series)
- Optometry (152-series)
- Pharmacy (183-series)
- Dietetics (133-series)
- Speech-Language-Hearing (235-series)
- Respiratory Therapy, Occupational Therapy, Physical Therapy, etc.

**Organizational Provider Types:**
- Hospitals (282-series)
- Laboratories (291-series)
- Managed Care Organizations (302-series)
- Nursing & Custodial Care Facilities (311-314 series)
- Ambulatory Health Care Facilities (261-series)
- Home Health (251-series)
- Pharmacy (333-series, organizational)

### Common Taxonomy Codes (Frequently Seen on Rosters)
| Code | Description |
|------|-------------|
| 207R00000X | Internal Medicine |
| 207Q00000X | Family Medicine |
| 208D00000X | General Practice |
| 207RC0000X | Cardiovascular Disease |
| 207RG0100X | Gastroenterology |
| 207RN0300X | Nephrology |
| 207RP1001X | Pulmonary Disease |
| 207RE0101X | Endocrinology |
| 207RH0000X | Hematology |
| 207V00000X | Obstetrics & Gynecology |
| 208600000X | Surgery |
| 2086S0120X | Pediatric Surgery |
| 207T00000X | Neurological Surgery |
| 207X00000X | Orthopaedic Surgery |
| 208C00000X | Colon & Rectal Surgery |
| 207Y00000X | Ophthalmology |
| 207W00000X | Ophthalmology |
| 207K00000X | Allergy & Immunology |
| 207L00000X | Anesthesiology |
| 2084P0800X | Psychiatry |
| 208000000X | Pediatrics |
| 207RR0500X | Rheumatology |
| 207RI0200X | Infectious Disease |
| 363L00000X | Nurse Practitioner |
| 363A00000X | Physician Assistant |
| 122300000X | Dentist |
| 213E00000X | Podiatrist |
| 111N00000X | Chiropractor |
| 152W00000X | Optometrist |
| 261QF0400X | Federally Qualified Health Center |
| 282N00000X | General Acute Care Hospital |

### Healthcare Context
- Required on NPI registration (at least one taxonomy code)
- Up to 15 taxonomy codes can be associated with a single NPI in NPPES
- One must be designated as the "primary" taxonomy
- Used on claims (Loop 2000A PRV segment in 837P)
- Health plans use taxonomy codes to determine network participation by specialty
- Taxonomy code should match the provider's actual scope of practice and board certification

### Common Data Quality Issues
- Using generic/broad taxonomy when specific subspecialty exists
- Taxonomy not updated when provider changes specialty focus
- Mismatch between taxonomy and board certification
- Organizational taxonomy used for individual provider (or vice versa)
- Legacy taxonomy codes that have been retired or replaced
- Multiple taxonomies creating confusion about primary specialty

---

# SECTION 2: CREDENTIALING PROCESS

## 2.1 What Is Provider Credentialing?

Provider credentialing is the systematic process of verifying a healthcare provider's qualifications, training, experience, licensure, and professional standing to ensure they meet standards for participation in health plans, hospital medical staffs, and other healthcare organizations. It is a risk management and quality assurance process.

### Purpose
- Protect patients from unqualified or impaired providers
- Meet regulatory and accreditation requirements
- Enable providers to participate in health plan networks
- Reduce liability exposure for healthcare organizations
- Comply with CMS, state, and NCQA requirements

## 2.2 Full Credentialing Lifecycle

### Phase 1: Application
1. Provider completes application (CAQH ProView or organizational application)
2. Submits supporting documentation (copies of licenses, certifications, malpractice face sheet, CV, etc.)
3. Signs attestation and release of information

### Phase 2: Primary Source Verification (PSV)
The following are verified directly from the issuing source (not from copies provided by the applicant):

| Item | Primary Source | NCQA Requirement |
|------|---------------|------------------|
| Medical/professional education | Medical school directly or AMA Masterfile or AOA | Must verify |
| Residency/fellowship training | Training program directly or AMA Masterfile | Must verify |
| Current state license(s) | State licensing board | Must verify; must be within 180 days of committee decision |
| Board certification (if claimed) | ABMS, AOA, or applicable specialty board | Must verify |
| DEA certificate (if applicable) | DEA or NTIS | Must verify if provider prescribes |
| Hospital privileges (if applicable) | Hospital directly | Must verify |
| Malpractice insurance | Insurance carrier | Must verify |
| Malpractice claims history | NPDB query + carrier | Must verify; NPDB query required |
| OIG/GSA exclusion | OIG LEIE + SAM.gov | Must verify |
| Medicare/Medicaid sanctions | CMS preclusion list, state Medicaid | Must verify |
| Work history (5 years minimum) | Applicant attestation; gaps >6 months explained | Must verify |
| ECFMG (if IMG) | ECFMG directly | Must verify for IMGs |
| Professional references | Contact references | Typically 3 peer references |

### Phase 3: Credentialing Committee Review
- All verified information presented to credentialing committee
- Committee composed of participating network physicians (peer review)
- Committee evaluates:
  - Completeness of application
  - Verified credentials
  - Any red flags (malpractice history, sanctions, gaps in work history, disciplinary actions)
  - Clinical competence indicators
- Committee makes decision: approve, deny, defer, or approve with conditions
- Adverse decisions require fair hearing rights (due process)

### Phase 4: Notification and Contracting
- Provider notified of credentialing decision
- If approved, enrollment/contracting process initiated
- Provider added to network, effective date established
- Provider directory listing created

### Phase 5: Ongoing Monitoring (Between Credentialing Cycles)
- Monthly or more frequent checks of:
  - OIG LEIE exclusion list
  - SAM.gov (federal debarment)
  - State license board actions
  - Medicare/Medicaid sanctions
  - OFAC SDN list (some organizations)
- Any adverse findings trigger immediate review

### Phase 6: Recredentialing
- Occurs every 3 years (36 months) per NCQA standards
- Some states or organizations require every 2 years
- Full re-verification of all primary sources
- Updated NPDB query
- Review of practitioner's performance data, complaints, quality metrics
- Committee review for reapproval

## 2.3 Initial Credentialing vs. Recredentialing

| Aspect | Initial Credentialing | Recredentialing |
|--------|----------------------|-----------------|
| Timing | First time joining network | Every 3 years (typically) |
| Application | Full application (CAQH or paper) | Updated application/re-attestation |
| PSV | All items verified fresh | All items re-verified from primary sources |
| NPDB | Initial query | New query |
| Performance Data | Not available (new to network) | Claims data, complaints, quality metrics reviewed |
| Timeline | 60-180 days typical | 60-120 days typical |
| Trigger | Provider request/recruitment | Calendar cycle (3-year anniversary) |

## 2.4 Primary Source Verification (PSV)

PSV means verifying credentials directly from the original issuing body, not from a copy, the applicant, or a secondary database (with some exceptions).

### NCQA-Recognized Primary Sources and Acceptable Alternatives

| Credential | Primary Source | Acceptable Alternatives |
|-----------|---------------|------------------------|
| Medical education | School directly | AMA Masterfile, AOA Physician Database |
| Residency/Fellowship | Program directly | AMA Masterfile, AOA Physician Database |
| State license | State licensing board | Federation Credentials Verification Service (FCVS) |
| Board certification | Specialty board | ABMS Display Agents, AOA website |
| DEA | DEA directly | NTIS database |
| Malpractice history | NPDB + carrier | No alternative to NPDB |
| OIG exclusion | OIG LEIE database | No alternative |
| SAM exclusion | SAM.gov | No alternative |
| ECFMG | ECFMG directly | — |

### PSV Time Limits
- License verification must be no older than 180 days at time of committee decision
- Board certification verification must be no older than 180 days
- NPDB query must be within 180 days
- Education verification does not expire (one-time verification acceptable)

## 2.5 NCQA Credentialing Standards

The National Committee for Quality Assurance (NCQA) sets the industry gold standard for credentialing. Most commercial health plans seek NCQA Health Plan Accreditation, which includes credentialing standards.

### Key NCQA Credentialing Requirements (CR Standards)
- **CR 1: Credentialing Policies** — Written policies and procedures for credentialing and recredentialing
- **CR 2: Credentialing Committee** — Peer review committee with defined authority
- **CR 3: Initial Credentialing Verification** — All required PSV elements
- **CR 4: Ongoing Monitoring** — Between-cycle monitoring of sanctions and actions
- **CR 5: Recredentialing Verification** — Full re-verification at 36-month intervals
- **CR 6: Notification and Appeals** — Due process for adverse decisions
- **CR 7: Delegated Credentialing** — Standards for oversight of delegated entities
- **CR 8: Credentialing of Organizational Providers** — Standards for facilities

### Provider Types Subject to NCQA Credentialing
NCQA requires credentialing of:
- Physicians (MD, DO)
- Podiatrists (DPM)
- Dentists (DDS, DMD) — if in network
- Chiropractors (DC)
- Behavioral health providers (PhD, PsyD, LCSW, LPC, LMFT)
- Nurse practitioners (NP/APRN)
- Physician assistants (PA)
- Optometrists (OD)
- Other licensed independent practitioners who are network participants

## 2.6 CVO (Credentials Verification Organization)

### What It Is
A CVO is a third-party organization that performs primary source verification on behalf of health plans, hospitals, or other organizations. CVOs centralize the verification process for efficiency.

### NCQA CVO Certification
- CVOs can obtain NCQA Certification or Accreditation
- **NCQA Credentialing Certification:** Assesses organizations that verify practitioner credentials through primary sources
- **NCQA Credentialing Accreditation:** Evaluates organizations providing full-scope credentialing services including committee review
- Health plans may delegate credentialing to NCQA-certified CVOs
- Delegation requires oversight, including annual audits and pre-delegation assessments

### Major CVOs
- VerityStream (Credential My Doc)
- Medallion (CredentialStream/HealthStream)
- symplr (formerly Cactus)
- Certify (CertifyOS)
- MultiPlan credentialing services
- CAQH (while not a traditional CVO, functions similarly)

## 2.7 Privileging vs. Credentialing

| Aspect | Credentialing | Privileging |
|--------|--------------|------------|
| Definition | Verifying qualifications and background | Granting specific clinical permissions |
| Who Does It | Health plans, hospitals, other orgs | Primarily hospitals/facilities |
| Scope | Confirms the provider IS qualified | Determines what the provider CAN DO |
| Example | "This surgeon has a valid license and board certification" | "This surgeon is approved to perform cardiac bypass surgery at this hospital" |
| Regulatory Basis | NCQA, state law, CMS | Joint Commission, CMS Conditions of Participation |
| Frequency | Every 3 years (recredentialing) | Typically every 2 years (reappointment) |
| Clinical Specificity | General qualifications | Specific procedures, patient populations |

### Provisional/Temporary Privileges
- Granted to new providers while full credentialing is completed
- Limited in duration (typically 30-120 days)
- Requires at minimum: current license verification, verification of no current sanctions/exclusions, and sometimes a completed application
- Used to avoid delays in patient care
- Must be supervised by a department chair or designated physician

## 2.8 Delegated Credentialing vs. Direct Credentialing

### Delegated Credentialing
- A health plan delegates the credentialing function to another entity (hospital system, IPA, CVO, medical group)
- The health plan retains ultimate accountability
- NCQA requires:
  - Written delegation agreement
  - Pre-delegation evaluation of the delegate's capabilities
  - Annual audit of the delegate's credentialing activities
  - Right to approve/deny credentialing decisions
  - Delegate must meet the same standards as the health plan

### Direct Credentialing
- The health plan performs all credentialing activities in-house
- Full control over the process
- More resource-intensive but provides direct oversight
- May use CVOs for verification but retains committee review authority

---

# SECTION 3: PROVIDER TYPES AND CREDENTIALS

## 3.1 Provider Types and Degree Abbreviations

### Physicians (Independent Prescribers)
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| MD | Doctor of Medicine | Allopathic physician; completed medical school (4 years) + residency (3-7 years) |
| DO | Doctor of Osteopathic Medicine | Osteopathic physician; same scope as MD with additional OMM training |

### Mid-Level / Advanced Practice Providers
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| NP | Nurse Practitioner | APRN with master's or doctoral degree; prescriptive authority varies by state |
| APRN | Advanced Practice Registered Nurse | Umbrella term for NP, CRNA, CNM, CNS |
| PA | Physician Assistant | Master's degree; practices under physician supervision (varies by state) |
| PA-C | Physician Assistant - Certified | PA who has passed NCCPA certification exam |
| CRNA | Certified Registered Nurse Anesthetist | APRN specializing in anesthesia; doctoral degree required for new graduates |
| CNM | Certified Nurse Midwife | APRN specializing in midwifery |
| CNS | Clinical Nurse Specialist | APRN with specialized clinical focus |

### Dental Providers
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| DDS | Doctor of Dental Surgery | Dentist |
| DMD | Doctor of Dental Medicine | Dentist (equivalent degree to DDS, different school naming) |

### Podiatric Medicine
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| DPM | Doctor of Podiatric Medicine | Foot and ankle specialist; 4-year podiatric medical school + 3-year residency |

### Chiropractic
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| DC | Doctor of Chiropractic | Chiropractor; 4-year chiropractic program |

### Vision Care
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| OD | Doctor of Optometry | Optometrist; 4-year optometry program |

### Behavioral Health
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| PhD | Doctor of Philosophy (Psychology) | Psychologist with research-focused doctoral degree |
| PsyD | Doctor of Psychology | Psychologist with clinical-focused doctoral degree |
| LCSW | Licensed Clinical Social Worker | Master's in Social Work + supervised clinical hours + exam |
| LPC | Licensed Professional Counselor | Master's in Counseling + supervised hours + exam |
| LMFT | Licensed Marriage and Family Therapist | Master's degree + supervised hours + exam |
| LCPC | Licensed Clinical Professional Counselor | Same as LPC in some states |
| LMHC | Licensed Mental Health Counselor | State variation of LPC |
| BCBA | Board Certified Behavior Analyst | Applied behavior analysis |

### Nursing
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| RN | Registered Nurse | Associate's or Bachelor's degree in nursing + NCLEX-RN exam |
| BSN | Bachelor of Science in Nursing | 4-year nursing degree |
| MSN | Master of Science in Nursing | Graduate nursing degree |
| DNP | Doctor of Nursing Practice | Doctoral nursing degree (clinical focus) |
| LPN/LVN | Licensed Practical/Vocational Nurse | 1-year nursing program + NCLEX-PN exam |

### Allied Health
| Abbreviation | Full Title | Description |
|-------------|-----------|-------------|
| PT | Physical Therapist | DPT (Doctor of Physical Therapy) degree required for new graduates |
| DPT | Doctor of Physical Therapy | Entry-level physical therapy degree |
| OT | Occupational Therapist | Master's or doctoral degree in OT |
| OTR/L | Occupational Therapist, Registered/Licensed | Certified OT |
| SLP / CCC-SLP | Speech-Language Pathologist | Master's degree + Certificate of Clinical Competence |
| AuD | Doctor of Audiology | Audiologist with clinical doctoral degree |
| RD / RDN | Registered Dietitian / Registered Dietitian Nutritionist | Bachelor's + supervised practice + exam |
| PharmD | Doctor of Pharmacy | Pharmacist |
| RPh | Registered Pharmacist | Licensed pharmacist |
| RT | Respiratory Therapist | Associate's or bachelor's degree |
| CRT / RRT | Certified/Registered Respiratory Therapist | Credentialed respiratory therapist |

## 3.2 Credential Requirements by Provider Type

### What Each Provider Type Typically Needs for Credentialing:

**Physicians (MD/DO):**
- Medical degree (verified)
- Completed residency training
- Board certification (or board eligible within 5 years of residency completion)
- Active state medical license (each state of practice)
- DEA registration (if prescribing controlled substances)
- Malpractice insurance
- Hospital privileges (if applicable)
- NPI
- CAQH ProView profile

**Nurse Practitioners (NP/APRN):**
- Graduate nursing degree (MSN or DNP)
- National certification in specialty (ANCC, AANP, etc.)
- Active state APRN license
- Collaborative/supervisory agreement (in states requiring one)
- DEA registration (if prescribing)
- Malpractice insurance
- NPI
- CAQH ProView profile

**Physician Assistants (PA/PA-C):**
- Master's degree from ARC-PA accredited program
- NCCPA certification (PA-C)
- Active state PA license
- Supervisory agreement with physician (requirements vary by state)
- DEA registration (if prescribing)
- Malpractice insurance
- NPI
- CAQH ProView profile

**Behavioral Health (PhD, PsyD, LCSW, LPC, LMFT):**
- Graduate degree in applicable discipline
- Active state license
- National certification (if applicable)
- Malpractice insurance
- NPI
- CAQH ProView profile
- Supervised practice hours documentation (for initial licensure)

## 3.3 Supervising Physician Requirements

### NP Supervision
- **Full Practice Authority States (~27 states + DC):** NPs practice independently without physician supervision
- **Reduced Practice States (~12 states):** NPs require a collaborative agreement but not direct supervision
- **Restricted Practice States (~11 states):** NPs require physician supervision or collaborative agreement for prescribing
- Trend is toward full practice authority

### PA Supervision
- Traditionally required a supervising physician for all practice
- **Optimal Team Practice (OTP):** AAPA model allowing PAs to practice to full education without required physician supervision
- ~10+ states have adopted OTP or similar models (as of 2025)
- Most states still require some form of physician collaboration/delegation agreement
- Supervision agreements typically specify scope of practice, prescribing authority, chart review requirements

## 3.4 Board Certification

### What It Means
Board certification indicates a physician has completed training in a specialty and passed rigorous examinations demonstrating expertise. It is a voluntary credential beyond licensure.

### Major Certifying Bodies
- **ABMS (American Board of Medical Specialties):** 24 member boards certifying in 40+ specialties and 90+ subspecialties. The primary board certification system for MD and DO physicians.
- **AOA (American Osteopathic Association) Bureau of Osteopathic Specialists:** Historically separate certification for DO physicians; now transitioning to a single GME accreditation system under ACGME
- **ABPS (American Board of Physician Specialties):** Alternative board certification (not universally recognized by all health plans)
- **Nursing Boards:** ANCC (American Nurses Credentialing Center), AANP (American Association of Nurse Practitioners)
- **NCCPA (National Commission on Certification of Physician Assistants):** PA certification

### ABMS Member Boards (24 Boards)
1. Allergy and Immunology
2. Anesthesiology
3. Colon and Rectal Surgery
4. Dermatology
5. Emergency Medicine
6. Family Medicine
7. Internal Medicine
8. Medical Genetics and Genomics
9. Neurological Surgery
10. Nuclear Medicine
11. Obstetrics and Gynecology
12. Ophthalmology
13. Orthopaedic Surgery
14. Otolaryngology - Head and Neck Surgery
15. Pathology
16. Pediatrics
17. Physical Medicine and Rehabilitation
18. Plastic Surgery
19. Preventive Medicine
20. Psychiatry and Neurology
21. Radiology
22. Surgery
23. Thoracic Surgery
24. Urology

### Maintenance of Certification (MOC) / Continuing Certification
- Most ABMS boards require ongoing certification maintenance
- Components typically include:
  - **Part 1: Professional Standing** — Active medical license
  - **Part 2: Lifelong Learning and Self-Assessment** — CME and knowledge assessment
  - **Part 3: Assessment of Knowledge, Judgment, and Skills** — Periodic secure examination
  - **Part 4: Improvement in Medical Practice** — Practice performance assessment
- Certification cycles vary by board (typically every 10 years for exam, continuous for other components)
- Some boards have moved to shorter, more frequent assessments
- Board-eligible status: Completed training but has not yet passed boards; many health plans allow a grace period (typically 3-5 years from residency/fellowship completion)

## 3.5 Education Pathway

### Physician (MD/DO) Pathway
1. **Undergraduate Education:** 4-year bachelor's degree (pre-med coursework)
2. **Medical School:** 4 years → MD or DO degree
3. **Residency:** 3-7 years depending on specialty:
   - Family Medicine: 3 years
   - Internal Medicine: 3 years
   - General Surgery: 5 years
   - Orthopedic Surgery: 5 years
   - Neurological Surgery: 7 years
   - Pediatrics: 3 years
   - Psychiatry: 4 years
   - OB/GYN: 4 years
   - Emergency Medicine: 3-4 years
   - Anesthesiology: 4 years
   - Radiology: 5 years
   - Pathology: 4 years
   - Dermatology: 4 years (1 transitional + 3 derm)
4. **Fellowship (Optional):** 1-3 years for subspecialty training
   - Cardiology: 3 years
   - Gastroenterology: 3 years
   - Pulmonary/Critical Care: 3 years
   - Infectious Disease: 2 years
   - Oncology: 3 years
   - Neonatology: 3 years
   - Hand Surgery: 1 year
   - Sports Medicine: 1 year
5. **Board Certification:** Pass specialty board exam(s)

### NP Pathway
1. BSN (Bachelor of Science in Nursing): 4 years
2. RN licensure + clinical experience (typically 1-2 years)
3. MSN or DNP with NP specialization: 2-4 years
4. National certification exam (ANCC or AANP)
5. State APRN licensure

### PA Pathway
1. Bachelor's degree with prerequisite coursework
2. Healthcare experience (many programs require 1,000+ hours)
3. Master's degree from ARC-PA accredited PA program: ~27 months
4. PANCE (Physician Assistant National Certifying Examination)
5. State PA licensure
6. PANRE every 10 years for recertification

---

# SECTION 4: PAYOR ENROLLMENT

## 4.1 What Is Provider/Payor Enrollment?

Provider enrollment is the process by which a healthcare provider registers with a health plan (payor) or government program to become an authorized participant who can submit claims and receive reimbursement. It is distinct from credentialing — credentialing verifies qualifications, while enrollment is the administrative/contractual process of joining a network.

### Credentialing vs. Enrollment

| Aspect | Credentialing | Enrollment |
|--------|--------------|------------|
| Purpose | Verify qualifications | Register provider in payor system |
| Focus | Clinical competency and safety | Administrative and contractual |
| Output | Credentialing decision (approve/deny) | Provider ID in payor system, contract execution |
| Timing | Occurs before enrollment | Occurs after credentialing approval |
| Regulatory | NCQA, state law, CMS | CMS enrollment rules, state Medicaid, payor-specific |
| Who does it | Credentialing department | Provider enrollment/network management department |

## 4.2 Medicare Enrollment

### PECOS (Provider Enrollment, Chain, and Ownership System)
- Internet-based system for Medicare enrollment
- Replaces paper CMS-855 forms (though paper is still accepted)
- Managed by CMS and processed by Medicare Administrative Contractors (MACs)
- Link: https://pecos.cms.hhs.gov/

### CMS-855 Enrollment Forms
| Form | Who Uses It | Purpose |
|------|------------|---------|
| CMS-855A | Institutional providers (hospitals, SNFs, HHAs, hospices, ASCs) | Initial enrollment, revalidation, change of information |
| CMS-855B | Clinics, group practices, certain other suppliers | Organizational enrollment |
| CMS-855I | Individual physicians and non-physician practitioners | Individual enrollment |
| CMS-855O | Ordering and certifying physicians/practitioners | Enroll solely to order/certify Medicare items and services (no billing) |
| CMS-855R | Reassignment of Medicare benefits | Allow an individual provider to reassign benefits to a group/organization |
| CMS-855S | DMEPOS suppliers | Durable medical equipment suppliers |

### Information Required on CMS-855
- Legal name and SSN/EIN
- NPI
- Practice location addresses
- Specialty/taxonomy
- Medical school and graduation date
- State licenses
- Current and previous affiliations
- Adverse action history
- Ownership and managing control information (for organizations)
- Authorized official information

### Enrollment Screening Levels
CMS applies different screening levels based on provider/supplier type and risk:
- **Limited:** License verification, database checks (OIG, SAM)
- **Moderate:** Limited screening + unscheduled/unannounced site visits
- **High:** Moderate screening + fingerprint-based criminal background check, enhanced site visit
- Risk categories are assigned by CMS based on provider type

### Revalidation
- All Medicare-enrolled providers must revalidate every 5 years (3 years for DMEPOS suppliers)
- Revalidation requires updating all enrollment information
- Failure to revalidate results in deactivation
- CMS sends notifications before revalidation deadline
- Re-enrollment after deactivation requires a new application

### Medicare Effective Dates
- Generally, effective date is the later of: (a) date of filing, (b) date the provider first began furnishing services at the location
- Retroactive billing allowed up to 30 days before filing date in some cases
- Special rules for new practices, reassignments, and change of ownership

### Medicare Enrollment Updates
- **30-day reporting:** Changes in ownership, adverse legal actions, practice location changes, managing employee changes
- **90-day reporting:** All other changes (phone, specialty, adding a practice location)
- Failure to report timely can result in revocation

## 4.3 Medicaid Enrollment

### State-Specific Process
- Each state has its own Medicaid enrollment process
- No single national enrollment form
- Many states have online enrollment portals
- Some states accept PECOS enrollment as a basis but require additional state-specific information

### Common Requirements
- Valid NPI
- Active state license
- Federal tax ID (TIN)
- Completed state-specific application
- Screening for exclusions (OIG, SAM, state exclusion lists)
- Site inspection (for some provider types)
- Disclosure of ownership and control
- Fingerprinting (some states, for high-risk providers)

### Managed Medicaid
- In most states, Medicaid is largely delivered through managed care organizations (MCOs)
- Providers must be enrolled with the state Medicaid agency AND contracted with the MCO
- MCOs often have their own credentialing and enrollment requirements on top of state requirements
- Network adequacy requirements drive MCO enrollment needs
- 42 CFR 438 governs Medicaid managed care credentialing requirements

## 4.4 Commercial Payor Enrollment

### Process Overview
1. Provider identifies target health plans for participation
2. Contacts health plan's provider relations or network department
3. Submits application (typically through CAQH ProView)
4. Health plan performs credentialing
5. Contract negotiation (fee schedule, terms)
6. Execution of participation agreement
7. Provider loaded into payor system with effective date
8. Provider appears in directory

### Network Participation Agreements
- Legal contracts between provider and health plan
- Specify:
  - Fee schedule or reimbursement methodology
  - Covered services
  - Provider obligations (credentialing, quality reporting, directory accuracy)
  - Health plan obligations (timely payment, dispute resolution)
  - Term and renewal provisions
  - Termination clauses (with/without cause, notice periods)
  - Hold-harmless provisions
  - Network type (PPO, HMO, etc.)

### Effective Dates
- The date the provider is officially in-network and can see patients under the plan
- May differ from credentialing approval date (contract must also be executed)
- Some payors allow retroactive effective dates (back to application date or credentialing approval date)
- Effective date determines when claims will be adjudicated as in-network
- Important for patients and billing: services rendered before effective date will process as out-of-network

### Retroactive Enrollment
- Some payors offer limited retroactive enrollment (30-90 days)
- Medicare allows limited retroactive billing
- Medicaid policies vary by state
- Critical issue for claims filing timely limits

## 4.5 Provider Directories

### Regulatory Requirements
- ACA Section 1311(e)(3) requires qualified health plans to maintain up-to-date provider directories
- CMS requires Medicare Advantage plans to maintain online provider directories
- State laws increasingly mandate directory accuracy (many states require updates within 30 days of changes)
- No Surprises Act (2022) strengthened directory accuracy requirements
- CMS Provider Directory Requirements for Medicare Advantage and Part D:
  - Online, searchable directory
  - Updated regularly (at least monthly)
  - Include: provider name, specialty, address, phone, accepting new patients, languages, accessibility, hospital affiliations

### Directory Data Elements
- Provider name (legal and display name)
- Gender
- Specialty
- Board certification
- Practice locations (all)
- Office phone number
- Accepting new patients (Y/N)
- Languages spoken
- Hospital affiliations
- Group affiliation
- Network participation (which products/plans)
- Telehealth availability
- Accessibility features

### Network Adequacy
- Health plans must maintain sufficient provider networks to ensure access to care
- Requirements include:
  - Geographic access standards (distance/time to nearest provider by specialty)
  - Appointment availability standards (urgent vs. routine wait times)
  - Provider-to-member ratios
  - Adequate specialty mix
- Network adequacy assessed by:
  - CMS (for Medicare Advantage)
  - State insurance departments (for commercial and Medicaid plans)
  - State Medicaid agencies (for managed Medicaid)

### Network Types
| Type | Description |
|------|-------------|
| **HMO (Health Maintenance Organization)** | Restricted to in-network providers; requires PCP and referrals; lowest premiums |
| **PPO (Preferred Provider Organization)** | In-network and out-of-network coverage; no PCP/referral required; higher premiums |
| **POS (Point of Service)** | Hybrid of HMO/PPO; PCP required, referrals for specialists, some out-of-network coverage |
| **EPO (Exclusive Provider Organization)** | Like HMO but typically no PCP/referral requirement; no out-of-network coverage except emergencies |
| **Indemnity** | Traditional fee-for-service; any provider; highest premiums |

### Line of Business (LOB)
- Commercial (individual marketplace, employer group, small group, large group)
- Medicare Advantage (HMO, PPO, PFFS, SNP)
- Medicaid Managed Care
- Exchange/Marketplace (Qualified Health Plans)
- CHIP (Children's Health Insurance Program)
- Workers' Compensation
- Tricare
- Federal Employee Health Benefits (FEHB)
- A provider may be credentialed once but enrolled in multiple LOBs with different contracts

---

# SECTION 5: MONITORING AND SANCTIONS

## 5.1 Ongoing Monitoring Requirements

Healthcare organizations (health plans, hospitals) must continuously monitor providers between credentialing cycles. NCQA Standard CR 4 requires ongoing monitoring.

### Monitoring Sources and Frequency

| Source | What It Checks | Required Frequency |
|--------|---------------|-------------------|
| OIG LEIE | Federal healthcare program exclusions | Monthly (NCQA minimum) |
| SAM.gov | Federal debarment/exclusion | Monthly |
| NPDB | Malpractice payments, adverse actions | At credentialing/recredentialing; some use Continuous Query |
| State License Boards | License status, disciplinary actions | Monthly or as available |
| Medicare/Medicaid Sanctions | CMS preclusion list, opt-out list | Monthly |
| OFAC SDN List | Terrorism/sanctions screening | At enrollment; periodic refresh |
| CMS Medicare Exclusion Database | Medicare revocations | Monthly |
| State Medicaid Exclusion Lists | State-level exclusions | Monthly (where available) |

## 5.2 OIG Exclusion List (LEIE)

### What It Is
The List of Excluded Individuals and Entities (LEIE) is maintained by the Office of Inspector General (OIG) of HHS. It contains individuals and organizations excluded from participation in all federal healthcare programs (Medicare, Medicaid, CHIP, TRICARE, etc.).

### Types of Exclusions
- **Mandatory Exclusions (Section 1128(a)):**
  - Conviction of healthcare program-related crimes
  - Conviction of patient abuse/neglect
  - Felony conviction for healthcare fraud
  - Felony conviction for controlled substance offenses
  - Minimum 5-year exclusion for mandatory exclusions

- **Permissive Exclusions (Section 1128(b)):**
  - Misdemeanor healthcare fraud conviction
  - License revocation or suspension
  - Exclusion from a state health program
  - Claims for excessive or unnecessary services
  - Failure to provide quality care
  - Fraud/kickback-related offenses
  - Variable exclusion periods

### Consequences of Exclusion
- No federal healthcare program payment for any items/services furnished, ordered, or prescribed by the excluded individual/entity
- Healthcare organizations that employ or contract with excluded individuals face Civil Monetary Penalties (CMPs) of up to $100,000 per item/service + 3x the amount claimed
- Organizations must refund any payments received for services by excluded individuals

### Checking Requirements
- All employees, contractors, and providers must be checked against LEIE
- Must check at time of hire/enrollment and monthly thereafter (best practice; NCQA requires at least monthly)
- Available: https://exclusions.oig.hhs.gov/
- Downloadable database and monthly supplements available

### Reinstatement
- Individual must apply after minimum exclusion period expires
- Not automatic — requires formal application and OIG approval
- Must demonstrate rehabilitation and compliance

## 5.3 SAM (System for Award Management)

### What It Is
SAM.gov is the federal government's system for entity registration and exclusions. The Exclusions section contains records of individuals and organizations debarred, suspended, proposed for debarment, or declared ineligible from receiving federal contracts or assistance.

### What It Tracks
- **Debarment:** Formal exclusion from federal contracts/programs
- **Suspension:** Temporary exclusion pending investigation
- **Proposed Debarment:** Notice of intent to debar
- **Ineligibility:** Statutory or regulatory disqualification
- **Voluntary Exclusion:** Self-imposed exclusion

### Healthcare Relevance
- Healthcare organizations receiving federal funds must screen against SAM
- Complementary to OIG LEIE — some exclusions appear in SAM but not LEIE, and vice versa
- Must check both OIG and SAM for comprehensive exclusion screening
- Monthly checking recommended

### Access
- Free public access at https://sam.gov
- Downloadable data extracts available
- API access available for bulk screening

## 5.4 NPDB (National Practitioner Data Bank)

### What It Is
The NPDB is a confidential information clearinghouse created by Congress in 1986 (operational since 1990) to improve healthcare quality by preventing practitioners from concealing past malpractice payments and adverse actions when moving between states.

### What Is Reported
1. **Medical Malpractice Payments:** All payments made on behalf of physicians, dentists, and other licensed health care practitioners, regardless of amount
2. **Adverse Clinical Privileges Actions:** Restrictions, suspensions, revocations, or denial of hospital privileges lasting >30 days
3. **Adverse Professional Society Membership Actions:** Related to professional competence or conduct
4. **State Licensure/Certification Actions:** Revocations, suspensions, censures, reprimands, probation, surrender of license
5. **Federal Licensure/Certification Actions:** DEA actions, etc.
6. **Negative Actions by Peer Review Organizations/QIOs**
7. **Negative Actions by Private Accreditation Organizations**
8. **Medicare/Medicaid Exclusions:** OIG exclusion actions
9. **Healthcare-Related Criminal Convictions:** Federal, state, and local
10. **Healthcare-Related Civil Judgments**
11. **Other Adjudicated Actions or Decisions**

### Who Must Report
- Medical malpractice payers (insurance companies, hospitals self-insured)
- Hospitals and healthcare entities (privileges actions)
- State medical boards and other licensing agencies
- Federal agencies (DEA, OIG, etc.)
- Health plans (adverse actions against providers)
- Professional societies (membership actions)

### Who Can Query
- Hospitals (mandatory — must query when granting/renewing privileges)
- Health plans (for credentialing/recredentialing)
- State licensing boards
- Federal and state government agencies
- Healthcare practitioners (self-query only)
- Professional societies (for membership decisions)
- Attorneys and researchers (limited access)

### Continuous Query
- An automated proactive notification service
- Organizations enroll providers in Continuous Query
- NPDB sends notification whenever a new report is filed about an enrolled provider
- Replaces the need for periodic one-time queries between credentialing cycles
- More timely than periodic queries — alerts within days of a new report
- Cost: per-provider annual fee
- Increasingly adopted as a best practice

### Query Timing Requirements
- Hospitals: Must query at appointment and every 2 years (Joint Commission) or per bylaws
- Health plans: Must query at initial credentialing and recredentialing (within 180 days of committee decision per NCQA)
- State licensing boards: May query at initial licensure and renewal

## 5.5 OFAC (Office of Foreign Assets Control)

### What It Is
OFAC, part of the U.S. Department of the Treasury, administers economic sanctions against targeted countries, terrorism, narcotics trafficking, and other threats. The Specially Designated Nationals (SDN) list includes individuals and entities whose assets are blocked.

### SDN List
- Contains names, aliases, addresses, identification numbers of sanctioned individuals and entities
- Anyone (including healthcare organizations) is prohibited from doing business with SDN-listed persons
- Violations carry severe civil and criminal penalties
- Civil penalties up to $356,579 per violation (adjusted periodically)
- Criminal penalties up to $1 million and 20 years imprisonment

### Healthcare Relevance
- Healthcare organizations screen providers and vendors against SDN list
- Not specifically a healthcare regulation, but compliance expected
- CMS does not explicitly require OFAC screening for credentialing, but many organizations include it as part of comprehensive sanctions screening
- Usually checked at enrollment and periodically thereafter

### Screening
- Free search tool at https://sanctionssearch.ofac.treas.gov/
- Downloadable SDN list available
- False positive rates can be high due to common names

## 5.6 State License Board Actions

### Types of Actions
- **Revocation:** Complete loss of license; most severe action
- **Suspension:** Temporary loss of license for defined period
- **Probation:** License remains active but with conditions/monitoring
- **Restriction/Limitation:** Specific activities limited (e.g., cannot prescribe opioids, cannot perform surgery)
- **Reprimand/Censure:** Formal written criticism; license remains intact
- **Fine/Penalty:** Monetary penalty
- **Surrender:** Voluntary surrender of license (may be in lieu of further action)
- **Consent Order/Agreement:** Negotiated agreement with conditions
- **Temporary Suspension/Emergency Order:** Immediate suspension pending investigation (public safety)
- **Denial of Initial Application:** Refused license

### Reporting and Monitoring
- State boards publish disciplinary actions (most available online)
- Federation of State Medical Boards (FSMB) aggregates physician disciplinary data
- Actions are reportable to NPDB
- Health plans must monitor for state board actions monthly
- An action in one state may affect licensure in other states

## 5.7 Medicare/Medicaid Sanctions

### CMS Preclusion List
- Contains providers/prescribers precluded from receiving payment under Medicare Part D or Medicare Advantage
- Updated periodically by CMS
- Based on revocation from Medicare enrollment or OIG exclusion
- Medicare Advantage and Part D sponsors must check and cannot pay precluded providers

### Medicare Revocation
- CMS can revoke Medicare enrollment for:
  - Felony conviction
  - License revocation/suspension
  - False/misleading information on enrollment application
  - Abuse of billing privileges
  - Non-compliance with enrollment requirements
  - Failure to report changes
- Revocation includes a re-enrollment bar (1-10 years depending on severity)

### Medicaid Termination
- States can terminate Medicaid enrollment for similar reasons
- Section 6501 of ACA requires state Medicaid agencies to terminate providers who have been terminated from another state's Medicaid or from Medicare
- Creates a domino effect across states

## 5.8 Continuous Monitoring vs. Point-in-Time Checks

| Aspect | Continuous Monitoring | Point-in-Time Checks |
|--------|----------------------|---------------------|
| Frequency | Ongoing/real-time alerts | At specific intervals (monthly, quarterly, annually) |
| NPDB | Continuous Query enrollment | One-time query at credentialing/recredentialing |
| Sanctions | Daily/real-time screening | Monthly batch screening |
| License | Automated status monitoring | Periodic manual verification |
| Advantages | Immediate alert on adverse events | Lower cost, simpler to implement |
| Disadvantages | Higher cost, requires infrastructure | Potential gap between event and detection |
| Industry Trend | Moving toward continuous monitoring | Still common but decreasing as primary method |
| NCQA Requirement | Not required but encouraged | Monthly OIG/SAM checks required |

---

# SECTION 6: ROSTER FILE STANDARDS

## 6.1 What Is a Provider Roster?

A provider roster is a structured data file containing information about healthcare providers participating in a health plan's network. It serves as the definitive record of which providers are in-network and their associated details.

### Purpose
- Define network composition for a health plan
- Feed provider directories (member-facing and internal)
- Support claims adjudication (determining in-network vs. out-of-network)
- Meet regulatory reporting requirements
- Verify network adequacy
- Support credentialing and recredentialing workflows
- CMS and state regulatory submissions

### Typical Submission Cycles
- Initial load: Full roster at contract inception
- Ongoing: Monthly or more frequent updates (additions, terminations, changes)
- Full file replacement: Quarterly or annually (some plans)
- Ad hoc: As needed for corrections or special requests

## 6.2 CMS Roster File Requirements

### Medicare Advantage Provider Roster
CMS requires Medicare Advantage organizations to submit provider network data. Key requirements:
- Provider name (first, middle, last)
- NPI (individual and organizational)
- TIN/EIN
- Specialty/taxonomy code
- Practice location addresses
- Phone number
- Accepting new patients indicator
- Languages spoken
- Accessibility indicators
- Hospital affiliations
- Board certification status
- Gender
- Network/product association

### Health Plan Management System (HPMS) Provider Data Submissions
- Medicare Advantage plans submit provider data through HPMS
- Used for network adequacy analysis
- Required fields aligned with CMS provider directory requirements

## 6.3 CAQH DirectAssure

### What It Is
CAQH DirectAssure (now known as CAQH Directory Management) is a solution for managing provider directory data accuracy and roster data exchange between health plans and providers.

### How It Works
- Providers confirm and update their practice-level information (non-clinical)
- Data collected includes: practice locations, office hours, phone numbers, languages, accepting new patients, telehealth capabilities
- Health plans receive confirmed/attested data
- Reduces outreach burden on health plans and providers
- Multi-layered analytics to improve data quality

### Statistics
- 1.8M+ confirmed provider profiles
- 2.5M+ providers confirming data within 120 days
- 53+ participating health plans

### Roster Data Quality
- Data is provider-attested (self-reported, confirmed)
- Higher quality than health plan-maintained data alone
- Helps meet directory accuracy regulations

## 6.4 Standard Roster Fields and Business Meaning

### Practitioner-Level Fields

| Field | Business Meaning | Format Notes |
|-------|-----------------|--------------|
| Provider Last Name | Legal last name | May differ from preferred/display name |
| Provider First Name | Legal first name | |
| Provider Middle Name | Legal middle name/initial | |
| Provider Suffix | Jr., Sr., III, etc. | NOT degrees (MD, DO) |
| Provider Degree | MD, DO, NP, PA, etc. | Credential abbreviation |
| Provider Gender | Male/Female/Non-binary | M/F or spelled out |
| Provider Date of Birth | DOB | MM/DD/YYYY or YYYY-MM-DD |
| NPI (Individual) | Type 1 NPI | 10 digits |
| NPI (Organization) | Type 2 NPI for billing group | 10 digits |
| TIN/EIN | Tax ID for billing group | 9 digits (XX-XXXXXXX) |
| SSN | Individual SSN | 9 digits — should NOT be on roster files |
| DEA Number | DEA registration | 2 letters + 7 digits |
| State License Number | State professional license | Format varies by state |
| State License State | State of license | 2-letter state code |
| State License Expiration | Expiration date | Date format |
| Board Certified | Y/N indicator | |
| Board Name | Certifying board | e.g., ABIM, ABFM |
| Board Specialty | Certified specialty | |
| Taxonomy Code | NUCC taxonomy | 10-character alphanumeric |
| Primary Specialty | Main practice specialty | |
| Secondary Specialty | Additional specialty | |
| Medicare ID | Medicare enrollment identifier | |
| Medicaid ID | State Medicaid identifier | State-specific format |
| CAQH ID | CAQH ProView identifier | 7-8 digits |
| Languages Spoken | Languages provider speaks | May be coded or free text |
| Practice Address Line 1 | Street address of practice | |
| Practice Address Line 2 | Suite/floor/building | |
| Practice City | City | |
| Practice State | State | 2-letter code |
| Practice ZIP | ZIP code | 5 or 9 digits (ZIP+4) |
| Practice Phone | Office phone | 10 digits |
| Practice Fax | Office fax | 10 digits |
| Accepting New Patients | Whether accepting new patients | Y/N |
| Effective Date | Date provider became in-network at this location | Date format |
| Termination Date | Date provider is no longer in-network at this location | Date format or blank |
| Network ID | Identifier for the network/plan | Payor-specific |
| Product/LOB | Line of business or product | e.g., Commercial PPO, Medicare Advantage |
| Credentialing Status | Current credentialing status | Active, Pending, Terminated |
| Hospital Affiliations | Associated hospitals | Name/NPI of hospital |
| Telehealth Indicator | Offers telehealth services | Y/N |

### Facility/Organization-Level Fields

| Field | Business Meaning |
|-------|-----------------|
| Facility Name | Legal name of organization |
| Facility DBA | Doing-business-as name |
| Facility NPI (Type 2) | Organization NPI |
| Facility TIN | Organization tax ID |
| Facility Type | Hospital, ASC, lab, clinic, etc. |
| CLIA Number | Lab certification number |
| Medicare Provider Number | Medicare-assigned facility ID |
| Accreditation | Joint Commission, AAAHC, etc. |
| Bed Count | Number of beds (hospitals) |
| Service Types | Services offered |
| Operating Hours | Hours of operation |

## 6.5 Practitioner vs. Facility vs. Group Roster Differences

### Practitioner Roster
- Individual-level provider data
- One row per provider-location-network combination
- Links individual NPI to group/billing NPI and TIN
- Contains personal credentials (education, license, board cert)

### Facility Roster
- Organization/location-level data
- One row per facility-location-network combination
- Contains organizational credentials (accreditation, CLIA, Medicare certification)
- No individual provider credentials
- Facility types: hospitals, ASCs, SNFs, labs, imaging centers, etc.

### Group Roster
- Group practice-level data
- Links group NPI/TIN to associated individual providers
- May contain aggregate information about the practice
- Used for contract and network management

## 6.6 Multi-Location Providers

### How Addresses Are Structured
- A single provider may practice at multiple locations
- Each location is typically a separate row on the roster
- Same individual NPI appears on multiple rows, each with a different practice address
- The billing/group NPI and TIN may differ by location (if the provider works for different groups)
- Primary vs. secondary location designation may be indicated

### Common Challenges
- Determining which locations are "active" vs. historical
- Matching providers across locations when identifiers differ (different TIN per location)
- Avoiding duplicate directory listings
- Keeping addresses current when providers change locations

## 6.7 Effective Dates and Termination Dates

### Effective Date
- Date the provider is considered in-network for a specific plan/product/location
- May be the credentialing approval date, contract execution date, or a specified start date
- Used for claims adjudication — claims before this date are out-of-network
- Should be specific to each plan/network/location combination

### Termination Date
- Date the provider is no longer in-network
- May be voluntary (provider leaves) or involuntary (plan terminates)
- Claims after this date are out-of-network
- Typically requires advance notice (30-90 days per contract)
- Some states have continuity-of-care requirements after termination
- Blank/null termination date typically means currently active

### Date Business Rules
- Effective date must be before termination date
- A provider can have multiple effective/termination date pairs (rejoining a network)
- Historical records may be maintained with past effective and termination dates
- Future effective dates indicate pending enrollment

## 6.8 Network Types and Line of Business

### Network Types

**HMO (Health Maintenance Organization):**
- Closed panel network
- Members must use in-network providers (except emergencies)
- Requires PCP selection and specialist referrals
- Typically lowest cost-sharing for members
- Narrow network

**PPO (Preferred Provider Organization):**
- Open panel network
- Members can see any provider but pay less for in-network
- No PCP or referral requirement
- Higher premiums, broader access
- Most common commercial product type

**POS (Point of Service):**
- Hybrid HMO/PPO
- In-network works like HMO (PCP, referrals)
- Out-of-network available but at higher cost
- Less common

**EPO (Exclusive Provider Organization):**
- Like PPO but no out-of-network coverage (except emergencies)
- No PCP/referral requirement
- Narrower network than PPO, broader than HMO

### Line of Business (LOB) Concepts
- **Commercial Individual:** Marketplace/Exchange plans, individual policies
- **Commercial Group:** Employer-sponsored plans (small group, large group)
- **Medicare Advantage (MA):** Medicare Part C managed plans (MA-HMO, MA-PPO, MA-PFFS, D-SNP, I-SNP, C-SNP)
- **Medicaid Managed Care:** State Medicaid through MCOs
- **Dual Special Needs Plan (D-SNP):** For dual-eligible (Medicare + Medicaid) members
- **Exchange/Marketplace:** ACA Qualified Health Plans (QHP)
- **CHIP:** Children's Health Insurance Program
- **ASO/Self-Funded:** Administrative Services Only — employer bears risk, plan provides administration/network
- A single provider may participate in multiple LOBs under different contracts with different fee schedules

---

# SECTION 7: DATA QUALITY PATTERNS IN HEALTHCARE

## 7.1 Common Data Entry Errors in Roster Files

### NPI/TIN Transposition
- NPI is 10 digits, TIN/EIN is 9 digits — transposition between these two fields is common
- Detection: Check digit validation on NPI (Luhn); TIN digit count validation
- TIN placed in NPI field (9 digits — invalid NPI)
- NPI placed in TIN field (10 digits — invalid TIN)

### Date Format Inconsistencies
- Different payors use different date formats:
  - MM/DD/YYYY (most common in U.S.)
  - YYYY-MM-DD (ISO 8601)
  - MMDDYYYY (no separators)
  - DD/MM/YYYY (rare in U.S. but occurs with international systems)
  - M/D/YYYY (no leading zeros)
- Excel date serial numbers appearing in CSV exports (e.g., 45292 instead of 01/01/2024)
- Two-digit vs. four-digit year confusion
- Date fields stored as text vs. actual date values

### Name Matching Challenges
- **Legal name vs. preferred name:** Provider credentialed as "William" but goes by "Bill"; roster may have either
- **Maiden name vs. married name:** Especially for female providers who change surnames
- **Suffixes:** Jr., Sr., II, III, IV — sometimes captured in last name field, sometimes in separate suffix field
- **Hyphens:** Hyphenated last names may be stored with or without hyphen, or split across fields
- **Prefixes:** "De", "Van", "O'", "Mc", "Mac" — inconsistent capitalization and spacing
- **Middle name/initial:** Sometimes present, sometimes missing
- **Credentials in name fields:** "John Smith, MD" in the last name field
- **Special characters:** Apostrophes, accented characters, periods
- **Name order:** First/Last swapped between systems

### Address Standardization Issues
- Street abbreviations: "Street" vs "St" vs "St." vs "STR"
- Directionals: "North" vs "N" vs "N."
- Suite/Unit: "Suite 100" vs "Ste 100" vs "Ste. 100" vs "#100"
- PO Box vs. physical address
- Incomplete ZIP codes (5 vs 9 digits)
- USPS CASS (Coding Accuracy Support System) certification standardizes addresses
- Building/floor numbers in inconsistent locations
- Multi-tenant buildings with different suites for different practices

### Phone/Fax Confusion
- Phone number in fax field and vice versa
- Inconsistent formatting: (555) 555-5555 vs 555-555-5555 vs 5555555555
- Missing area codes
- Extension numbers handled inconsistently
- Outdated phone numbers
- Cell phone vs. office phone in wrong field
- Answering service number listed as practice number

### Degree/Credential Abbreviation Variations
- "MD" vs "M.D." vs "Doctor of Medicine"
- "PA-C" vs "PA" vs "PAC" vs "Physician Assistant"
- "NP" vs "APRN" vs "ARNP" vs "APN" vs "FNP-C" vs "ANP-BC"
- "LCSW" vs "L.C.S.W." vs "Licensed Clinical Social Worker"
- "DPM" vs "D.P.M."
- Degrees in name suffix field vs. degree field
- Multiple credentials listed in various orders

### State License Number Format Variations
- Different formats by state (see Section 1.10)
- Leading zeros dropped or added
- Alpha prefixes sometimes included, sometimes not
- License type prefix confusion (MD license vs. DO license)
- Cross-referencing wrong license type for the provider's actual credential

### Taxonomy Code Assignment Challenges
- Wrong taxonomy code for the provider's actual specialty
- Using the primary care taxonomy when provider is a specialist
- Organizational taxonomy assigned to individual provider
- Outdated taxonomy codes that have been replaced
- Multiple taxonomy codes with wrong one designated as primary
- Taxonomy not matching board certification
- "General Practice" (208D00000X) used as catch-all when specific specialty exists

### Provider Type vs. Role Confusion
- **Provider type** = credential/degree (MD, NP, PA) — what the provider IS
- **Provider role** = function in the practice (PCP, specialist, hospitalist) — what the provider DOES
- An MD can be a PCP or a specialist
- An NP can be a PCP
- Roster fields may conflate type and role
- "PCP" is not a provider type but a network role designation
- Important for network adequacy: PCP counts depend on role, not just type

## 7.2 Other Common Data Quality Issues

### Duplicate Records
- Same provider appearing multiple times with slight variations
- Different spellings of name, different address format, but same NPI
- Result of multiple data sources being merged without deduplication
- Critical to resolve: affects directory accuracy, credentialing tracking, claims processing

### Stale Data
- Providers who have left a practice but remain on roster
- Expired licenses listed as active
- Old addresses for providers who have relocated
- Terminated providers without termination dates

### Missing Required Fields
- NPI missing or invalid
- TIN missing
- No taxonomy code
- No practice address (especially for telehealth-only providers)
- Missing license information

### Inconsistent Identifiers Across Files
- Provider with different NPIs across different roster submissions (usually indicates an error)
- Different CAQH IDs for the same provider
- TIN changes not reflected across all files

---

# SECTION 8: REGULATORY FRAMEWORK

## 8.1 CMS Conditions of Participation (CoPs)

### What They Are
CMS Conditions of Participation are federal regulations that healthcare organizations must meet to participate in Medicare and Medicaid programs. They establish minimum quality and safety standards.

### Credentialing-Related CoPs
- **42 CFR 482.12 (Hospitals):** Governing body must ensure medical staff credentialing and privileging
- **42 CFR 482.22 (Medical Staff):** Requirements for organized medical staff, bylaws, credentialing, privileging, peer review
- **42 CFR 485 (CAH):** Critical Access Hospital conditions including credentialing
- **42 CFR 416 (ASC):** Ambulatory Surgical Center requirements
- Requirements include:
  - Medical staff bylaws
  - Criteria for granting privileges
  - Mechanism for privileging and credentialing
  - Peer review process
  - Due process rights for adverse privileging decisions

## 8.2 State Credentialing Laws

### Overview
- Each state has its own laws governing provider credentialing
- State laws may be more stringent than federal requirements
- Key state-level requirements often include:
  - Maximum timeframes for credentialing decisions (e.g., 60, 90, or 180 days from complete application)
  - Specific verification requirements beyond NCQA minimums
  - Clean claim payment timelines once enrollment is effective
  - Provider access to credentialing files
  - Due process requirements for adverse decisions
  - Delegated credentialing oversight requirements
  - Provisional credentialing allowances

### Notable State Requirements (Examples)
- **California:** Knox-Keene Act requirements for managed care credentialing; maximum 60 working days for credentialing decision
- **Texas:** TDI credentialing rules under 28 TAC Ch. 3; 60-day provisional credentialing available
- **New York:** Public Health Law requirements; credentialing within 150 days
- **Florida:** Specific requirements under Florida Administrative Code for managed care credentialing
- States increasingly requiring telehealth credentialing standards

## 8.3 NCQA Health Plan Accreditation — Credentialing Standards

### Overview
NCQA (National Committee for Quality Assurance) accreditation is the industry gold standard for health plan quality. Credentialing is a core component.

### Credentialing Standards (CR)
NCQA credentialing standards require:

**For Individual Practitioners:**
1. Written credentialing and recredentialing policies
2. Application with attestation
3. Primary source verification of:
   - License (within 180 days of decision)
   - Board certification (if claimed)
   - Education and training
   - DEA/CDS (if applicable)
   - Work history (5 years)
   - Malpractice history (NPDB query)
   - OIG/SAM exclusion check
4. Credentialing committee review by peers
5. Recredentialing every 36 months
6. Ongoing monitoring (monthly OIG/SAM, license)
7. Due process for adverse decisions

**For Organizational Providers:**
1. Confirm accreditation or CMS certification
2. Verify good standing with regulatory bodies
3. Verify malpractice/liability insurance
4. Site visits for certain organizational types
5. Recredentialing every 36 months

### NCQA Scoring
- Health plans scored on compliance with each standard element
- Results factor into overall NCQA accreditation score
- Non-compliance can result in corrective action plans or loss of accreditation

## 8.4 URAC Credentialing Accreditation

### What It Is
URAC (Utilization Review Accreditation Commission) is an independent accreditation organization that offers credentialing accreditation as one of its programs.

### Key Standards
- Similar to NCQA but with some differences in specifics
- Covers:
  - Credentialing policies and procedures
  - Verification requirements
  - Committee structure
  - Recredentialing cycle
  - Ongoing monitoring
  - Delegated credentialing oversight
  - Appeals and due process
- Less widely adopted than NCQA for health plan accreditation but recognized by CMS and some states

## 8.5 Joint Commission Requirements

### Medical Staff Standards
The Joint Commission (formerly JCAHO) accredits hospitals and healthcare facilities. Its Medical Staff (MS) standards cover credentialing and privileging:

- **MS.06.01.01:** Medical staff bylaws define credentialing process
- **MS.06.01.03:** Process for granting initial privileges
- **MS.06.01.05:** Criteria-based evaluation of practitioners
- **MS.09.01.01:** Focused Professional Practice Evaluation (FPPE) for all new privileges
- **MS.08.01.01:** Ongoing Professional Practice Evaluation (OPPE)

### Key Requirements
- Initial appointment and reappointment process defined in bylaws
- Credentials verification from primary sources
- Hospital-specific privilege delineation (what procedures a provider can perform at that facility)
- Minimum every 2-year reappointment cycle
- FPPE: Monitoring period for all newly privileged practitioners
- OPPE: Ongoing monitoring of all practitioners' performance data
- Peer review of adverse events
- Due process rights for practitioners facing adverse decisions

## 8.6 42 CFR Part 438 — Medicaid Managed Care

### Credentialing Requirements (438.214)
This federal regulation governs credentialing for Medicaid managed care programs:

- **438.214(a):** Each state must establish a uniform credentialing and recredentialing policy for its MCOs
- **438.214(b):** MCO selection and retention policies must:
  - Not discriminate against providers serving high-risk populations or specializing in costly conditions
  - Follow the state's credentialing policy
- **438.214(c):** MCOs must follow a documented process for credentialing and recredentialing that complies with requirements in 438.214(b)(2)
- **438.214(d):** States may require MCOs to credential providers meeting certain qualifications
- **438.214(e):** MCOs must comply with any additional state-specified credentialing/recredentialing requirements

### Network Adequacy (438.68 and 438.206)
- States must develop and enforce network adequacy standards
- Standards must include:
  - Time and distance standards for key provider types
  - Provider-to-member ratios
  - Standards for timely access to care
- MCOs must maintain networks sufficient to provide adequate access
- States must monitor and enforce compliance

## 8.7 ACA Network Adequacy Requirements

### Essential Requirements
The Affordable Care Act established network adequacy requirements for Qualified Health Plans (QHPs):

- Plans must maintain a network sufficient in number and types of providers to ensure all services are accessible without unreasonable delay
- Provider directories must be accurate and publicly available
- Essential Community Providers (ECPs) requirement: QHPs must contract with a minimum percentage of available ECPs in their service area
- CMS reviews network adequacy as part of QHP certification

### Federal Standards
- CMS developed quantitative network adequacy standards
- Time and distance standards by provider type and county type (large metro, metro, micro, rural, CEAC)
- Example standards (vary by provider type):
  - PCP: 10 miles/15 minutes in urban areas, up to 60 miles in rural
  - Specialists: 20-60 miles depending on specialty and area type
  - Hospitals: 20-40 miles depending on area type
- Appointment availability standards increasingly enforced
- States may impose stricter requirements than federal minimums

## 8.8 Additional Regulatory Considerations

### No Surprises Act (2022)
- Requires accurate provider directories
- Patients cannot be balance-billed if they relied on an inaccurate directory
- Health plans must verify directory information every 90 days
- Increases importance of roster accuracy

### HIPAA
- Provider identifiers (NPI) mandated under HIPAA
- Standard transaction sets use NPI
- Privacy and security rules govern handling of provider and patient data on rosters
- SSN/DOB on roster files must be protected as PII

### Anti-Kickback Statute and Stark Law
- Relevant to network composition and referral patterns
- Enrollment and network participation cannot be conditioned on referral arrangements
- Impacts how organizations structure provider relationships

### False Claims Act
- Filing claims with incorrect provider information (wrong NPI, unlicensed provider, excluded provider) can constitute false claims
- Significant financial penalties and potential criminal liability
- Underscores importance of roster accuracy and ongoing monitoring

---

# SECTION 9: REFERENCE TABLES AND QUICK LOOKUPS

## 9.1 Identifier Quick Reference

| Identifier | Digits/Characters | Format | Issuer | Validation |
|-----------|-------------------|--------|--------|------------|
| NPI | 10 digits | NNNNNNNNNN | CMS/NPPES | Luhn check digit |
| TIN/EIN | 9 digits | NN-NNNNNNN | IRS | Prefix range check |
| SSN | 9 digits | NNN-NN-NNNN | SSA | Area/group/serial rules |
| DEA | 9 characters | AANNNNNNC | DEA | Check digit algorithm |
| CAQH ID | 7-8 digits | NNNNNNN(N) | CAQH | None published |
| CLIA | 10 characters | Alphanumeric | CMS | State prefix |
| Medicaid ID | Varies | State-specific | State Medicaid | State-specific |
| MBI | 11 characters | N-C-AN-N-C-AN-N-C-C-N-N | CMS | Position rules |
| Taxonomy | 10 characters | Alphanumeric + X | NUCC | Valid code lookup |
| UPIN (Legacy) | 6 characters | ANNNNN | CMS | Deprecated |

## 9.2 Primary Source Verification Quick Reference

| Credential | Primary Source | URL/Method | Verification Timeframe |
|-----------|---------------|------------|----------------------|
| NPI | NPPES Registry | npiregistry.cms.hhs.gov | Current |
| State Medical License | State Medical Board | State board website | Within 180 days |
| Board Certification (MD/DO) | ABMS/AOA | Certificationmatters.org / board websites | Within 180 days |
| DEA Registration | DEA/NTIS | Subscription service | Current |
| Education (MD) | AMA Physician Masterfile | AMA subscription | One-time |
| Education (DO) | AOA Physician Database | AOA subscription | One-time |
| Education (IMG) | ECFMG | ecfmg.org verification | One-time |
| Malpractice History | NPDB | npdb.hrsa.gov | Within 180 days |
| OIG Exclusion | OIG LEIE | exclusions.oig.hhs.gov | Monthly |
| SAM Exclusion | SAM.gov | sam.gov | Monthly |
| Medicare Sanctions | CMS | CMS preclusion list | Monthly |
| OFAC SDN | Treasury OFAC | sanctionssearch.ofac.treas.gov | Periodic |
| Hospital Privileges | Hospital directly | Direct contact | At credentialing |
| Malpractice Insurance | Insurance carrier | Direct contact | At credentialing |

## 9.3 Credentialing Timeline Reference

| Activity | Typical Duration | Regulatory Requirement |
|----------|-----------------|----------------------|
| Application collection | 1-4 weeks | — |
| PSV process | 2-8 weeks | — |
| Committee review | 1-4 weeks | Peer review required |
| Total initial credentialing | 60-180 days | Some states mandate maximum |
| Recredentialing cycle | Every 36 months | NCQA standard |
| Medicare revalidation | Every 5 years | CMS requirement |
| CAQH re-attestation | Every 120 days | CAQH requirement |
| OIG/SAM monitoring | Monthly | NCQA CR 4 |
| NPDB query validity | 180 days | NCQA |
| License verification validity | 180 days | NCQA |

## 9.4 CMS-855 Form Quick Reference

| Form | Provider Type | Key Use |
|------|-------------|---------|
| 855A | Institutional (hospital, SNF, HHA, hospice, ASC, ESRD) | Facility enrollment |
| 855B | Group practice, clinic, independent lab | Organizational enrollment |
| 855I | Individual physician, NP, PA, therapist, etc. | Individual provider enrollment |
| 855O | Physician/practitioner ordering/certifying only | Non-billing enrollment |
| 855R | Any enrolled individual provider | Reassign billing to organization |
| 855S | DMEPOS supplier | DME supplier enrollment |

## 9.5 Sanctions Screening Quick Reference

| Database | Managed By | Content | Access | Frequency |
|----------|-----------|---------|--------|-----------|
| LEIE | OIG/HHS | Excluded individuals/entities | Free online | Monthly minimum |
| SAM | GSA | Debarred/suspended entities | Free online | Monthly minimum |
| NPDB | HRSA | Malpractice, adverse actions | Fee-based; authorized users only | At credentialing + continuous query |
| OFAC SDN | Treasury | Sanctioned nationals | Free online | Periodic |
| CMS Preclusion List | CMS | Medicare-precluded prescribers | CMS distribution | Per update cycle |
| State Exclusion Lists | State Medicaid agencies | State-excluded providers | State-specific | Monthly where available |
| Medicare Opt-Out List | CMS | Providers who opted out of Medicare | CMS.gov | Periodic |

## 9.6 Common Roster Validation Rules

| Field | Validation Rule |
|-------|----------------|
| NPI | Exactly 10 digits; passes Luhn check; exists in NPPES; not deactivated |
| TIN/EIN | Exactly 9 digits; not all same digit; valid prefix range |
| DEA | 2 letters + 7 digits; passes DEA check digit; second letter matches last name initial |
| Taxonomy | Valid NUCC code; matches provider type; 10 characters |
| State License | Valid format for specified state; not expired; active status |
| Phone/Fax | 10 digits; valid area code |
| ZIP Code | Valid 5-digit or ZIP+4; matches city/state |
| Effective Date | Valid date format; not in distant future; before termination date |
| Termination Date | Valid date format; after effective date; or null/blank for active |
| Email | Valid email format |
| Address | Non-PO Box for practice location (some plans); USPS-standardized |

---

# SECTION 10: GLOSSARY OF KEY TERMS

| Term | Definition |
|------|-----------|
| **Attestation** | Provider's formal statement that all information provided is accurate and complete |
| **Board Eligible** | Provider has completed residency/fellowship training and is eligible to sit for board exams but has not yet passed |
| **Board Certified** | Provider has passed specialty board examination(s) |
| **CAH** | Critical Access Hospital — small, rural hospital with special Medicare designation |
| **CAQH** | Council for Affordable Quality Health Care — manages ProView credentialing platform |
| **CMS** | Centers for Medicare & Medicaid Services — federal agency overseeing Medicare, Medicaid, ACA marketplace |
| **CoP** | Conditions of Participation — federal standards for Medicare/Medicaid participation |
| **Covered Entity** | Under HIPAA: health plan, healthcare clearinghouse, or healthcare provider that transmits health information electronically |
| **Credentialing** | Process of verifying a provider's qualifications, training, licensure, and professional standing |
| **CVO** | Credentials Verification Organization — entity that performs PSV on behalf of others |
| **Delegated Credentialing** | Health plan authorizes another entity to perform credentialing on its behalf |
| **DMEPOS** | Durable Medical Equipment, Prosthetics, Orthotics, and Supplies |
| **ECP** | Essential Community Provider — provider serving predominantly low-income or medically underserved populations |
| **FPPE** | Focused Professional Practice Evaluation — monitoring period for newly privileged practitioners |
| **HPMS** | Health Plan Management System — CMS system for Medicare Advantage plan data |
| **IMG** | International Medical Graduate — physician who graduated from a medical school outside the U.S. |
| **LOB** | Line of Business — product type (Commercial, Medicare, Medicaid, etc.) |
| **MAC** | Medicare Administrative Contractor — entities that process Medicare claims by region |
| **MCO** | Managed Care Organization — entity that manages healthcare delivery under contract with Medicaid or other programs |
| **MOC** | Maintenance of Certification — ongoing requirements to keep board certification active |
| **NCQA** | National Committee for Quality Assurance — accreditation organization for health plans |
| **NPDB** | National Practitioner Data Bank — federal repository of malpractice payments and adverse actions |
| **NPPES** | National Plan and Provider Enumeration System — CMS system that issues and manages NPIs |
| **OPPE** | Ongoing Professional Practice Evaluation — continuous monitoring of practitioner performance |
| **OTP** | Optimal Team Practice — AAPA model for PA practice without required physician supervision |
| **PECOS** | Provider Enrollment, Chain, and Ownership System — CMS Medicare enrollment system |
| **Privileging** | Process of granting specific clinical permissions to a provider at a facility |
| **PSV** | Primary Source Verification — verifying credentials directly from the issuing body |
| **Recredentialing** | Periodic re-verification of all credentials (typically every 3 years) |
| **Revalidation** | Medicare-specific: periodic re-verification of Medicare enrollment (every 5 years) |
| **Roster** | Structured data file listing providers in a health plan network with associated details |
| **Taxonomy Code** | NUCC-maintained code classifying healthcare provider type and specialty |
| **URAC** | Utilization Review Accreditation Commission — accreditation body |

---

# SECTION 11: COMMON SCENARIOS FOR ROSTER ANALYSTS

## 11.1 Provider Leaves a Practice
- Termination date should be set on the old practice roster record
- New roster record created at new practice (if staying in network)
- NPI remains the same (it's permanent)
- TIN changes to new group's TIN
- CAQH profile must be updated with new practice information
- May need new DEA registration if changing states
- State license remains valid (if same state); new license needed for new state

## 11.2 Provider Joins Multiple Practices
- Multiple roster rows: one per practice location per network
- Same Type 1 NPI on all rows
- Different Type 2 (group) NPIs and TINs on each row
- Different practice addresses
- Provider must ensure each group files CMS-855R for Medicare reassignment

## 11.3 Provider Gets Credentialed — Enrollment Flow
1. Application received (CAQH ProView or direct)
2. PSV completed and clean
3. Credentialing committee approves
4. Participation agreement (contract) executed
5. Provider loaded into payor system with effective date
6. Provider appears on roster file
7. Provider listed in directory
8. Claims processing begins from effective date

## 11.4 Sanction Detected During Monitoring
1. Monthly OIG/SAM screening identifies a match
2. Verification of match (not a false positive) — confirm NPI, name, DOB
3. If confirmed: immediate notification to credentialing committee/medical director
4. Provider may be immediately suspended from network
5. Claims review for any services after exclusion date
6. Refund/recoupment of any payments made after exclusion
7. Report to appropriate authorities if applicable
8. Termination from network if exclusion not resolved

## 11.5 Roster File Reconciliation
- Compare submitted roster against internal credentialing database
- Flag providers on roster who are not credentialed
- Flag credentialed providers missing from roster
- Validate all identifiers (NPI check digit, DEA format, etc.)
- Check for expired licenses, certifications
- Verify effective dates are logical
- Check for duplicate records
- Standardize addresses against USPS
- Validate taxonomy codes against NUCC
- Cross-reference NPI against NPPES for accuracy

---

*End of Knowledge Base*
