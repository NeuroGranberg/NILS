import type { AnonymizeCategory } from '../../types';

export interface AnonymizeCategoryOption {
  value: AnonymizeCategory;
  label: string;
  description?: string;
  caution?: boolean;
}

export interface CategoryTagDefinition {
  code: string;
  name: string;
}

export const ANONYMIZE_CATEGORY_OPTIONS: AnonymizeCategoryOption[] = [
  {
    value: 'Patient_Information',
    label: 'Patient Information',
    description: 'Names, demographics, identifiers, contact details.',
  },
  {
    value: 'Time_And_Date_Information',
    label: 'Time & Date',
    description: 'Acquisition/scan timestamps and scheduling data.',
    caution: true,
  },
  {
    value: 'Clinical_Trial_Information',
    label: 'Clinical Trial',
    description: 'Sponsor, protocol, site, subject identifiers.',
  },
  {
    value: 'Healthcare_Provider_Information',
    label: 'Healthcare Provider',
    description: 'Physician names, operators, procedure metadata.',
  },
  {
    value: 'Institution_Information',
    label: 'Institution',
    description: 'Facility, station, and department information.',
  },
];

const formatTagCode = (group: number, element: number) =>
  `${group.toString(16).toUpperCase().padStart(4, '0')},${element
    .toString(16)
    .toUpperCase()
    .padStart(4, '0')}`;

const patientInformationTags: CategoryTagDefinition[] = [
  { code: formatTagCode(0x0010, 0x0010), name: "Patient's Name" },
  { code: formatTagCode(0x0010, 0x0030), name: "Patient's Birth Date" },
  { code: formatTagCode(0x0010, 0x0032), name: "Patient's Birth Time" },
  { code: formatTagCode(0x0010, 0x0040), name: "Patient's Sex" },
  { code: formatTagCode(0x0010, 0x1010), name: "Patient's Age" },
  { code: formatTagCode(0x0010, 0x1020), name: "Patient's Size" },
  { code: formatTagCode(0x0010, 0x1030), name: "Patient's Weight" },
  { code: formatTagCode(0x0010, 0x1040), name: "Patient's Address" },
  { code: formatTagCode(0x0010, 0x0050), name: 'Insurance Plan Identification' },
  { code: formatTagCode(0x0010, 0x1090), name: 'Medical Record Locator' },
  { code: formatTagCode(0x0010, 0x1080), name: 'Military Rank' },
  { code: formatTagCode(0x0010, 0x1081), name: 'Branch of Service' },
  { code: formatTagCode(0x0010, 0x2000), name: 'Medical Alerts' },
  { code: formatTagCode(0x0010, 0x2110), name: 'Allergies' },
  { code: formatTagCode(0x0010, 0x2150), name: 'Country of Residence' },
  { code: formatTagCode(0x0010, 0x2152), name: 'Region of Residence' },
  { code: formatTagCode(0x0010, 0x2154), name: "Patient's Telephone Numbers" },
  { code: formatTagCode(0x0010, 0x2160), name: 'Ethnic Group' },
  { code: formatTagCode(0x0010, 0x2180), name: 'Occupation' },
  { code: formatTagCode(0x0010, 0x21A0), name: 'Smoking Status' },
  { code: formatTagCode(0x0010, 0x21B0), name: 'Additional Patient History' },
  { code: formatTagCode(0x0010, 0x21C0), name: 'Pregnancy Status' },
  { code: formatTagCode(0x0010, 0x21D0), name: 'Last Menstrual Date' },
  { code: formatTagCode(0x0010, 0x1000), name: 'Other Patient IDs' },
  { code: formatTagCode(0x0010, 0x1001), name: 'Other Patient Names' },
  { code: formatTagCode(0x0010, 0x1002), name: 'Other Patient IDs Sequence' },
  { code: formatTagCode(0x0010, 0x0050), name: 'Patient Insurance Plan Code Sequence' },
  { code: formatTagCode(0x0010, 0x0101), name: 'Patient Primary Language Code Sequence' },
  { code: formatTagCode(0x0010, 0x0102), name: 'Patient Primary Language Modifier Code Sequence' },
  { code: formatTagCode(0x0010, 0x2297), name: 'Responsible Person' },
  { code: formatTagCode(0x0010, 0x2298), name: 'Responsible Person Role' },
  { code: formatTagCode(0x0010, 0x0021), name: 'Issuer of Patient ID' },
  { code: formatTagCode(0x0010, 0x1060), name: "Patient's Mother's Birth Name" },
  { code: formatTagCode(0x0010, 0x21F0), name: "Patient's Religious Preference" },
  { code: formatTagCode(0x0010, 0x4000), name: 'Patient Comments' },
];

const clinicalTrialTags: CategoryTagDefinition[] = [
  { code: formatTagCode(0x0012, 0x0010), name: 'Clinical Trial Sponsor Name' },
  { code: formatTagCode(0x0012, 0x0020), name: 'Clinical Trial Protocol ID' },
  { code: formatTagCode(0x0012, 0x0021), name: 'Clinical Trial Protocol Name' },
  { code: formatTagCode(0x0012, 0x0030), name: 'Clinical Trial Site ID' },
  { code: formatTagCode(0x0012, 0x0031), name: 'Clinical Trial Site Name' },
  { code: formatTagCode(0x0012, 0x0040), name: 'Clinical Trial Subject ID' },
  { code: formatTagCode(0x0012, 0x0042), name: 'Clinical Trial Subject Reading ID' },
  { code: formatTagCode(0x0012, 0x0050), name: 'Clinical Trial Time Point ID' },
  { code: formatTagCode(0x0012, 0x0051), name: 'Clinical Trial Time Point Description' },
  { code: formatTagCode(0x0012, 0x0060), name: 'Clinical Trial Coordinating Center Name' },
  { code: formatTagCode(0x0012, 0x0071), name: 'Clinical Trial Series ID' },
  { code: formatTagCode(0x0012, 0x0072), name: 'Clinical Trial Series Description' },
  { code: formatTagCode(0x0012, 0x0081), name: 'Clinical Trial Protocol Ethics Committee Name' },
  { code: formatTagCode(0x0012, 0x0082), name: 'Clinical Trial Protocol Ethics Committee Approval Number' },
  { code: formatTagCode(0x0012, 0x0083), name: 'Clinical Trial Protocol Ethics Committee Approval Date' },
  { code: formatTagCode(0x0012, 0x0084), name: 'Clinical Trial Protocol Ethics Committee Contact' },
  { code: formatTagCode(0x0012, 0x0085), name: 'Clinical Trial Subject Consent Indicator' },
  { code: formatTagCode(0x0012, 0x0086), name: 'Clinical Trial Data Processing Protocol' },
  { code: formatTagCode(0x0012, 0x0087), name: 'Clinical Trial Data Processing Description' },
  { code: formatTagCode(0x0012, 0x0022), name: 'Clinical Trial Protocol ID Sequence' },
  { code: formatTagCode(0x0012, 0x0023), name: 'Clinical Trial Protocol Name Sequence' },
  { code: formatTagCode(0x0012, 0x0032), name: 'Clinical Trial Site ID Sequence' },
  { code: formatTagCode(0x0012, 0x0033), name: 'Clinical Trial Site Name Sequence' },
  { code: formatTagCode(0x0012, 0x0043), name: 'Clinical Trial Subject ID Sequence' },
  { code: formatTagCode(0x0012, 0x0044), name: 'Clinical Trial Subject Reading ID Sequence' },
  { code: formatTagCode(0x0012, 0x0052), name: 'Clinical Trial Time Point ID Sequence' },
  { code: formatTagCode(0x0012, 0x0053), name: 'Clinical Trial Time Point Description Sequence' },
  { code: formatTagCode(0x0012, 0x0061), name: 'Clinical Trial Coordinating Center Name Sequence' },
  { code: formatTagCode(0x0012, 0x0073), name: 'Clinical Trial Series ID Sequence' },
  { code: formatTagCode(0x0012, 0x0074), name: 'Clinical Trial Series Description Sequence' },
  { code: formatTagCode(0x0012, 0x0088), name: 'Clinical Trial Protocol Ethics Committee Name Sequence' },
  { code: formatTagCode(0x0012, 0x0089), name: 'Clinical Trial Protocol Ethics Committee Approval Number Sequence' },
  { code: formatTagCode(0x0012, 0x0090), name: 'Clinical Trial Protocol Ethics Committee Approval Date Sequence' },
  { code: formatTagCode(0x0012, 0x0091), name: 'Clinical Trial Protocol Ethics Committee Contact Sequence' },
];

const healthcareProviderTags: CategoryTagDefinition[] = [
  { code: formatTagCode(0x0008, 0x0090), name: "Referring Physician's Name" },
  { code: formatTagCode(0x0008, 0x0092), name: "Referring Physician's Address" },
  { code: formatTagCode(0x0008, 0x0094), name: "Referring Physician's Telephone Numbers" },
  { code: formatTagCode(0x0008, 0x0096), name: 'Referring Physician Identification Sequence' },
  { code: formatTagCode(0x0008, 0x1060), name: "Consulting Physician's Name" },
  { code: formatTagCode(0x0008, 0x106E), name: 'Consulting Physician Identification Sequence' },
  { code: formatTagCode(0x0008, 0x1048), name: "Physician(s) of Record" },
  { code: formatTagCode(0x0008, 0x1049), name: "Physician(s) of Record Identification Sequence" },
  { code: formatTagCode(0x0008, 0x1060), name: "Physician(s) Reading Study" },
  { code: formatTagCode(0x0008, 0x1062), name: "Physician(s) Reading Study Identification Sequence" },
  { code: formatTagCode(0x0008, 0x1050), name: "Performing Physician's Name" },
  { code: formatTagCode(0x0008, 0x1052), name: 'Performing Physician Identification Sequence' },
  { code: formatTagCode(0x0008, 0x1070), name: "Operators' Name" },
  { code: formatTagCode(0x0008, 0x1072), name: 'Operator Identification Sequence' },
  { code: formatTagCode(0x0008, 0x2111), name: 'Derivation Description' },
  { code: formatTagCode(0x0008, 0x1080), name: 'Admitting Diagnoses Description' },
  { code: formatTagCode(0x0008, 0x1120), name: 'Referenced Patient Sequence' },
  { code: formatTagCode(0x0008, 0x1110), name: 'Referenced Study Sequence' },
  { code: formatTagCode(0x0008, 0x1125), name: 'Referenced Visit Sequence' },
  { code: formatTagCode(0x0008, 0x1115), name: 'Referenced Series Sequence' },
  { code: formatTagCode(0x0008, 0x1140), name: 'Referenced Image Sequence' },
  { code: formatTagCode(0x0018, 0x1007), name: "Technician's Name" },
  { code: formatTagCode(0x0032, 0x1032), name: 'Requesting Physician' },
  { code: formatTagCode(0x0032, 0x1033), name: 'Requesting Physician Identification Sequence' },
  { code: formatTagCode(0x0032, 0x1060), name: 'Requested Procedure Description' },
  { code: formatTagCode(0x0040, 0x1001), name: 'Requested Procedure ID' },
  { code: formatTagCode(0x0040, 0x0007), name: 'Scheduled Procedure Step Description' },
  { code: formatTagCode(0x0040, 0x0009), name: 'Scheduled Procedure Step ID' },
  { code: formatTagCode(0x0040, 0x0006), name: "Scheduled Performing Physician's Name" },
  { code: formatTagCode(0x0040, 0x000B), name: 'Scheduled Performing Physician Identification Sequence' },
  { code: formatTagCode(0x0040, 0x0254), name: 'Performed Procedure Step Description' },
  { code: formatTagCode(0x0040, 0x0253), name: 'Performed Procedure Step ID' },
  { code: formatTagCode(0x0040, 0xA075), name: 'Verifying Physician Name' },
  { code: formatTagCode(0x0040, 0xA073), name: 'Verifying Observer Sequence' },
  { code: formatTagCode(0x0040, 0x0275), name: 'Request Attributes Sequence' },
  { code: formatTagCode(0x0040, 0x0260), name: 'Performed Protocol Code Sequence' },
  { code: formatTagCode(0x0040, 0xA730), name: 'Content Sequence' },
  { code: formatTagCode(0x0040, 0x1102), name: "Person's Address" },
  { code: formatTagCode(0x0040, 0x1103), name: "Person's Telephone Numbers" },
  { code: formatTagCode(0x0040, 0x1104), name: "Person's Telecom Information" },
  { code: formatTagCode(0x0400, 0x0561), name: 'Original Attributes Sequence' },
  { code: formatTagCode(0x0040, 0x1002), name: 'Reason for the Requested Procedure' },
  { code: formatTagCode(0x0040, 0x1400), name: 'Requested Procedure Comments' },
  { code: formatTagCode(0x0070, 0x0084), name: "Content Creator's Name" },
  { code: formatTagCode(0x0070, 0x0086), name: "Content Creator's Identification Code Sequence" },
];

const institutionTags: CategoryTagDefinition[] = [
  { code: formatTagCode(0x0008, 0x1010), name: 'Station Name' },
  { code: formatTagCode(0x0008, 0x0080), name: 'Institution Name' },
  { code: formatTagCode(0x0008, 0x0081), name: 'Institution Address' },
  { code: formatTagCode(0x0008, 0x1040), name: 'Institutional Department Name' },
  { code: formatTagCode(0x0008, 0x1041), name: 'Institutional Department Type Code Sequence' },
];

const timeAndDateTags: CategoryTagDefinition[] = [
  { code: formatTagCode(0x0008, 0x0021), name: 'Series Date' },
  { code: formatTagCode(0x0008, 0x0022), name: 'Acquisition Date' },
  { code: formatTagCode(0x0008, 0x0023), name: 'Content Date' },
  { code: formatTagCode(0x0008, 0x0030), name: 'Study Time' },
  { code: formatTagCode(0x0008, 0x0031), name: 'Series Time' },
  { code: formatTagCode(0x0008, 0x0032), name: 'Acquisition Time' },
  { code: formatTagCode(0x0008, 0x0033), name: 'Content Time' },
  { code: formatTagCode(0x0008, 0x0012), name: 'Instance Creation Date' },
  { code: formatTagCode(0x0008, 0x0013), name: 'Instance Creation Time' },
  { code: formatTagCode(0x0032, 0x1050), name: 'Study Completion Date' },
  { code: formatTagCode(0x0032, 0x1051), name: 'Study Completion Time' },
  { code: formatTagCode(0x0040, 0x0244), name: 'Performed Procedure Step Start Date' },
  { code: formatTagCode(0x0040, 0x0245), name: 'Performed Procedure Step Start Time' },
  { code: formatTagCode(0x0040, 0x0250), name: 'Performed Procedure Step End Date' },
  { code: formatTagCode(0x0040, 0x0251), name: 'Performed Procedure Step End Time' },
  { code: formatTagCode(0x0040, 0x2004), name: 'Issue Date of Imaging Service Request' },
  { code: formatTagCode(0x0040, 0x2005), name: 'Issue Time of Imaging Service Request' },
  { code: formatTagCode(0x0040, 0xA030), name: 'Verification DateTime' },
  { code: formatTagCode(0x0040, 0xA032), name: 'Observation DateTime' },
];

export const CATEGORY_TAGS: Record<AnonymizeCategory, CategoryTagDefinition[]> = {
  Patient_Information: patientInformationTags,
  Clinical_Trial_Information: clinicalTrialTags,
  Healthcare_Provider_Information: healthcareProviderTags,
  Institution_Information: institutionTags,
  Time_And_Date_Information: timeAndDateTags,
};

export const TIME_AND_DATE_CODES = timeAndDateTags.map((tag) => tag.code);

const TAG_LABEL_LOOKUP: Record<string, string> = Object.fromEntries(
  Object.values(CATEGORY_TAGS)
    .flat()
    .map((tag) => [tag.code, tag.name]),
);

export const ALL_CATEGORY_TAG_CODES = Object.values(CATEGORY_TAGS)
  .flat()
  .map((tag) => tag.code);

const normalizeTagKey = (tag: string) => {
  const cleaned = tag.trim().replace(/[^0-9a-fA-F]/g, '').toUpperCase();
  if (cleaned.length === 8) {
    return `${cleaned.slice(0, 4)},${cleaned.slice(4)}`;
  }
  if (/^[0-9A-F]{4},[0-9A-F]{4}$/u.test(tag.trim())) {
    return tag.trim().toUpperCase();
  }
  return tag.trim();
};

export const resolveTagLabel = (tag: string) => {
  const key = normalizeTagKey(tag);
  const label = TAG_LABEL_LOOKUP[key];
  return label ? `${label} (${key})` : key;
};
