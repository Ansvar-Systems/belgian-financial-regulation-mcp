#!/usr/bin/env npx tsx
/**
 * FSMA / NBB Regulatory Ingestion Crawler
 *
 * Crawls the FSMA website (fsma.be) to populate the belgian-financial-regulation-mcp
 * database with circulars, communications, guidance, regulations, and enforcement
 * actions published by the Financial Services and Markets Authority (FSMA) and
 * the National Bank of Belgium (NBB).
 *
 * Data sources:
 *   - Supervised entity category pages (circulars, communications, laws per sector)
 *   - Warnings archive pages (consumer fraud warnings)
 *   - Administrative sanctions search results
 *   - PDF documents linked from category pages
 *
 * Content languages: French (primary), Dutch, English.
 *
 * Usage:
 *   npx tsx scripts/ingest-fsma.ts                  # full crawl
 *   npx tsx scripts/ingest-fsma.ts --resume         # skip already-ingested references
 *   npx tsx scripts/ingest-fsma.ts --dry-run        # crawl without writing to DB
 *   npx tsx scripts/ingest-fsma.ts --force          # drop existing data first
 *   npx tsx scripts/ingest-fsma.ts --resume --dry-run
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["FSMA_DB_PATH"] ?? "data/fsma.db";
const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 2_000;
const REQUEST_TIMEOUT_MS = 30_000;

const USER_AGENT =
  "AnsvarFSMACrawler/1.0 (+https://ansvar.eu; compliance research)";

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

function log(msg: string): void {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
}

function warn(msg: string): void {
  const ts = new Date().toISOString();
  console.warn(`[${ts}] WARN: ${msg}`);
}

function error(msg: string): void {
  const ts = new Date().toISOString();
  console.error(`[${ts}] ERROR: ${msg}`);
}

// ---------------------------------------------------------------------------
// Stats tracker
// ---------------------------------------------------------------------------

interface Stats {
  pagesVisited: number;
  provisionsInserted: number;
  provisionsSkipped: number;
  enforcementsInserted: number;
  enforcementsSkipped: number;
  errors: number;
}

const stats: Stats = {
  pagesVisited: 0,
  provisionsInserted: 0,
  provisionsSkipped: 0,
  enforcementsInserted: 0,
  enforcementsSkipped: 0,
  errors: 0,
};

// ---------------------------------------------------------------------------
// HTTP fetch with retry
// ---------------------------------------------------------------------------

async function fetchWithRetry(
  url: string,
  retries = MAX_RETRIES,
): Promise<string> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort(),
        REQUEST_TIMEOUT_MS,
      );

      const resp = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,*/*",
          "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,en;q=0.7",
        },
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
      }

      return await resp.text();
    } catch (err) {
      const isLast = attempt === retries;
      const msg = err instanceof Error ? err.message : String(err);
      if (isLast) {
        throw new Error(
          `Failed to fetch ${url} after ${retries} attempts: ${msg}`,
        );
      }
      const backoff = RETRY_BACKOFF_MS * attempt;
      warn(
        `Attempt ${attempt}/${retries} failed for ${url}: ${msg} — retrying in ${backoff}ms`,
      );
      await sleep(backoff);
    }
  }
  // unreachable, but satisfies TS
  throw new Error(`Failed to fetch ${url}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rateLimitedFetch(url: string): Promise<string> {
  const html = await fetchWithRetry(url);
  stats.pagesVisited++;
  await sleep(RATE_LIMIT_MS);
  return html;
}

// ---------------------------------------------------------------------------
// Database setup
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

// ---------------------------------------------------------------------------
// Sourcebook definitions
// ---------------------------------------------------------------------------

interface SourcebookDef {
  id: string;
  name: string;
  description: string;
}

const SOURCEBOOKS: SourcebookDef[] = [
  {
    id: "FSMA_CIRCULAIRES",
    name: "FSMA Circulaires",
    description:
      "Circulaires publiées par l'Autorité des services et marchés financiers (FSMA). Couvrent la gouvernance, la gestion des risques ICT, la protection des investisseurs, les exigences MiFID II, et les obligations de conformité pour les établissements financiers belges.",
  },
  {
    id: "FSMA_COMMUNICATIONS",
    name: "FSMA Communications",
    description:
      "Communications officielles et orientations pratiques de la FSMA. Comprend les communications sur la lutte contre le blanchiment de capitaux et le financement du terrorisme (LBC/FT), la protection des consommateurs, et les orientations sectorielles.",
  },
  {
    id: "FSMA_REGLEMENTS",
    name: "FSMA Règlements",
    description:
      "Règlements adoptés par la FSMA en vertu de ses compétences réglementaires. Couvrent la distribution de produits financiers dérivés, les obligations de transparence, et la gouvernance des produits.",
  },
  {
    id: "NBB_CIRCULAIRES",
    name: "NBB Circulaires",
    description:
      "Circulaires prudentielles publiées par la Banque Nationale de Belgique (NBB). Couvrent la résilience opérationnelle, l'externalisation, les fonds propres, les exigences de liquidité, et la gestion des risques pour les établissements de crédit.",
  },
  {
    id: "NBB_GUIDANCE",
    name: "NBB Guidance",
    description:
      "Orientations et recommandations prudentielles de la Banque Nationale de Belgique. Précisions sur l'application des exigences réglementaires, attentes de surveillance, et mise en oeuvre de DORA.",
  },
  {
    id: "FSMA_WARNINGS",
    name: "FSMA Warnings",
    description:
      "Avertissements publiés par la FSMA à l'attention du public concernant des entreprises non autorisées, des plateformes de trading frauduleuses, et des pratiques commerciales trompeuses.",
  },
  {
    id: "FSMA_SANCTIONS",
    name: "FSMA Sanctions",
    description:
      "Décisions de sanctions administratives prononcées par le comité de sanctions de la FSMA. Amendes, avertissements, et interdictions d'activité pour violation de la réglementation financière belge.",
  },
];

// ---------------------------------------------------------------------------
// Category pages to crawl
// ---------------------------------------------------------------------------

/** Each category page contains circulars, communications, and laws for a sector. */
interface CategoryPage {
  url: string;
  lang: "fr" | "nl" | "en";
  label: string;
}

const CATEGORY_PAGES: CategoryPage[] = [
  // French pages (primary regulatory language)
  {
    url: "https://www.fsma.be/fr/circulaires-et-communications",
    lang: "fr",
    label: "FSMA Circulaires et communications (FR)",
  },
  {
    url: "https://www.fsma.be/fr/etablissement-de-credit",
    lang: "fr",
    label: "Établissements de crédit (FR)",
  },
  {
    url: "https://www.fsma.be/fr/intermediaire-en-assurances",
    lang: "fr",
    label: "Intermédiaires en assurances (FR)",
  },
  {
    url: "https://www.fsma.be/fr/intermediaire-en-services-bancaires-et-dinvestissement",
    lang: "fr",
    label: "Intermédiaires en services bancaires et d'investissement (FR)",
  },
  {
    url: "https://www.fsma.be/fr/intermediaire-en-reassurance",
    lang: "fr",
    label: "Intermédiaires en réassurance (FR)",
  },
  {
    url: "https://www.fsma.be/fr/intermediaire-en-credit-hypothecaire",
    lang: "fr",
    label: "Intermédiaires en crédit hypothécaire (FR)",
  },
  {
    url: "https://www.fsma.be/fr/intermediaire-en-credit-la-consommation",
    lang: "fr",
    label: "Intermédiaires en crédit à la consommation (FR)",
  },
  {
    url: "https://www.fsma.be/fr/entreprise-dassurance-ou-de-reassurance",
    lang: "fr",
    label: "Entreprises d'assurance ou de réassurance (FR)",
  },
  {
    url: "https://www.fsma.be/fr/institution-de-retraite-professionnelle-irp",
    lang: "fr",
    label: "Institutions de retraite professionnelle (FR)",
  },
  {
    url: "https://www.fsma.be/fr/societe-de-gestion-dorganismes-de-placement-collectif",
    lang: "fr",
    label: "Sociétés de gestion d'OPC (FR)",
  },

  // Dutch pages
  {
    url: "https://www.fsma.be/nl/circulaires-en-mededelingen",
    lang: "nl",
    label: "FSMA Circulaires en mededelingen (NL)",
  },

  // English pages
  {
    url: "https://www.fsma.be/en/circulars-and-communications",
    lang: "en",
    label: "FSMA Circulars and communications (EN)",
  },
  {
    url: "https://www.fsma.be/en/credit-institution",
    lang: "en",
    label: "Credit institutions (EN)",
  },
  {
    url: "https://www.fsma.be/en/intermediary-insurance",
    lang: "en",
    label: "Insurance intermediaries (EN)",
  },
  {
    url: "https://www.fsma.be/en/intermediary-banking-and-investment-services",
    lang: "en",
    label: "Banking and investment intermediaries (EN)",
  },
  {
    url: "https://www.fsma.be/en/reinsurance-company",
    lang: "en",
    label: "(Re)insurance companies (EN)",
  },
  {
    url: "https://www.fsma.be/en/institution-occupational-retirement-provision-iorp",
    lang: "en",
    label: "Occupational retirement provisions (EN)",
  },
];

// ---------------------------------------------------------------------------
// Warning archive pages
// ---------------------------------------------------------------------------

const WARNING_ARCHIVE_PAGES = [
  "https://www.fsma.be/en/warnings/archive?page=0",
  "https://www.fsma.be/en/warnings/archive?page=1",
  "https://www.fsma.be/fr/mises-en-garde/archive?page=0",
  "https://www.fsma.be/fr/mises-en-garde/archive?page=1",
  "https://www.fsma.be/fr/mises-en-garde/archive?page=2",
];

const WARNING_CURRENT_PAGES = [
  "https://www.fsma.be/en/warnings",
  "https://www.fsma.be/fr/mises-en-garde",
];

// ---------------------------------------------------------------------------
// Helpers: reference extraction
// ---------------------------------------------------------------------------

/**
 * Extract FSMA/NBB/CBFA reference number from a title or filename.
 * Patterns: FSMA_2024_06, CBFA_2009_17, NBB_2022_05, CTR_2019_01, CSR_2018_01
 */
function extractReference(text: string): string | null {
  // Standard references: FSMA_YYYY_NN, CBFA_YYYY_NN, NBB_YYYY_NN
  const stdMatch = text.match(
    /\b((?:FSMA|CBFA|NBB|CTR|CSR)[_-]\d{4}[_-]\d{2,3}(?:[_-]\w+)?)\b/i,
  );
  if (stdMatch?.[1]) {
    return stdMatch[1].replace(/-/g, "_").toUpperCase();
  }

  // Try extracting from PDF filenames
  const fileMatch = text.match(
    /\/((?:fsma|cbfa|nbb|ctr|csr)[_-]\d{4}[_-]\d{2,3}[^./]*?)(?:_(?:fr|nl|en)(?:_\d+)?)?\.pdf/i,
  );
  if (fileMatch?.[1]) {
    return fileMatch[1].replace(/-/g, "_").toUpperCase();
  }

  return null;
}

/**
 * Determine sourcebook from reference and document type.
 */
function classifySourcebook(
  reference: string,
  type: string,
  title: string,
): string {
  const ref = reference.toUpperCase();

  if (ref.startsWith("NBB_GUIDE") || type === "guidance") {
    return "NBB_GUIDANCE";
  }
  if (ref.startsWith("NBB_") || ref.startsWith("CBF_")) {
    return "NBB_CIRCULAIRES";
  }

  const lowerTitle = title.toLowerCase();

  if (type === "communication" || lowerTitle.includes("communication")) {
    return "FSMA_COMMUNICATIONS";
  }
  if (type === "règlement" || type === "regulation" || lowerTitle.includes("règlement")) {
    return "FSMA_REGLEMENTS";
  }

  // CSR/CTR refs are from the Commission des sanctions / oversight board
  if (ref.startsWith("CTR_") || ref.startsWith("CSR_")) {
    return "FSMA_COMMUNICATIONS";
  }

  // CBFA was the predecessor of FSMA
  if (ref.startsWith("CBFA_")) {
    return "FSMA_CIRCULAIRES";
  }

  return "FSMA_CIRCULAIRES";
}

/**
 * Determine document type from title and context.
 */
function classifyType(title: string, url: string): string {
  const lower = title.toLowerCase();
  if (lower.includes("circulaire") || lower.includes("circular")) return "circulaire";
  if (lower.includes("communication") || lower.includes("mededeling")) return "communication";
  if (lower.includes("règlement") || lower.includes("regulation") || lower.includes("reglement")) return "regulation";
  if (lower.includes("orientation") || lower.includes("guidance") || lower.includes("guideline")) return "guidance";
  if (lower.includes("handbook") || lower.includes("vademecum") || lower.includes("questionnaire")) return "handbook";
  if (lower.includes("newsletter") || lower.includes("nieuwsbrief")) return "newsletter";
  if (lower.includes("décision") || lower.includes("beslissing") || lower.includes("decision")) return "decision";
  if (lower.includes("avertissement") || lower.includes("warning") || lower.includes("waarschuwing")) return "warning";
  if (lower.includes("loi") || lower.includes("law") || lower.includes("wet")) return "law";

  // Infer from URL path
  if (url.includes("/circ/")) return "circulaire";
  if (url.includes("/comm/")) return "communication";

  return "circulaire";
}

/**
 * Generate a stable reference for items without an official one.
 */
function generateReference(title: string, date: string, index: number): string {
  const slug = title
    .replace(/[^a-zA-Z0-9\s]/g, "")
    .trim()
    .split(/\s+/)
    .slice(0, 4)
    .join("_")
    .toUpperCase()
    .substring(0, 40);
  const datePart = date.replace(/-/g, "");
  return `FSMA_AUTO_${datePart}_${index}_${slug}`;
}

/**
 * Parse date from DD/MM/YYYY format to YYYY-MM-DD.
 */
function parseDate(dateStr: string): string | null {
  // DD/MM/YYYY
  const dmy = dateStr.match(/(\d{2})\/(\d{2})\/(\d{4})/);
  if (dmy) {
    return `${dmy[3]}-${dmy[2]}-${dmy[1]}`;
  }
  // YYYY-MM-DD already
  const iso = dateStr.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (iso) {
    return dateStr;
  }
  // YYYY-MM
  const ym = dateStr.match(/(\d{4})-(\d{2})/);
  if (ym) {
    return `${ym[1]}-${ym[2]}-01`;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Crawlers
// ---------------------------------------------------------------------------

interface ProvisionRecord {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string | null;
  chapter: string | null;
  section: string | null;
}

interface EnforcementRecord {
  firm_name: string;
  reference_number: string | null;
  action_type: string;
  amount: number;
  date: string | null;
  summary: string;
  sourcebook_references: string | null;
}

/**
 * Crawl a category page and extract linked regulatory documents.
 *
 * FSMA category pages list documents grouped under headings (Circulars,
 * Communications, Laws, ESA Guidelines, etc.). Each entry has a date,
 * a linked title (pointing to a PDF or detail page), and sometimes a
 * reference number embedded in the title or filename.
 */
async function crawlCategoryPage(
  page: CategoryPage,
): Promise<ProvisionRecord[]> {
  log(`Crawling category page: ${page.label}`);

  let html: string;
  try {
    html = await rateLimitedFetch(page.url);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    error(`Failed to fetch ${page.url}: ${msg}`);
    stats.errors++;
    return [];
  }

  const $ = cheerio.load(html);
  const provisions: ProvisionRecord[] = [];
  let autoIndex = 0;

  // The FSMA site uses Drupal. Content is in the main article/content area.
  // Documents are listed as links, often preceded by dates.
  // We look for all links to PDF files and detail pages within the main content.
  const mainContent = $("main, .main-content, article, .field--name-body, .node__content, #block-fsma-content");
  const contentEl = mainContent.length > 0 ? mainContent : $("body");

  // Strategy 1: Find all PDF links with associated text
  contentEl.find('a[href$=".pdf"], a[href$=".PDF"]').each((_i, el) => {
    const $a = $(el);
    const href = $a.attr("href") ?? "";
    const linkText = $a.text().trim();

    if (!linkText || linkText.length < 5) return;

    // Skip navigation, footer, and non-regulatory links
    if (href.includes("/logo") || href.includes("/css/") || href.includes("/js/")) return;

    // Build full URL
    const fullUrl = href.startsWith("http")
      ? href
      : `https://www.fsma.be${href}`;

    // Try to find the date near this element
    let dateStr: string | null = null;
    const parentText = $a.parent().text();
    const dateMatch = parentText.match(/(\d{2}\/\d{2}\/\d{4})/);
    if (dateMatch) {
      dateStr = parseDate(dateMatch[1]!);
    }
    // Also try preceding siblings or the previous element
    if (!dateStr) {
      const prevText = $a.parent().prev().text();
      const prevDateMatch = prevText.match(/(\d{2}\/\d{2}\/\d{4})/);
      if (prevDateMatch) {
        dateStr = parseDate(prevDateMatch[1]!);
      }
    }
    // Try extracting from filename: /2024-06/ or /2021-12/
    if (!dateStr) {
      const urlDateMatch = href.match(/\/(\d{4})-(\d{2})\//);
      if (urlDateMatch) {
        dateStr = `${urlDateMatch[1]!}-${urlDateMatch[2]!}-01`;
      }
    }
    // Try /YYYY/ in URL
    if (!dateStr) {
      const yearMatch = href.match(/\/(\d{4})\//);
      if (yearMatch) {
        dateStr = `${yearMatch[1]}-01-01`;
      }
    }

    // Clean up the title: remove file size info like "(pdf - 523.36 KB)"
    let title = linkText.replace(/\s*\((?:pdf|PDF|docx?|DOCX?)[\s\-–]*[\d.,]+\s*[KMG]?B?\s*\)/g, "").trim();
    // Also remove trailing format indicators
    title = title.replace(/\s*(?:pdf|PDF)\s*$/, "").trim();

    if (!title || title.length < 3) return;

    const reference = extractReference(title) ?? extractReference(href);
    const type = classifyType(title, href);
    const finalRef = reference ?? generateReference(title, dateStr ?? "0000-00-00", autoIndex++);
    const sourcebook = classifySourcebook(finalRef, type, title);

    // Build a text description that includes the title and source URL
    const text = `${title}\n\nSource: ${fullUrl}`;

    provisions.push({
      sourcebook_id: sourcebook,
      reference: finalRef,
      title,
      text,
      type,
      status: "in_force",
      effective_date: dateStr,
      chapter: null,
      section: null,
    });
  });

  // Strategy 2: Find non-PDF links that point to FSMA node/detail pages
  // These are typically news articles about circulars or regulatory updates.
  contentEl.find('a[href*="/news/"], a[href*="/circ/"]').each((_i, el) => {
    const $a = $(el);
    const href = $a.attr("href") ?? "";
    const linkText = $a.text().trim();

    // Skip if we already captured this as a PDF link
    if (href.endsWith(".pdf") || href.endsWith(".PDF")) return;
    if (!linkText || linkText.length < 10) return;
    // Skip navigation links
    if (href.includes("#") && !href.includes("/news/")) return;

    const fullUrl = href.startsWith("http")
      ? href
      : `https://www.fsma.be${href}`;

    let dateStr: string | null = null;
    const parentText = $a.parent().text();
    const dateMatch2 = parentText.match(/(\d{2}\/\d{2}\/\d{4})/);
    if (dateMatch2) {
      dateStr = parseDate(dateMatch2[1]!);
    }

    const reference = extractReference(linkText) ?? extractReference(href);
    const type = classifyType(linkText, href);
    const finalRef =
      reference ?? generateReference(linkText, dateStr ?? "0000-00-00", autoIndex++);
    const sourcebook = classifySourcebook(finalRef, type, linkText);

    provisions.push({
      sourcebook_id: sourcebook,
      reference: finalRef,
      title: linkText,
      text: `${linkText}\n\nSource: ${fullUrl}`,
      type,
      status: "in_force",
      effective_date: dateStr,
      chapter: null,
      section: null,
    });
  });

  log(`  Found ${provisions.length} documents on ${page.label}`);
  return provisions;
}

/**
 * Crawl a single warning detail page to extract the full text.
 */
async function crawlWarningDetail(url: string): Promise<string | null> {
  try {
    const html = await rateLimitedFetch(url);
    const $ = cheerio.load(html);

    // Extract main content text from the article body
    const body = $(
      ".field--name-body, .node__content, article .content, main .content",
    );
    let text = body.text().trim();
    if (!text) {
      text = $("main").text().trim();
    }

    // Clean up whitespace
    text = text.replace(/\s+/g, " ").trim();
    return text.length > 20 ? text : null;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    warn(`Failed to fetch warning detail ${url}: ${msg}`);
    stats.errors++;
    return null;
  }
}

/**
 * Crawl warning listing pages (current + archive).
 */
async function crawlWarnings(): Promise<ProvisionRecord[]> {
  log("Crawling FSMA warnings pages...");
  const provisions: ProvisionRecord[] = [];

  const allPages = [...WARNING_CURRENT_PAGES, ...WARNING_ARCHIVE_PAGES];

  for (const pageUrl of allPages) {
    log(`  Fetching warnings page: ${pageUrl}`);

    let html: string;
    try {
      html = await rateLimitedFetch(pageUrl);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      error(`Failed to fetch ${pageUrl}: ${msg}`);
      stats.errors++;
      continue;
    }

    const $ = cheerio.load(html);

    // Warnings are displayed as table rows or linked list items
    // Each row: date | linked title → /en/warnings/slug or /fr/mises-en-garde/slug
    $('a[href*="/warnings/"], a[href*="/mises-en-garde/"]').each((_i, el) => {
      const $a = $(el);
      const href = $a.attr("href") ?? "";
      const title = $a.text().trim();

      // Skip navigation and archive links
      if (!title || title.length < 10) return;
      if (href.includes("/archive")) return;
      if (href === "/en/warnings" || href === "/fr/mises-en-garde") return;
      if (title.toLowerCase() === "archive") return;

      const fullUrl = href.startsWith("http")
        ? href
        : `https://www.fsma.be${href}`;

      // Extract date from the table row or parent element
      let dateStr: string | null = null;
      const row = $a.closest("tr, li, .views-row, .item-list");
      if (row.length > 0) {
        const rowText = row.text();
        const warnDateMatch = rowText.match(/(\d{2}\/\d{2}\/\d{4})/);
        if (warnDateMatch) {
          dateStr = parseDate(warnDateMatch[1]!);
        }
      }
      // Fallback: check preceding td or sibling
      if (!dateStr) {
        const prevTd = $a.closest("td").prev("td");
        if (prevTd.length) {
          const dm = prevTd.text().match(/(\d{2}\/\d{2}\/\d{4})/);
          if (dm) dateStr = parseDate(dm[1]!);
        }
      }

      // Create a slug-based reference from the URL
      const slugMatch = href.match(/\/(?:warnings|mises-en-garde)\/(.+?)(?:\?|$)/);
      const slug = slugMatch?.[1] ?? title.substring(0, 40);
      const reference = `FSMA_WARN_${slug.replace(/[^a-zA-Z0-9]/g, "_").toUpperCase().substring(0, 60)}`;

      provisions.push({
        sourcebook_id: "FSMA_WARNINGS",
        reference,
        title,
        text: `${title}\n\nSource: ${fullUrl}`,
        type: "warning",
        status: "in_force",
        effective_date: dateStr,
        chapter: null,
        section: null,
      });
    });
  }

  // Deduplicate by reference (same warning may appear on current + archive)
  const seen = new Set<string>();
  const deduped = provisions.filter((p) => {
    if (seen.has(p.reference)) return false;
    seen.add(p.reference);
    return true;
  });

  log(`  Found ${deduped.length} unique warnings (from ${provisions.length} raw entries)`);

  // Fetch detail pages for a representative subset to get full text
  // (all of them would take too long; we get the first 50)
  const toEnrich = deduped.slice(0, 50);
  log(`  Enriching ${toEnrich.length} warnings with detail page content...`);

  for (let i = 0; i < toEnrich.length; i++) {
    const p = toEnrich[i]!;
    const sourceMatch = p.text.match(/Source: (.+)/);
    if (!sourceMatch) continue;

    const detailUrl = sourceMatch[1]!;
    const detailText = await crawlWarningDetail(detailUrl);
    if (detailText) {
      p.text = `${p.title}\n\n${detailText}\n\nSource: ${detailUrl}`;
    }

    if ((i + 1) % 10 === 0) {
      log(`    Enriched ${i + 1}/${toEnrich.length} warnings`);
    }
  }

  return deduped;
}

/**
 * Crawl the FSMA search page for administrative sanctions.
 *
 * The FSMA search at /en/search?f[0]=type_of_document:Administrative+sanctions
 * returns sanctioned entities. We also check the sanctions procedure page for
 * linked decisions.
 */
async function crawlSanctions(): Promise<EnforcementRecord[]> {
  log("Crawling FSMA sanctions...");
  const enforcements: EnforcementRecord[] = [];

  // Crawl the sanctions procedure page for linked decisions
  const sanctionPages = [
    "https://www.fsma.be/fr/procedure-de-sanction",
    "https://www.fsma.be/fr/sanctions-administratives",
    "https://www.fsma.be/en/administrative-sanctions",
    "https://www.fsma.be/en/warnings-sanctions",
  ];

  for (const pageUrl of sanctionPages) {
    log(`  Fetching sanctions page: ${pageUrl}`);

    let html: string;
    try {
      html = await rateLimitedFetch(pageUrl);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      warn(`Failed to fetch ${pageUrl}: ${msg}`);
      stats.errors++;
      continue;
    }

    const $ = cheerio.load(html);

    // Look for links to sanction decisions (PDFs or detail pages)
    $('a[href*="sanction"], a[href*="amende"], a[href*="fine"], a[href*="beslissing"]').each(
      (_i, el) => {
        const $a = $(el);
        const href = $a.attr("href") ?? "";
        const title = $a.text().trim();

        if (!title || title.length < 10) return;
        if (href.includes("#")) return;

        const fullUrl = href.startsWith("http")
          ? href
          : `https://www.fsma.be${href}`;

        // Try to extract firm name, amount, date from surrounding text
        const parentText = $a.parent().text();
        let dateStr: string | null = null;
        const sanctDateMatch = parentText.match(/(\d{2}\/\d{2}\/\d{4})/);
        if (sanctDateMatch) {
          dateStr = parseDate(sanctDateMatch[1]!);
        }

        const amountMatch = parentText.match(
          /(\d[\d.,]+)\s*(?:euros?|EUR|€)/i,
        );
        const amount = amountMatch
          ? parseFloat(amountMatch[1]!.replace(/\./g, "").replace(",", "."))
          : 0;

        enforcements.push({
          firm_name: title.substring(0, 200),
          reference_number: extractReference(title) ?? extractReference(href),
          action_type: amount > 0 ? "fine" : "decision",
          amount,
          date: dateStr,
          summary: `${title}\n\nSource: ${fullUrl}`,
          sourcebook_references: null,
        });
      },
    );
  }

  // Also crawl the news pages for sanction-related news
  const sanctionNewsPages = [
    "https://www.fsma.be/en/search?s=sanction+decision&page=0",
    "https://www.fsma.be/en/search?s=sanction+decision&page=1",
    "https://www.fsma.be/fr/search?s=sanction+amende&page=0",
  ];

  for (const pageUrl of sanctionNewsPages) {
    log(`  Fetching sanctions news: ${pageUrl}`);

    let html: string;
    try {
      html = await rateLimitedFetch(pageUrl);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      warn(`Failed to fetch ${pageUrl}: ${msg}`);
      stats.errors++;
      continue;
    }

    const $ = cheerio.load(html);

    // Search results are rendered as linked items
    $('a[href*="/news/"]').each((_i, el) => {
      const $a = $(el);
      const title = $a.text().trim();
      const href = $a.attr("href") ?? "";

      if (!title || title.length < 15) return;

      // Only include results mentioning sanctions, fines, decisions
      const lower = title.toLowerCase();
      if (
        !lower.includes("sanction") &&
        !lower.includes("fine") &&
        !lower.includes("amende") &&
        !lower.includes("committee") &&
        !lower.includes("comité") &&
        !lower.includes("nyrstar") &&
        !lower.includes("penalty") &&
        !lower.includes("decision") &&
        !lower.includes("décision")
      ) {
        return;
      }

      const fullUrl = href.startsWith("http")
        ? href
        : `https://www.fsma.be${href}`;

      // Try to extract a date
      let dateStr: string | null = null;
      const newsRow = $a.closest(".search-result, .views-row, tr, li");
      if (newsRow.length > 0) {
        const dm = newsRow.text().match(/(\d{2}\/\d{2}\/\d{4})/);
        if (dm) dateStr = parseDate(dm[1]!);
      }

      // Try to extract firm name from the title
      // Common patterns: "Decision ... concerning [FIRM]"
      const firmMatch = title.match(
        /(?:concerning|contre|betreffende|regarding)\s+(.+?)(?:\s*[-–—]|$)/i,
      );
      const firmName = firmMatch?.[1]?.trim() ?? title.substring(0, 150);

      enforcements.push({
        firm_name: firmName,
        reference_number: null,
        action_type: "sanction",
        amount: 0,
        date: dateStr,
        summary: `${title}\n\nSource: ${fullUrl}`,
        sourcebook_references: null,
      });
    });
  }

  // Deduplicate by firm_name + date
  const seen = new Set<string>();
  const deduped = enforcements.filter((e) => {
    const key = `${e.firm_name}|${e.date ?? ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  log(`  Found ${deduped.length} unique enforcement entries`);
  return deduped;
}

// ---------------------------------------------------------------------------
// PDF text extraction (metadata only — we don't download full PDFs)
// ---------------------------------------------------------------------------

// Note: Full PDF text extraction is out of scope for this crawler.
// The crawler captures the metadata (title, reference, date, source URL)
// available on the listing pages. A future enhancement could download and
// parse PDF content using pdf-parse or similar.

// ---------------------------------------------------------------------------
// Main ingestion flow
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("=== FSMA Ingestion Crawler ===");
  log(`  Database: ${DB_PATH}`);
  log(`  Flags: resume=${FLAG_RESUME} dry-run=${FLAG_DRY_RUN} force=${FLAG_FORCE}`);
  log(`  Rate limit: ${RATE_LIMIT_MS}ms between requests`);
  log("");

  // Init database
  const db = FLAG_DRY_RUN ? null : initDb();

  // Track existing references for --resume
  const existingRefs = new Set<string>();
  const existingEnforcementKeys = new Set<string>();

  if (FLAG_RESUME && db) {
    const rows = db
      .prepare("SELECT reference FROM provisions")
      .all() as Array<{ reference: string }>;
    for (const row of rows) {
      existingRefs.add(row.reference);
    }

    const enfRows = db
      .prepare("SELECT firm_name, date FROM enforcement_actions")
      .all() as Array<{ firm_name: string; date: string | null }>;
    for (const row of enfRows) {
      existingEnforcementKeys.add(`${row.firm_name}|${row.date ?? ""}`);
    }

    log(`Resume mode: ${existingRefs.size} existing provisions, ${existingEnforcementKeys.size} existing enforcements`);
  }

  // Insert sourcebooks
  if (db) {
    const insertSourcebook = db.prepare(
      "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
    );
    for (const sb of SOURCEBOOKS) {
      insertSourcebook.run(sb.id, sb.name, sb.description);
    }
    log(`Upserted ${SOURCEBOOKS.length} sourcebooks`);
  } else {
    log(`[DRY RUN] Would insert ${SOURCEBOOKS.length} sourcebooks`);
  }

  // Prepare insert statements
  const insertProvision = db
    ? db.prepare(`
        INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
    : null;

  const insertEnforcement = db
    ? db.prepare(`
        INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `)
    : null;

  // -----------------------------------------------------------------------
  // Phase 1: Crawl category pages for circulars, communications, regulations
  // -----------------------------------------------------------------------
  log("");
  log("=== Phase 1: Category pages ===");

  const allProvisions: ProvisionRecord[] = [];

  for (const page of CATEGORY_PAGES) {
    const provisions = await crawlCategoryPage(page);
    allProvisions.push(...provisions);
  }

  // Deduplicate across all category pages (same doc may appear on multiple sector pages)
  const seenProvisions = new Set<string>();
  const dedupedProvisions = allProvisions.filter((p) => {
    if (seenProvisions.has(p.reference)) return false;
    seenProvisions.add(p.reference);
    return true;
  });

  log(`\nPhase 1 total: ${dedupedProvisions.length} unique provisions (from ${allProvisions.length} raw)`);

  // Insert provisions
  if (db && insertProvision) {
    const tx = db.transaction(() => {
      for (const p of dedupedProvisions) {
        if (FLAG_RESUME && existingRefs.has(p.reference)) {
          stats.provisionsSkipped++;
          continue;
        }

        insertProvision.run(
          p.sourcebook_id,
          p.reference,
          p.title,
          p.text,
          p.type,
          p.status,
          p.effective_date,
          p.chapter,
          p.section,
        );
        stats.provisionsInserted++;
      }
    });
    tx();
  } else {
    for (const p of dedupedProvisions) {
      if (FLAG_RESUME && existingRefs.has(p.reference)) {
        stats.provisionsSkipped++;
      } else {
        stats.provisionsInserted++;
      }
    }
  }

  log(`  Inserted: ${stats.provisionsInserted}, Skipped (resume): ${stats.provisionsSkipped}`);

  // -----------------------------------------------------------------------
  // Phase 2: Crawl warnings
  // -----------------------------------------------------------------------
  log("");
  log("=== Phase 2: Warnings ===");

  const warnings = await crawlWarnings();

  const warningsBefore = stats.provisionsInserted;
  if (db && insertProvision) {
    const tx = db.transaction(() => {
      for (const w of warnings) {
        if (FLAG_RESUME && existingRefs.has(w.reference)) {
          stats.provisionsSkipped++;
          continue;
        }

        insertProvision.run(
          w.sourcebook_id,
          w.reference,
          w.title,
          w.text,
          w.type,
          w.status,
          w.effective_date,
          w.chapter,
          w.section,
        );
        stats.provisionsInserted++;
      }
    });
    tx();
  } else {
    for (const w of warnings) {
      if (FLAG_RESUME && existingRefs.has(w.reference)) {
        stats.provisionsSkipped++;
      } else {
        stats.provisionsInserted++;
      }
    }
  }

  log(`  Warnings inserted: ${stats.provisionsInserted - warningsBefore}`);

  // -----------------------------------------------------------------------
  // Phase 3: Crawl sanctions / enforcement
  // -----------------------------------------------------------------------
  log("");
  log("=== Phase 3: Sanctions ===");

  const sanctions = await crawlSanctions();

  if (db && insertEnforcement) {
    const tx = db.transaction(() => {
      for (const e of sanctions) {
        const key = `${e.firm_name}|${e.date ?? ""}`;
        if (FLAG_RESUME && existingEnforcementKeys.has(key)) {
          stats.enforcementsSkipped++;
          continue;
        }

        insertEnforcement.run(
          e.firm_name,
          e.reference_number,
          e.action_type,
          e.amount,
          e.date,
          e.summary,
          e.sourcebook_references,
        );
        stats.enforcementsInserted++;
      }
    });
    tx();
  } else {
    for (const e of sanctions) {
      const key = `${e.firm_name}|${e.date ?? ""}`;
      if (FLAG_RESUME && existingEnforcementKeys.has(key)) {
        stats.enforcementsSkipped++;
      } else {
        stats.enforcementsInserted++;
      }
    }
  }

  log(`  Enforcements inserted: ${stats.enforcementsInserted}, Skipped: ${stats.enforcementsSkipped}`);

  // -----------------------------------------------------------------------
  // Summary
  // -----------------------------------------------------------------------
  log("");
  log("=== Ingestion complete ===");
  log(`  Pages visited:          ${stats.pagesVisited}`);
  log(`  Provisions inserted:    ${stats.provisionsInserted}`);
  log(`  Provisions skipped:     ${stats.provisionsSkipped}`);
  log(`  Enforcements inserted:  ${stats.enforcementsInserted}`);
  log(`  Enforcements skipped:   ${stats.enforcementsSkipped}`);
  log(`  Errors:                 ${stats.errors}`);

  if (db) {
    const provisionCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as {
        cnt: number;
      }
    ).cnt;
    const sourcebookCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as {
        cnt: number;
      }
    ).cnt;
    const enforcementCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
        cnt: number;
      }
    ).cnt;
    const ftsCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as {
        cnt: number;
      }
    ).cnt;

    log("");
    log("Database totals:");
    log(`  Sourcebooks:          ${sourcebookCount}`);
    log(`  Provisions:           ${provisionCount}`);
    log(`  Enforcement actions:  ${enforcementCount}`);
    log(`  FTS entries:          ${ftsCount}`);
    log(`\nDatabase at ${DB_PATH}`);

    db.close();
  } else {
    log("\n[DRY RUN] No data written to database.");
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  error(`Fatal: ${err instanceof Error ? err.message : String(err)}`);
  if (err instanceof Error && err.stack) {
    console.error(err.stack);
  }
  process.exit(1);
});
