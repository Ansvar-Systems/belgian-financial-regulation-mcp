/**
 * Seed the FSMA/NBB database with sample provisions for testing.
 *
 * Inserts representative FSMA circulaires, NBB circulaires, FSMA communications,
 * NBB guidance, and enforcement actions so MCP tools can be verified without
 * running a full ingestion crawl. Content is in French, reflecting the primary
 * publication language of FSMA and NBB.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["FSMA_DB_PATH"] ?? "data/fsma.db";
const force = process.argv.includes("--force");

// -- Bootstrap database -------------------------------------------------------

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// -- Sourcebooks --------------------------------------------------------------

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "FSMA_CIRCULAIRES",
    name: "FSMA Circulaires",
    description:
      "Circulaires réglementaires publiées par l'Autorité des services et marchés financiers (FSMA). Couvrent la gouvernance, la gestion des risques ICT, la protection des investisseurs, et les exigences de conformité pour les établissements financiers belges.",
  },
  {
    id: "FSMA_COMMUNICATIONS",
    name: "FSMA Communications",
    description:
      "Communications officielles et orientations pratiques de la FSMA à destination des établissements supervisés. Comprend les communications sur la lutte contre le blanchiment de capitaux et le financement du terrorisme.",
  },
  {
    id: "NBB_CIRCULAIRES",
    name: "NBB Circulaires",
    description:
      "Circulaires prudentielles publiées par la Banque Nationale de Belgique (NBB). Couvrent la résilience opérationnelle, l'externalisation, les fonds propres, et les exigences de liquidité pour les établissements de crédit.",
  },
  {
    id: "NBB_GUIDANCE",
    name: "NBB Guidance",
    description:
      "Orientations et recommandations prudentielles de la Banque Nationale de Belgique. Fournissent des précisions sur l'application des exigences réglementaires et les attentes de surveillance pour les établissements soumis à la supervision NBB.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

// -- Sample provisions --------------------------------------------------------

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  // -- FSMA Circulaires — ICT Risk Management ----------------------------------
  {
    sourcebook_id: "FSMA_CIRCULAIRES",
    reference: "FSMA_2022_01",
    title: "Circulaire FSMA_2022_01 — Gestion du risque lié aux technologies de l'information et de la communication (TIC)",
    text: "La présente circulaire précise les attentes de la FSMA concernant la gestion du risque lié aux TIC par les sociétés de gestion de portefeuille et de conseil en investissement. Les établissements doivent disposer d'un cadre de gestion du risque TIC couvrant l'identification, la protection, la détection, la réponse et le rétablissement. Ils doivent mettre en place des politiques et procédures documentées pour assurer la continuité des activités en cas d'incident TIC majeur. Le cadre doit prévoir des tests réguliers, notamment des tests de pénétration et des exercices de gestion de crise. Les établissements doivent notifier à la FSMA tout incident TIC majeur dans les délais fixés par la réglementation.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-03-01",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "FSMA_CIRCULAIRES",
    reference: "FSMA_2022_01_S2",
    title: "Circulaire FSMA_2022_01 — Section 2 : Exigences organisationnelles en matière de TIC",
    text: "Les établissements doivent désigner un responsable de la sécurité des systèmes d'information (RSSI) disposant des compétences, de l'autorité et des ressources nécessaires. La fonction de gestion du risque TIC doit être indépendante des fonctions opérationnelles. Le conseil d'administration est responsable de la supervision du cadre de gestion du risque TIC et doit recevoir des rapports réguliers sur l'état de la sécurité informatique. Les dirigeants effectifs doivent approuver la politique de sécurité TIC et en assurer la mise en oeuvre.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-03-01",
    chapter: "1",
    section: "1.2",
  },

  // -- FSMA Circulaires — Governance -------------------------------------------
  {
    sourcebook_id: "FSMA_CIRCULAIRES",
    reference: "FSMA_2021_05",
    title: "Circulaire FSMA_2021_05 — Gouvernance interne des sociétés de gestion",
    text: "La présente circulaire établit les exigences de gouvernance interne applicables aux sociétés de gestion d'organismes de placement collectif (OPC) et de fonds d'investissement alternatifs (FIA). Les établissements doivent disposer d'une structure organisationnelle claire avec des lignes de responsabilité bien définies. Le conseil d'administration doit comprendre un nombre suffisant de membres indépendants et doit être doté d'un comité d'audit, d'un comité de rémunération et d'un comité des risques. Les politiques de rémunération doivent être alignées sur les intérêts à long terme des investisseurs et décourager une prise de risque excessive.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-09-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "FSMA_CIRCULAIRES",
    reference: "FSMA_2021_05_S3",
    title: "Circulaire FSMA_2021_05 — Section 3 : Gestion des conflits d'intérêts",
    text: "Les établissements doivent identifier, prévenir et gérer les conflits d'intérêts susceptibles de nuire aux intérêts des clients ou des investisseurs. Une politique de gestion des conflits d'intérêts doit être établie, tenue à jour et communiquée au personnel concerné. Lorsque les mesures organisationnelles prises ne suffisent pas à prévenir un risque de préjudice pour les clients, les établissements doivent informer clairement les clients de la nature générale et des sources des conflits d'intérêts avant d'agir pour leur compte.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-09-01",
    chapter: "2",
    section: "2.3",
  },

  // -- FSMA Communications — Anti-Money Laundering ----------------------------
  {
    sourcebook_id: "FSMA_COMMUNICATIONS",
    reference: "FSMA_COMM_2023_03",
    title: "Communication FSMA — Prévention du blanchiment de capitaux et du financement du terrorisme : attentes de surveillance",
    text: "La FSMA rappelle aux établissements sous sa supervision les exigences applicables en matière de lutte contre le blanchiment de capitaux et le financement du terrorisme (LBC/FT) conformément à la loi du 18 septembre 2017. Les établissements doivent disposer d'une analyse de risque LBC/FT actualisée, d'une politique d'acceptation des clients fondée sur une approche par les risques, et de procédures de vigilance appropriées. Les établissements à risque élevé doivent faire l'objet de mesures de vigilance renforcées. Tout soupçon doit être signalé à la Cellule de traitement des informations financières (CTIF) sans délai.",
    type: "communication",
    status: "in_force",
    effective_date: "2023-06-01",
    chapter: "3",
    section: "3.1",
  },
  {
    sourcebook_id: "FSMA_COMMUNICATIONS",
    reference: "FSMA_COMM_2023_03_S2",
    title: "Communication FSMA — LBC/FT : Vigilance à l'égard des personnes politiquement exposées (PPE)",
    text: "Lorsqu'une relation d'affaires est nouée avec une personne politiquement exposée (PPE) ou un membre de sa famille proche ou une personne connue pour être étroitement associée à une PPE, les établissements sont tenus d'appliquer des mesures de vigilance renforcées. Ces mesures comprennent l'obtention de l'autorisation d'un niveau hiérarchique supérieur avant d'établir ou de poursuivre la relation d'affaires, la détermination de l'origine du patrimoine et des fonds impliqués, et la surveillance renforcée et continue de la relation d'affaires.",
    type: "communication",
    status: "in_force",
    effective_date: "2023-06-01",
    chapter: "3",
    section: "3.2",
  },

  // -- NBB Circulaires — Operational Resilience --------------------------------
  {
    sourcebook_id: "NBB_CIRCULAIRES",
    reference: "NBB_2022_05",
    title: "Circulaire NBB_2022_05 — Résilience opérationnelle des établissements de crédit",
    text: "La présente circulaire précise les attentes prudentielles de la Banque Nationale de Belgique concernant la résilience opérationnelle des établissements de crédit conformément aux orientations EBA. Les établissements doivent cartographier leurs services critiques et leurs dépendances, établir des seuils de tolérance aux perturbations et développer des plans de continuité des activités et de rétablissement après sinistre. Ils doivent tester régulièrement leurs dispositifs de résilience opérationnelle, notamment par des simulations de crise et des tests de basculement. Les résultats des tests doivent être communiqués à l'organe de direction et à la NBB sur demande.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-07-01",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "NBB_CIRCULAIRES",
    reference: "NBB_2022_05_S2",
    title: "Circulaire NBB_2022_05 — Section 2 : Gestion de la continuité des activités",
    text: "Les établissements doivent disposer d'un plan de continuité des activités (PCA) documenté et régulièrement mis à jour, couvrant l'ensemble des fonctions critiques. Le PCA doit définir les objectifs de temps de rétablissement (RTO) et les objectifs de point de rétablissement (RPO) pour chaque service critique. Les plans de crise doivent prévoir des procédures de communication interne et externe et des mécanismes de coordination avec les prestataires de services essentiels. Les établissements doivent désigner un responsable de la continuité des activités disposant d'une autorité suffisante.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2022-07-01",
    chapter: "1",
    section: "1.2",
  },

  // -- NBB Circulaires — Outsourcing -------------------------------------------
  {
    sourcebook_id: "NBB_CIRCULAIRES",
    reference: "NBB_2021_08",
    title: "Circulaire NBB_2021_08 — Externalisation par les établissements de crédit",
    text: "La présente circulaire précise les exigences applicables à l'externalisation d'activités par les établissements de crédit conformément aux orientations EBA sur l'externalisation. Les établissements doivent disposer d'une politique d'externalisation approuvée par l'organe de direction, définissant les critères de distinction entre externalisations critiques ou importantes et non critiques. Avant de procéder à une externalisation critique ou importante, les établissements doivent effectuer une évaluation préalable des risques et notifier la NBB. Les accords d'externalisation doivent comporter des clauses contractuelles minimales, notamment des droits d'accès et d'audit.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-12-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "NBB_CIRCULAIRES",
    reference: "NBB_2021_08_S3",
    title: "Circulaire NBB_2021_08 — Section 3 : Sous-externalisation et concentration",
    text: "Lorsqu'un prestataire de services externalisés fait lui-même appel à des sous-traitants pour les activités externalisées, l'établissement doit s'assurer que les sous-contrats répondent aux mêmes exigences que le contrat principal. Les établissements doivent évaluer et gérer les risques de concentration découlant de la dépendance à l'égard de prestataires communs, notamment dans le domaine du cloud computing. Un registre de toutes les externalisations doit être tenu à jour et communiqué à la NBB sur demande.",
    type: "circulaire",
    status: "in_force",
    effective_date: "2021-12-01",
    chapter: "2",
    section: "2.3",
  },

  // -- NBB Guidance ------------------------------------------------------------
  {
    sourcebook_id: "NBB_GUIDANCE",
    reference: "NBB_GUIDE_2023_01",
    title: "Orientations NBB — Application du règlement DORA aux établissements belges",
    text: "Les présentes orientations précisent les modalités d'application du règlement (UE) 2022/2554 relatif à la résilience opérationnelle numérique du secteur financier (DORA) aux établissements soumis à la supervision de la NBB. À partir du 17 janvier 2025, les établissements doivent se conformer aux exigences DORA en matière de gestion des risques TIC, de gestion des incidents, de tests, de gestion du risque lié aux tiers prestataires de services TIC, et de partage d'informations. La NBB fournira des précisions sur les modalités de notification des incidents majeurs liés aux TIC au fur et à mesure de la finalisation des normes techniques de règlementation (RTS) par les autorités européennes de surveillance.",
    type: "guidance",
    status: "in_force",
    effective_date: "2023-11-01",
    chapter: "1",
    section: "1.1",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
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
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

// -- Sample enforcement actions -----------------------------------------------

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "Optima Bank NV",
    reference_number: "FSMA_ENF_2012_001",
    action_type: "ban",
    amount: 0,
    date: "2012-06-01",
    summary:
      "La FSMA a imposé un moratoire sur la vente de produits d'investissement par Optima Bank NV suite à la constatation de manquements graves dans les pratiques commerciales de l'établissement. Optima Bank commercialisait des produits financiers structurés complexes à des clients de détail sans effectuer d'évaluation d'adéquation appropriée et sans fournir une information claire et complète sur les risques. L'établissement a ultérieurement subi d'importantes pertes et été mis en liquidation, causant un préjudice significatif à plusieurs milliers d'investisseurs belges.",
    sourcebook_references: "FSMA_2021_05, FSMA_COMM_2023_03",
  },
  {
    firm_name: "Arco Group / Arcopar CVBA",
    reference_number: "FSMA_ENF_2014_002",
    action_type: "warning",
    amount: 0,
    date: "2014-03-01",
    summary:
      "La FSMA a publié un avertissement relatif à la commercialisation par Arco Group de parts coopératives auprès du grand public sans prospectus approuvé, en violation des règles applicables aux offres publiques de valeurs mobilières. L'affaire Arco a mis en évidence des lacunes dans la protection des investisseurs de détail ayant souscrit des parts coopératives présentées comme équivalentes à des produits d'épargne garantis. Cette affaire a conduit à des réformes importantes du cadre réglementaire belge relatif aux coopératives financières.",
    sourcebook_references: "FSMA_CIRCULAIRES, FSMA_COMMUNICATIONS",
  },
  {
    firm_name: "Établissement de crédit anonyme (décision NBB)",
    reference_number: "NBB_ENF_2023_004",
    action_type: "fine",
    amount: 2_500_000,
    date: "2023-09-15",
    summary:
      "La NBB a infligé une amende administrative de 2,5 millions d'euros à un établissement de crédit belge pour des manquements graves aux exigences de résilience opérationnelle. L'établissement n'avait pas effectué de tests suffisants de son plan de continuité des activités, n'avait pas correctement notifié les incidents opérationnels majeurs dans les délais réglementaires, et n'avait pas mis en place de dispositifs adéquats de gestion des risques liés à l'externalisation critique de ses services informatiques vers un prestataire de cloud.",
    sourcebook_references: "NBB_2022_05, NBB_2021_08",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name,
      e.reference_number,
      e.action_type,
      e.amount,
      e.date,
      e.summary,
      e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

// -- Summary ------------------------------------------------------------------

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

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
