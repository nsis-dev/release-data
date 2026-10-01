import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

import { pMap } from './concurrency.ts';
import { discover } from './discover.ts';
import { fetchBuffer, fetchHead, toIsoDate } from './fetch.ts';
import { computeHashes } from './hash.ts';
import { mergeDataset } from './merge.ts';
import { srcTarballUrl, zipUrl } from './sourceforge.ts';
import type { Channel, Dataset, Seed, VersionEntry } from './types.ts';
import { assertDataset } from './validate.ts';
import { majorOf, preReleaseSuffix } from './version.ts';

const ROOT = path.resolve(fileURLToPath(import.meta.url), '../..');
const DATA_PATH = path.join(ROOT, 'data', 'versions.json');
const SEED_PATH = path.join(ROOT, 'seed', 'versions.json');
const SCHEMA_URL = 'https://nsis-dev.github.io/release-data/versions.schema.json';

interface Candidate {
  version: string;
  channel: Channel;
}

function emptyDataset(): Dataset {
  return {
    $schema: SCHEMA_URL,
    generatedAt: new Date().toISOString(),
    source: 'sourceforge.net/projects/nsis',
    versions: {},
    latest: {
      stable: { v2: null, v3: null },
      prerelease: { v2: null, v3: null },
    },
  };
}

async function loadDataset(): Promise<Dataset> {
  if (!existsSync(DATA_PATH)) return emptyDataset();
  const raw = await readFile(DATA_PATH, 'utf-8');
  const parsed = JSON.parse(raw) as Dataset;
  if (!parsed.$schema) parsed.$schema = SCHEMA_URL;
  return parsed;
}

async function loadSeed(): Promise<Seed> {
  const raw = await readFile(SEED_PATH, 'utf-8');
  return JSON.parse(raw) as Seed;
}

async function processCandidate(
  candidate: Candidate,
): Promise<VersionEntry | null> {
  const { version, channel } = candidate;
  const major = majorOf(version);
  const zipUrlStr = zipUrl(version, major, channel);
  const tarUrlStr = srcTarballUrl(version, major, channel);

  console.log(`fetch ${version} (${channel})`);
  const zipRes = await fetchBuffer(zipUrlStr);
  if (!zipRes) {
    console.warn(`skip ${version}: zip not found at ${zipUrlStr}`);
    return null;
  }
  const tarRes = await fetchBuffer(tarUrlStr);

  return {
    version,
    major,
    channel,
    preRelease: preReleaseSuffix(version),
    releasedAt: zipRes.lastModified ? toIsoDate(zipRes.lastModified) : null,
    artifacts: {
      zip: {
        url: zipUrlStr,
        filename: `nsis-${version}.zip`,
        size: zipRes.body.length,
        hashes: computeHashes(zipRes.body),
      },
      sourceTarball: tarRes
        ? {
          url: tarUrlStr,
          filename: `nsis-${version}-src.tar.bz2`,
          size: tarRes.body.length,
          hashes: computeHashes(tarRes.body),
        }
        : null,
    },
  };
}

async function backfillReleasedAt(
  dataset: Dataset,
  concurrency: number,
): Promise<number> {
  const targets = Object.values(dataset.versions).filter(
    (e) => e.releasedAt === null,
  );
  if (targets.length === 0) return 0;

  console.log(`backfill releasedAt: HEAD ${targets.length} version(s)`);
  let filled = 0;
  await pMap(targets, concurrency, async (entry) => {
    const head = await fetchHead(entry.artifacts.zip.url);
    if (head?.lastModified) {
      entry.releasedAt = toIsoDate(head.lastModified);
      filled += 1;
    }
  });
  return filled;
}

interface Args {
  noDiscover: boolean;
  only: string[];
  concurrency: number;
}

function parseCli(): Args {
  const { values } = parseArgs({
    options: {
      'no-discover': { type: 'boolean', default: false },
      only: { type: 'string', multiple: true },
      concurrency: { type: 'string', default: '4' },
    },
  });
  const concurrency = Number(values.concurrency);
  if (!Number.isFinite(concurrency) || concurrency < 1) {
    throw new Error(`invalid --concurrency: ${values.concurrency as string}`);
  }
  return {
    noDiscover: values['no-discover'] === true,
    only: (values.only as string[] | undefined) ?? [],
    concurrency,
  };
}

async function main(): Promise<void> {
  const args = parseCli();

  const [dataset, seed] = await Promise.all([loadDataset(), loadSeed()]);

  const candidates = new Map<string, Channel>();
  for (const e of seed.versions) candidates.set(e.version, e.channel);

  if (!args.noDiscover) {
    const discovered = await discover();
    for (const [version, channel] of discovered) {
      if (!candidates.has(version)) candidates.set(version, channel);
    }
  }

  let missing: Candidate[] = [];
  for (const [version, channel] of candidates) {
    if (!(version in dataset.versions)) missing.push({ version, channel });
  }

  if (args.only.length > 0) {
    const filter = new Set(args.only);
    missing = missing.filter((m) => filter.has(m.version));
  }

  let newEntries: VersionEntry[] = [];
  if (missing.length === 0) {
    console.log('no missing versions to fetch');
  } else {
    console.log(
      `processing ${missing.length} version(s) with concurrency ${args.concurrency}`,
    );
    const results = await pMap(missing, args.concurrency, processCandidate);
    newEntries = results.filter((r): r is VersionEntry => r !== null);
  }

  const merged = mergeDataset(dataset, newEntries);

  const filled = await backfillReleasedAt(merged, args.concurrency);
  if (filled > 0) {
    merged.generatedAt = new Date().toISOString();
    console.log(`backfilled releasedAt for ${filled} version(s)`);
  }

  if (newEntries.length === 0 && filled === 0) {
    console.log('dataset is up to date');
    return;
  }

  assertDataset(merged);
  await mkdir(path.dirname(DATA_PATH), { recursive: true });
  await writeFile(DATA_PATH, JSON.stringify(merged, null, 2) + '\n', 'utf-8');
  console.log(
    `wrote dataset: ${newEntries.length} new entries, ${filled} dates backfilled`,
  );
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
