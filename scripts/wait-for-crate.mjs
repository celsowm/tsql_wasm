#!/usr/bin/env node

const crateName = process.argv[2];
const expectedVersion = process.argv[3];
const maxAttempts = Number(process.env.MAX_ATTEMPTS ?? 30);
const delayMs = Number(process.env.DELAY_MS ?? 10000);

if (!crateName || !expectedVersion) {
  console.error("Usage: node scripts/wait-for-crate.mjs <crate-name> <expected-version>");
  process.exit(1);
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchLatestVersion() {
  const response = await fetch(`https://crates.io/api/v1/crates/${crateName}`, {
    headers: {
      "User-Agent": "iridium-sql-wait-for-crate",
    },
  });

  if (!response.ok) {
    throw new Error(`Could not query crates.io for ${crateName}: ${response.status}`);
  }

  const body = await response.json();
  return body?.crate?.newest_version ?? null;
}

for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
  const latestVersion = await fetchLatestVersion();
  if (latestVersion === expectedVersion) {
    console.log(`${crateName}@${expectedVersion} is visible on crates.io`);
    process.exit(0);
  }

  console.log(
    `Waiting for ${crateName}@${expectedVersion} on crates.io (attempt ${attempt}/${maxAttempts}, current: ${latestVersion ?? "missing"})`,
  );
  await sleep(delayMs);
}

console.error(`Timed out waiting for ${crateName}@${expectedVersion} on crates.io`);
process.exit(1);
