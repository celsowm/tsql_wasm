#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const npmPackage = {
  key: "npm",
  name: "@celsowm/iridium-sql-client",
  manifest: "packages/iridium-client/package.json",
  registry: "npm",
};

const crates = [
  {
    key: "iridium_core",
    name: "iridium_core",
    manifest: "crates/iridium_core/Cargo.toml",
    registry: "crates",
  },
  {
    key: "iridium_server",
    name: "iridium_server",
    manifest: "crates/iridium_server/Cargo.toml",
    registry: "crates",
  },
  {
    key: "iridium_wasm",
    name: "iridium_wasm",
    manifest: "crates/iridium_wasm/Cargo.toml",
    registry: "crates",
  },
  {
    key: "iridium_server_test_support",
    name: "iridium_server_test_support",
    manifest: "crates/iridium_server_test_support/Cargo.toml",
    registry: "crates",
  },
];

function parseSemver(version) {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(version ?? "");
  if (!match) {
    throw new Error(`Unsupported semantic version: ${version}`);
  }

  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
  };
}

function compareSemver(left, right) {
  const l = parseSemver(left);
  const r = parseSemver(right);

  if (l.major !== r.major) return l.major - r.major;
  if (l.minor !== r.minor) return l.minor - r.minor;
  return l.patch - r.patch;
}

async function readJson(relativePath) {
  const filePath = path.join(repoRoot, relativePath);
  return JSON.parse(await readFile(filePath, "utf8"));
}

async function readTomlVersion(relativePath) {
  const filePath = path.join(repoRoot, relativePath);
  const contents = await readFile(filePath, "utf8");
  const match = contents.match(/^version = "([^"]+)"/m);

  if (!match) {
    throw new Error(`Could not read version from ${relativePath}`);
  }

  return match[1];
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      "User-Agent": "iridium-sql-release-status",
    },
  });

  if (response.status === 404) {
    return null;
  }

  if (!response.ok) {
    throw new Error(`Request failed for ${url}: ${response.status}`);
  }

  return response.json();
}

async function fetchNpmVersion(packageName) {
  const encodedName = packageName.replace("/", "%2f");
  const body = await fetchJson(`https://registry.npmjs.org/${encodedName}/latest`);
  return body?.version ?? null;
}

async function fetchCrateVersion(crateName) {
  const body = await fetchJson(`https://crates.io/api/v1/crates/${crateName}`);
  return body?.crate?.newest_version ?? null;
}

function asOutputValue(value) {
  return value == null ? "" : String(value);
}

async function collectStatus() {
  const npmManifest = await readJson(npmPackage.manifest);
  const npmLocalVersion = npmManifest.version;
  const npmRemoteVersion = await fetchNpmVersion(npmPackage.name);

  const crateStatuses = [];
  for (const crateDef of crates) {
    const localVersion = await readTomlVersion(crateDef.manifest);
    const remoteVersion = await fetchCrateVersion(crateDef.name);
    crateStatuses.push({
      ...crateDef,
      localVersion,
      remoteVersion,
      shouldPublish:
        remoteVersion == null || compareSemver(localVersion, remoteVersion) > 0,
    });
  }

  return {
    npm: {
      ...npmPackage,
      localVersion: npmLocalVersion,
      remoteVersion: npmRemoteVersion,
      shouldPublish:
        npmRemoteVersion == null || compareSemver(npmLocalVersion, npmRemoteVersion) > 0,
    },
    crates: crateStatuses,
  };
}

function writeGithubOutput(status) {
  const lines = [
    `npm_local_version=${asOutputValue(status.npm.localVersion)}`,
    `npm_remote_version=${asOutputValue(status.npm.remoteVersion)}`,
    `npm_should_publish=${status.npm.shouldPublish}`,
    `shared_version=${asOutputValue(status.npm.localVersion)}`,
    `any_crates_should_publish=${status.crates.some((crate) => crate.shouldPublish)}`,
  ];

  for (const crate of status.crates) {
    lines.push(`${crate.key}_local_version=${asOutputValue(crate.localVersion)}`);
    lines.push(`${crate.key}_remote_version=${asOutputValue(crate.remoteVersion)}`);
    lines.push(`${crate.key}_should_publish=${crate.shouldPublish}`);
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  const status = await collectStatus();

  if (process.env.GITHUB_OUTPUT) {
    const { appendFile } = await import("node:fs/promises");
    await appendFile(process.env.GITHUB_OUTPUT, writeGithubOutput(status), "utf8");
  } else {
    console.log(JSON.stringify(status, null, 2));
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
