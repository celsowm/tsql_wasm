#!/usr/bin/env node

import { readFile, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const command = process.argv[2];

if (!["bump", "check"].includes(command)) {
  console.error("Usage: node scripts/version-sync.mjs <bump|check>");
  process.exit(1);
}

const jsonFiles = [
  "packages/iridium-client/package.json",
  "packages/iridium-client/package-lock.json",
  "packages/iridium-playground/package.json",
  "packages/iridium-tests/package.json",
];

const cargoTomlFiles = [
  {
    path: "crates/iridium_core/Cargo.toml",
    pathDependencies: [],
  },
  {
    path: "crates/iridium_wasm/Cargo.toml",
    pathDependencies: [{ name: "iridium_core", path: "../iridium_core" }],
  },
  {
    path: "crates/iridium_server/Cargo.toml",
    pathDependencies: [{ name: "iridium_core", path: "../iridium_core" }],
  },
  {
    path: "crates/iridium_server_test_support/Cargo.toml",
    pathDependencies: [
      { name: "iridium_core", path: "../iridium_core" },
      { name: "iridium_server", path: "../iridium_server" },
    ],
  },
];

const cargoLockPackages = [
  "iridium_core",
  "iridium_server",
  "iridium_server_test_support",
  "iridium_wasm",
];

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseSemver(version) {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(version);
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
  if (left.major !== right.major) return left.major - right.major;
  if (left.minor !== right.minor) return left.minor - right.minor;
  return left.patch - right.patch;
}

function incrementPatch(version) {
  const parsed = parseSemver(version);
  return `${parsed.major}.${parsed.minor}.${parsed.patch + 1}`;
}

async function readText(relativePath) {
  return readFile(path.join(repoRoot, relativePath), "utf8");
}

async function writeText(relativePath, contents) {
  await writeFile(path.join(repoRoot, relativePath), `${contents.replace(/\s*$/, "")}\n`, "utf8");
}

async function readJson(relativePath) {
  return JSON.parse(await readText(relativePath));
}

async function writeJson(relativePath, data) {
  await writeText(relativePath, JSON.stringify(data, null, 2));
}

function collectVersionsFromJson(relativePath, data) {
  const versions = [];
  if (typeof data.version === "string") {
    versions.push({ source: `${relativePath}#version`, version: data.version });
  }

  if (relativePath.endsWith("package-lock.json")) {
    const rootVersion = data.packages?.[""]?.version;
    if (typeof rootVersion === "string") {
      versions.push({ source: `${relativePath}#packages[""].version`, version: rootVersion });
    }
  }

  return versions;
}

function collectVersionsFromCargoToml(relativePath, contents, pathDependencies) {
  const versions = [];
  const packageVersionMatch = contents.match(/^version = "([^"]+)"/m);
  if (packageVersionMatch) {
    versions.push({ source: `${relativePath}#package.version`, version: packageVersionMatch[1] });
  }

  for (const dependency of pathDependencies) {
    const dependencyPattern = new RegExp(
      `^${escapeRegex(dependency.name)} = \\{ path = "${escapeRegex(dependency.path)}", version = "([^"]+)" \\}$`,
      "m",
    );
    const dependencyMatch = contents.match(dependencyPattern);
    if (dependencyMatch) {
      versions.push({
        source: `${relativePath}#dependency.${dependency.name}`,
        version: dependencyMatch[1],
      });
    }
  }

  return versions;
}

function collectVersionsFromCargoLock(contents) {
  const versions = [];

  for (const packageName of cargoLockPackages) {
    const pattern = new RegExp(
      `name = "${escapeRegex(packageName)}"\\r?\\nversion = "([^"]+)"`,
      "m",
    );
    const match = contents.match(pattern);
    if (match) {
      versions.push({ source: `Cargo.lock#${packageName}`, version: match[1] });
    }
  }

  return versions;
}

async function collectAllVersions() {
  const versions = [];

  for (const file of jsonFiles) {
    const data = await readJson(file);
    versions.push(...collectVersionsFromJson(file, data));
  }

  for (const file of cargoTomlFiles) {
    const contents = await readText(file.path);
    versions.push(...collectVersionsFromCargoToml(file.path, contents, file.pathDependencies));
  }

  const cargoLockContents = await readText("Cargo.lock");
  versions.push(...collectVersionsFromCargoLock(cargoLockContents));

  return versions;
}

function getUniqueVersions(versions) {
  return [...new Set(versions.map((entry) => entry.version))];
}

async function updateJsonVersions(nextVersion) {
  for (const file of jsonFiles) {
    const data = await readJson(file);

    if (typeof data.version === "string") {
      data.version = nextVersion;
    }

    if (file.endsWith("package-lock.json") && data.packages?.[""]) {
      data.packages[""].version = nextVersion;
    }

    await writeJson(file, data);
  }
}

function updateCargoToml(contents, nextVersion, pathDependencies) {
  let updated = contents.replace(/^version = "[^"]+"/m, `version = "${nextVersion}"`);

  for (const dependency of pathDependencies) {
    const dependencyPattern = new RegExp(
      `^(${escapeRegex(dependency.name)} = \\{ path = "${escapeRegex(dependency.path)}", version = ")[^"]+(" \\})$`,
      "m",
    );
    updated = updated.replace(dependencyPattern, `$1${nextVersion}$2`);
  }

  return updated;
}

function updateCargoLock(contents, nextVersion) {
  let updated = contents;

  for (const packageName of cargoLockPackages) {
    const packagePattern = new RegExp(
      `(name = "${escapeRegex(packageName)}"\\r?\\nversion = ")[^"]+(")`,
      "m",
    );
    updated = updated.replace(packagePattern, `$1${nextVersion}$2`);
  }

  return updated;
}

async function writeCargoVersions(nextVersion) {
  for (const file of cargoTomlFiles) {
    const contents = await readText(file.path);
    const updated = updateCargoToml(contents, nextVersion, file.pathDependencies);
    await writeText(file.path, updated);
  }

  const cargoLockContents = await readText("Cargo.lock");
  await writeText("Cargo.lock", updateCargoLock(cargoLockContents, nextVersion));
}

function printMismatch(versions) {
  for (const entry of versions) {
    console.error(`- ${entry.source}: ${entry.version}`);
  }
}

async function stagePaths(paths) {
  const result = spawnSync("git", ["add", "--", ...paths], {
    cwd: repoRoot,
    stdio: "inherit",
    shell: false,
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

async function main() {
  const versions = await collectAllVersions();
  const uniqueVersions = getUniqueVersions(versions);

  if (command === "check") {
    if (uniqueVersions.length > 1) {
      console.error("Version mismatch detected across manifests:");
      printMismatch(versions);
      process.exit(1);
    }

    console.log(`Version sync ok: ${uniqueVersions[0]}`);
    return;
  }

  const currentVersion = uniqueVersions
    .map((version) => parseSemver(version))
    .sort(compareSemver)
    .at(-1);

  if (!currentVersion) {
    throw new Error("Could not determine a current version");
  }

  const nextVersion = incrementPatch(
    `${currentVersion.major}.${currentVersion.minor}.${currentVersion.patch}`,
  );

  await updateJsonVersions(nextVersion);
  await writeCargoVersions(nextVersion);
  await stagePaths([
    ...jsonFiles,
    ...cargoTomlFiles.map((file) => file.path),
    "Cargo.lock",
  ]);

  console.log(`Bumped synchronized version to ${nextVersion}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
