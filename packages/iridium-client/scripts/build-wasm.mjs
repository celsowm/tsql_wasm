import { spawnSync } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const packageDir = resolve(scriptDir, "..");
const repoDir = resolve(packageDir, "../..");
const crateDir = resolve(repoDir, "crates/iridium_wasm");
const outDir = resolve(packageDir, "src/wasm");

const result = spawnSync(
  "wasm-pack",
  [
    "build",
    crateDir,
    "--target",
    "web",
    "--out-dir",
    outDir,
    "--out-name",
    "iridium_wasm",
  ],
  {
    stdio: "inherit",
    cwd: packageDir,
    shell: false,
  },
);

if (result.error) {
  throw result.error;
}

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
