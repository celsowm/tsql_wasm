import { copyFile, mkdir, rm } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = dirname(fileURLToPath(import.meta.url));
const packageDir = resolve(rootDir, "..");
const sourceDir = resolve(packageDir, "src/wasm");
const targetDir = resolve(packageDir, "dist/wasm");
const wasmFiles = [
  "iridium_wasm.js",
  "iridium_wasm.d.ts",
  "iridium_wasm_bg.wasm",
  "iridium_wasm_bg.wasm.d.ts",
];

await rm(targetDir, { recursive: true, force: true });
await mkdir(targetDir, { recursive: true });

for (const fileName of wasmFiles) {
  await copyFile(resolve(sourceDir, fileName), resolve(targetDir, fileName));
}
