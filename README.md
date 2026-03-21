# tsql-wasm

Base de um engine T-SQL embutível em WASM, com API estilo PGlite, mas implementado como projeto próprio.

## O que já vem neste recorte

- `CREATE TABLE`
- `INSERT INTO ... VALUES`
- `INSERT INTO ... DEFAULT VALUES`
- `SELECT`
- `SELECT TOP n`
- `WHERE`
- `ORDER BY`
- `GROUP BY`
- `INNER JOIN`
- `LEFT JOIN`
- `UPDATE`
- `DELETE`
- `COUNT(*)`
- alias em `SELECT ... AS ...`
- `IS NULL` / `IS NOT NULL`
- `CAST(expr AS TYPE)`
- `CONVERT(TYPE, expr)`
- `IDENTITY(1,1)`
- `DEFAULT`
- `NOT NULL`
- `PRIMARY KEY` lógico
- `GETDATE()`
- `ISNULL(a, b)`
- schemas com `dbo`
- wrapper `wasm-bindgen`
- client TS simples
- playground Vite

## O que ainda é MVP

- sem persistência
- sem índices
- sem transações reais
- sem `HAVING`
- sem `JOIN` de múltiplas condições complexas
- sem catálogo `sys.*`
- sem fidelidade total de `NULL` / three-valued logic
- sem T-SQL procedural (`DECLARE`, `BEGIN/END`, procedures)


## Refatoração SOLID

O core passou por uma refatoração estrutural para reduzir acoplamento e separar responsabilidades. O resumo está em `docs/solid-review.md`.

Mudanças principais:

- parser separado em `statements`, `expression` e `utils`
- engine reduzido a orquestração
- executores dedicados para schema, mutation e query
- evaluator e value ops extraídos do engine
- abstração `Clock` para suportar inversão de dependência em `GETDATE()`

## Build

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-pack
wasm-pack build crates/tsql_wasm --target web --out-dir crates/tsql_wasm/pkg
```

## Client / playground

```bash
cd packages/client
npm install
npm run build

cd ../playground
npm install
npm run dev
```

## Exemplo

```ts
const db = await TsqlDatabase.create();

await db.exec(`
  CREATE TABLE dbo.Users (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1
  )
`);

await db.exec(`
  CREATE TABLE dbo.Posts (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    UserId INT NOT NULL,
    Title NVARCHAR(100) NOT NULL
  )
`);

await db.exec(`
  INSERT INTO dbo.Users (Name, IsActive)
  VALUES (N'Ana', 1), (N'Bruno', 0), (N'Carla', 1)
`);

await db.exec(`
  INSERT INTO dbo.Posts (UserId, Title)
  VALUES (1, N'A'), (1, N'B'), (3, N'C')
`);

const joined = await db.query(`
  SELECT TOP 2 u.Name AS UserName, p.Title,
         CAST(u.Id AS BIGINT) AS UserId64,
         CONVERT(NVARCHAR(20), u.IsActive) AS ActiveText
  FROM dbo.Users u
  LEFT JOIN dbo.Posts p ON u.Id = p.UserId
  WHERE u.IsActive = 1
  ORDER BY u.Id DESC
`);

const grouped = await db.query(`
  SELECT u.Name, COUNT(*) AS TotalPosts
  FROM dbo.Users u
  INNER JOIN dbo.Posts p ON u.Id = p.UserId
  GROUP BY u.Name
  ORDER BY u.Name ASC
`);
```

## Observação

Aqui no ambiente em que gerei o projeto não havia toolchain Rust instalada, então eu consegui montar e empacotar os arquivos, mas não rodar `cargo check` localmente.
