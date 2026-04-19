# Plan: Create SQL Server 2025 Implementation Status Document

This plan outlines the creation of a new English documentation file that details the current implementation status of reserved keywords, system stored procedures, and system catalog views in Iridium SQL, based on the reference files in the `docs/` directory.

## User Request
"look in docs the files with prefix sql-server-2025 and create a new docs in English with a table saying our implementation status for each one"

## Proposed Strategy

1.  **Analysis Phase**:
    *   Parse `docs/sql-server-2025-palavras-reservadas-completas.md` to extract the list of 185+ keywords.
    *   Parse `docs/sql-server-2025-procedimentos-sp-completos.md` to extract the list of 320+ `sp_` procedures.
    *   Parse `docs/sql-server-2025-sys-catalogo-consolidado.md` to extract the list of system views and columns.
    *   Cross-reference these lists with the actual codebase:
        *   Keywords: `crates/iridium_core/src/parser/token/keyword.rs`
        *   Procedures: `crates/iridium_core/src/executor/script/procedural/system_procedures.rs`
        *   Catalog: `crates/iridium_core/src/executor/metadata/sys/mod.rs` and related files in `crates/iridium_core/src/executor/metadata/sys/`.

2.  **Implementation Phase**:
    *   Create a new file `docs/sql-server-2025-implementation-status.md` (in English).
    *   The file will contain detailed tables for each category showing the "Implemented" or "Pending" status.
    *   Add a summary section with completion percentages.

3.  **Verification Phase**:
    *   Verify that all supported items in the codebase are marked as "Implemented".
    *   Ensure the document is correctly formatted and uses clear English.

## Execution Steps

1.  **Extract Data**: (Already partially done in Research phase)
2.  **Generate Document**: I will create the `docs/sql-server-2025-implementation-status.md` file with the compiled data.
3.  **Final Review**: Review the document for accuracy.

## Approval Required
Does this strategy meet your expectations? If so, I will proceed to generate the document.
