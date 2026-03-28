# SOLID review and refactor notes

## Principais problemas encontrados

### S — Single Responsibility Principle
Antes, `parser.rs` e `executor/engine.rs` concentravam parsing, validação, avaliação de expressão, coerção de tipo, execução de query, DML e regras auxiliares no mesmo arquivo.
Também, `ast.rs` era um arquivo monolítico de ~800 linhas contendo todas as definições de nós da AST, dificultando a manutenção e evolução independente de diferentes partes da linguagem.

### O — Open/Closed Principle
Adicionar funções como `GETDATE()` ou mudar uma regra de execução exigia editar o arquivo monolítico do engine, aumentando acoplamento e risco de regressão.

### L — Liskov Substitution Principle
Não havia hierarquias grandes quebrando LSP, então aqui o problema era menor do que nos outros princípios.

### I — Interface Segregation Principle
O `Engine` conhecia detalhes demais de schema, mutation, query e avaliação de expressão ao mesmo tempo.

### D — Dependency Inversion Principle
Regras como `GETDATE()` dependiam diretamente de implementação concreta e fixa dentro do evaluator. Não existia abstração para fonte de tempo.

## Refatorações aplicadas

### AST (Modularização)
O arquivo `ast.rs` foi refatorado em um módulo `ast/` com sub-módulos organizados por domínio:
- `ast/common.rs`: Tipos básicos como `ObjectName`, `TableName`, `TableRef`.
- `ast/expressions.rs`: `Expr` e operadores relacionados.
- `ast/data_types.rs`: Definições de `DataTypeSpec`.
- `ast/statements/`: Sub-diretório para comandos SQL.
  - `mod.rs`: Enum principal `Statement` e tipos compartilhados.
  - `dml.rs`: Comandos de manipulação de dados (`INSERT`, `UPDATE`, `DELETE`, `MERGE`).
  - `ddl.rs`: Comandos de definição de dados (`CREATE`, `ALTER`, `DROP`).
  - `query.rs`: Comandos de consulta (`SELECT`, `UNION`).
  - `procedural.rs`: Lógica procedural T-SQL (`IF`, `WHILE`, `EXEC`, procedimentos, gatilhos).

### Parser
O parser foi dividido em módulos com responsabilidade clara:
- `parser/mod.rs`: dispatch de statements.
- `parser/statements/`: Parsing de diferentes tipos de comandos, agora espelhando a estrutura da AST.
- `parser/expression/`: Parsing de expressões complexas.
- `parser/utils.rs`: utilitários de tokenização e split top-level.

### Executor
O executor foi dividido em componentes de aplicação:
- `executor/engine.rs`: orquestração.
- `executor/schema.rs`: operações de schema.
- `executor/mutation/`: `INSERT/UPDATE/DELETE/MERGE` agora em sub-módulos.
- `executor/query/`: `SELECT`, join, grouping e projection.
- `executor/evaluator.rs`: avaliação de expressões.
- `executor/value_ops.rs`: coerção e comparação de valores.
- `executor/type_mapping.rs`: mapeamento `DataTypeSpec -> DataType`.
- `executor/model.rs`: tipos internos compartilhados.
- `executor/clock.rs`: abstração de relógio.

### Dependency inversion
Foi introduzido o trait `Clock`, com `SystemClock` e `FixedClock`, para que `GETDATE()` dependa de abstração, não de implementação fixa.
`Engine::new()` usa `SystemClock`, e `Engine::with_clock(...)` permite injeção explícita para testes.

## Resultado prático

O código continua com a mesma API pública principal, mas agora está muito mais coeso. A modularização da AST permite que novos comandos sejam adicionados sem tocar em definições não relacionadas. A estrutura refletida entre AST, Parser e Executor facilita a navegação e o entendimento do fluxo de dados.

As áreas que mais melhoraram foram:
- coesão
- legibilidade
- testabilidade
- previsibilidade de mudança
- isolamento de regras de negócio

## O que ainda pode evoluir

Mesmo após essa refatoração, ainda existe espaço para aprofundar SOLID em etapas futuras:
- extrair um registry de funções T-SQL para OCP mais forte.
- criar traits de storage e catalog para DIP além do relógio.
- separar planner físico de executor de query.
- introduzir testes unitários por módulo do core.
