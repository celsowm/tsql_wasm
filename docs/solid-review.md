# SOLID review and refactor notes

## Principais problemas encontrados

### S — Single Responsibility Principle
Antes, `parser.rs` e `executor/engine.rs` concentravam parsing, validação, avaliação de expressão, coerção de tipo, execução de query, DML e regras auxiliares no mesmo arquivo.

### O — Open/Closed Principle
Adicionar funções como `GETDATE()` ou mudar uma regra de execução exigia editar o arquivo monolítico do engine, aumentando acoplamento e risco de regressão.

### L — Liskov Substitution Principle
Não havia hierarquias grandes quebrando LSP, então aqui o problema era menor do que nos outros princípios.

### I — Interface Segregation Principle
O `Engine` conhecia detalhes demais de schema, mutation, query e avaliação de expressão ao mesmo tempo.

### D — Dependency Inversion Principle
Regras como `GETDATE()` dependiam diretamente de implementação concreta e fixa dentro do evaluator. Não existia abstração para fonte de tempo.

## Refatorações aplicadas

### Parser
O parser foi dividido em módulos com responsabilidade clara:

- `parser/mod.rs`: dispatch de statements
- `parser/statements.rs`: parsing de `CREATE/INSERT/SELECT/UPDATE/DELETE`
- `parser/expression.rs`: parsing de expressões
- `parser/utils.rs`: utilitários de tokenização e split top-level

### Executor
O executor foi dividido em componentes de aplicação:

- `executor/engine.rs`: orquestração
- `executor/schema.rs`: operações de schema
- `executor/mutation.rs`: `INSERT/UPDATE/DELETE`
- `executor/query.rs`: `SELECT`, join, grouping e projection
- `executor/evaluator.rs`: avaliação de expressões
- `executor/value_ops.rs`: coerção e comparação de valores
- `executor/type_mapping.rs`: mapeamento `DataTypeSpec -> DataType`
- `executor/model.rs`: tipos internos compartilhados
- `executor/clock.rs`: abstração de relógio

### Dependency inversion
Foi introduzido o trait `Clock`, com `SystemClock` e `FixedClock`, para que `GETDATE()` dependa de abstração, não de implementação fixa.

`Engine::new()` usa `SystemClock`, e `Engine::with_clock(...)` permite injeção explícita para testes.

## Resultado prático

O código continua com a mesma API pública principal, mas agora está mais coeso e com pontos de extensão mais claros.

As áreas que mais melhoraram foram:

- coesão
- legibilidade
- testabilidade
- previsibilidade de mudança
- isolamento de regras de negócio

## O que ainda pode evoluir

Mesmo após essa refatoração, ainda existe espaço para aprofundar SOLID em etapas futuras:

- extrair um registry de funções T-SQL para OCP mais forte
- criar traits de storage e catalog para DIP além do relógio
- separar planner físico de executor de query
- introduzir testes unitários por módulo do core
