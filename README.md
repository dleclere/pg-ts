# pg-ts
Typescript wrapper around node-postgres

## v5.0.0
- `Either`ish functions no longer return a simple `Error` type in their `Left`s. Instead, we now
  provide a union of concrete error types that make it explicit what the failure conditions are.
  Clients can pattern match on error type using the provided `ErrorPredicate` functions, e.g,
  `isRowCountError`. Application-defined error types are propagated through the `pg-ts` stack.
- Commonly-grouped errors are provided as unions, e.g. `ConnectionError<L>`, `TransactionError<L>`.
- Renamed `withConnectionE` to `widenToConnectionE` for clarity and to differentiate from the
  `withConnectionE` function defined on `Connection`.
- `withTransaction` has been replaced by three similar functions that assist you depending on
   whether you want to go from:

     | Outer Context    | Inner Context    | Function to use     |
     |------------------|------------------|---------------------|
     | `Connection`     | `Connection`     | `withTransactionC`  |
     | `ConnectionE<E>` | `ConnectionE<E>` | `withTransactionE`  |
     | `ConnectionE<E>` | `Connection`     | `withTransactionEC` |

- Fixed the `types` setting in package.json, so imports should now default to `from "pg-ts"`
  instead of `from "pg-ts/dist"`.
- Integration tests are provided. A `DATABASE_URL` environment variable must be present. The test
  suite will create the table it needs and will truncate before each test. Every query type is
  tested and transaction commit/rollback behaviour is also verified.
