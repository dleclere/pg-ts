import { Either, fold } from "fp-ts/lib/Either";
import { constant } from "fp-ts/lib/function";
import { pipe } from "fp-ts/lib/pipeable";
import {
  ask,
  chain as chainRTE,
  fromTaskEither,
  map,
  ReaderTaskEither,
} from "fp-ts/lib/ReaderTaskEither";
import { chain, fromEither, TaskEither, tryCatch } from "fp-ts/lib/TaskEither";
import { mixed } from "io-ts";
import {
  isDriverQueryError,
  isTransactionRollbackError,
  makeUnhandledConnectionError,
  PgDriverQueryError,
  PgTransactionRollbackError,
  PgUnhandledConnectionError,
} from "./errors";
import {
  ConnectedEnvironment,
  Connection,
  connectionLens,
  TransactionError,
  TransactionOptions,
} from "./types";
import { eitherToPromise } from "./utils/eitherToPromise";
import { SQL } from "./utils/sql";

export const defaultTxOptions: TransactionOptions = {
  context: undefined,
  deferrable: false,
  isolation: "READ COMMITTED",
  readOnly: false,
};

const beginTransactionQuery = ({
  deferrable,
  isolation,
  readOnly,
}: TransactionOptions): ReturnType<typeof SQL> => SQL`
  BEGIN TRANSACTION
  ISOLATION LEVEL ${() => isolation}
  ${() => (readOnly ? "READ ONLY" : "")}
  ${() => (deferrable ? "DEFERRABLE" : "")}
`;

const rollbackTransaction = (connection: Connection, context: mixed) => <L>(
  err: L,
): TaskEither<L | PgTransactionRollbackError, never> =>
  tryCatch(
    () =>
      connection
        .query(SQL`ROLLBACK;`, context)()
        .then(eitherToPromise)
        .catch(rollbackErr =>
          Promise.reject(new PgTransactionRollbackError(rollbackErr, err, context)),
        )
        .then(() => Promise.reject(err)),
    e => (isTransactionRollbackError(e) ? e : (e as L)),
  );

const commitTransaction = (connection: Connection, context: mixed) => <A>(
  a: A,
): TaskEither<PgDriverQueryError | PgUnhandledConnectionError, A> =>
  tryCatch(
    () =>
      connection
        .query(SQL`COMMIT;`, context)()
        .then(eitherToPromise)
        .then(constant(a)),
    e => (isDriverQueryError(e) ? e : makeUnhandledConnectionError(e)),
  );

const executeTransaction = <L, A>(
  connection: Connection,
  opts: TransactionOptions,
  program: () => Promise<Either<L, A>>,
): TaskEither<TransactionError<L>, A> => {
  const runner = () =>
    program().then(programE =>
      fold<TransactionError<L>, A, TaskEither<TransactionError<L>, A>>(
        rollbackTransaction(connection, opts.context),
        commitTransaction(connection, opts.context),
      )(programE)(),
    );
  const fromAsync = () => tryCatch(runner, makeUnhandledConnectionError);
  const p = pipe(
    connection.query(beginTransactionQuery(opts), opts.context),
    chain<TransactionError<L>, any, Either<TransactionError<L>, A>>(fromAsync),
    chain(fromEither),
  );
  return p;
};

export function withTransaction<E, L, A>(
  x: Partial<TransactionOptions>,
  y: ReaderTaskEither<E & ConnectedEnvironment, L, A>,
): ReaderTaskEither<E & ConnectedEnvironment, TransactionError<L>, A>;
export function withTransaction<E, L, A>(
  x: ReaderTaskEither<E & ConnectedEnvironment, L, A>,
): ReaderTaskEither<E & ConnectedEnvironment, TransactionError<L>, A>;
export function withTransaction<E, L, A>(
  x: any,
  y?: any,
): ReaderTaskEither<E & ConnectedEnvironment, TransactionError<L>, A> {
  const opts: TransactionOptions = y ? { ...defaultTxOptions, ...x } : defaultTxOptions;
  const program: ReaderTaskEither<E & ConnectedEnvironment, L, A> = y || x;

  const c = chainRTE(fromTaskEither);
  return pipe(
    ask<E & ConnectedEnvironment, TransactionError<L>>(),
    map(e => executeTransaction(connectionLens.get(e), opts, program(e))),
    c,
  );
}
