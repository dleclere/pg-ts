import { Either } from "fp-ts/lib/Either";
import { flow } from "fp-ts/lib/function";
import { mapLeft as mapLeftIOE, tryCatch as ioTryCatch } from "fp-ts/lib/IOEither";
import { fromNullable, getOrElse as getOrElseO, map as mapO } from "fp-ts/lib/Option";
import { pipe } from "fp-ts/lib/pipeable";
import {
  ask,
  chain as chainRTE,
  fromTaskEither,
  ReaderTaskEither,
} from "fp-ts/lib/ReaderTaskEither";
import {
  chain as chainTE,
  fromEither,
  fromIOEither,
  map as mapTE,
  mapLeft as mapLeftTE,
  TaskEither,
  taskEither,
  tryCatch as tryCatchTE,
} from "fp-ts/lib/TaskEither";
import * as pg from "pg";
import { wrapPoolClient } from "./connection";
import {
  isPoolCreationError,
  isTransactionRollbackError,
  makePoolCheckoutError,
  makePoolCreationError,
  makePoolShutdownError,
  makeUnhandledConnectionError,
  makeUnhandledPoolError,
  PgPoolCreationError,
  PgTypeParserSetupError,
  PgUnhandledConnectionError,
} from "./errors";
import { setupParsers } from "./parser";
import {
  ConnectedEnvironment,
  Connection,
  ConnectionError,
  ConnectionPool,
  ConnectionPoolConfig,
  ConnectionSymbol,
} from "./types";

export const makeConnectionPool = (
  poolConfig: ConnectionPoolConfig,
): TaskEither<PgPoolCreationError | PgTypeParserSetupError, ConnectionPool> => {
  const { onError, parsers } = poolConfig;

  const poolIo = mapLeftIOE(error =>
    isPoolCreationError(error) ? error : makePoolCreationError(error),
  )(
    ioTryCatch(() => {
      const pool = new pg.Pool(poolConfig);

      pool.on("error", flow(makeUnhandledPoolError, onError));

      return pool;
    }, makePoolCreationError),
  );

  const setup = (
    pool: pg.Pool,
  ): TaskEither<PgPoolCreationError | PgTypeParserSetupError, pg.Pool> => {
    return pipe(
      fromNullable(parsers),
      mapO(setupParsers(pool)),
      getOrElseO(() => taskEither.of<PgTypeParserSetupError, pg.Pool>(pool)),
    );
  };

  return pipe(fromIOEither(poolIo), chainTE(setup), mapTE(wrapConnectionPool));
};

const checkoutConnection = (pool: pg.Pool) =>
  tryCatchTE(() => pool.connect(), makePoolCheckoutError);

const executeProgramWithConnection = <E extends {}, L, A>(
  environment: E,
  program: ReaderTaskEither<E & ConnectedEnvironment, L, A>,
) => (connection: Connection): TaskEither<PgUnhandledConnectionError | L, A> => {
  return pipe(
    tryCatchTE(
      program(Object.assign({}, environment, { [ConnectionSymbol]: connection })),
      makeUnhandledConnectionError,
    ),
    chainTE<PgUnhandledConnectionError | L, Either<L, A>, A>(fromEither),
    mapLeftTE((err: PgUnhandledConnectionError | L) => {
      // If a rollback error reaches this point, we should assume the connection
      // is poisoned and ask the pool implementation to dispose of it.
      connection.release(isTransactionRollbackError(err) ? err : undefined);
      return err;
    }),
    mapTE((a: A) => {
      connection.release();
      return a;
    }),
  );
};

const withConnectionFromPool = (pool: pg.Pool) => <L, A>(
  program: ReaderTaskEither<ConnectedEnvironment, L, A>,
) =>
  pipe(
    checkoutConnection(pool),
    mapTE(wrapPoolClient),
    chainTE<ConnectionError<L>, Connection, A>(executeProgramWithConnection({}, program)),
  );

const withConnectionEFromPool = (pool: pg.Pool) => <E extends {}, L, A>(
  program: ReaderTaskEither<E & ConnectedEnvironment, L, A>,
): ReaderTaskEither<E, ConnectionError<L>, A> => {
  return pipe(
    ask<E, ConnectionError<L>>(),
    chainRTE((environment: E) => {
      const a2 = pipe(
        checkoutConnection(pool),
        mapTE(wrapPoolClient),
        chainTE<ConnectionError<L>, Connection, A>(
          executeProgramWithConnection(environment, program),
        ),
      );
      return fromTaskEither<E, ConnectionError<L>, A>(a2);
    }),
  );
};

export const wrapConnectionPool = (pool: pg.Pool): ConnectionPool => {
  return {
    end: () =>
      tryCatchTE(
        () => ((pool as any).ending ? Promise.resolve<void>(undefined) : pool.end()),
        makePoolShutdownError,
      ),

    withConnection: withConnectionFromPool(pool),
    withConnectionE: withConnectionEFromPool(pool),
  };
};
