import { Either } from "fp-ts/lib/Either";
import { pipe } from "fp-ts/lib/pipeable";
import { chain as chainRTE, ReaderTaskEither } from "fp-ts/lib/ReaderTaskEither";
import { chain, fold, left, right } from "fp-ts/lib/TaskEither";
import {
  camelCasedQueries,
  ConnectedEnvironment,
  ConnectionPool,
  makeConnectionPool,
  PgPoolCheckoutError,
  PgPoolCreationError,
  PgTypeParserSetupError,
  PgUnhandledConnectionError,
  SQL,
} from "../../../src";
import { QueryNoneError } from "../../../src/query";
import { eitherToPromise } from "../../../src/utils/eitherToPromise";
import { getPoolConfig, truncate } from "./db";

const { queryNone } = camelCasedQueries;

export const connectionTest = <L, A>(
  program: ReaderTaskEither<ConnectedEnvironment, L, A>,
): Promise<A> => {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    throw new Error("DATABASE_URL environment variable not found");
  }

  const createTable = queryNone(
    SQL`CREATE TABLE IF NOT EXISTS units (id integer, name varchar(100));`,
  );

  const insertUnits = queryNone(
    SQL`INSERT INTO units (id, name) VALUES (1, 'Car'), (2, 'Bike'), (3, 'Motorbike'), (4, 'Car');`,
  );

  const prepareDb = pipe(
    createTable,
    chainRTE(() => truncate("units")),
    chainRTE(() => insertUnits),
  );

  type ProgramError =
    | PgPoolCheckoutError
    | PgPoolCreationError
    | PgTypeParserSetupError
    | PgUnhandledConnectionError
    | QueryNoneError
    | L;

  return pipe(
    makeConnectionPool(getPoolConfig(connectionString)),
    chain((pool: ConnectionPool) => {
      const pp = pool.withConnection(
        pipe(
          prepareDb,
          chainRTE<ConnectedEnvironment, ProgramError, unknown, A>(() => program),
        ),
      );
      return pipe(
        pp,
        fold<ProgramError, A, Either<ProgramError, A>>(
          err => {
            const f = fold(
              () => left(err),
              () => left(err),
            );
            return pipe(pool.end(), f);
          },
          result => {
            const f = fold(
              () => right(result),
              () => right(result),
            );
            return pipe(pool.end(), f);
          },
        ),
      );
    }),
  )().then(eitherToPromise) as Promise<A>;
};
