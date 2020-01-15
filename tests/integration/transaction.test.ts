import { pipe } from "fp-ts/lib/pipeable";
import { ask, chain, fold, left, map, mapLeft, right } from "fp-ts/lib/ReaderTaskEither";
import {
  camelCasedQueries,
  ConnectedEnvironment,
  isRowCountError,
  SQL,
  TransactionError,
  withTransaction,
} from "../../src";
import { QueryAnyError, QueryNoneError } from "../../src/query";
import { UnexpectedRightError } from "./support/errors";
import { connectionTest } from "./support/testTypes";
import { Unit } from "./support/types";

const { queryNone, queryAny } = camelCasedQueries;

describe("transaction", () => {
  test("two rows inserted inside a committed transaction can be found", () =>
    connectionTest(
      pipe(
        withTransaction(
          pipe(
            queryNone(SQL`INSERT INTO units (id, name) VALUES (10, 'tx first')`),
            chain(() => queryNone(SQL`INSERT INTO units (id, name) VALUES (11, 'tx second')`)),
          ),
        ),
        chain<ConnectedEnvironment, TransactionError<QueryNoneError> | QueryAnyError, void, Unit[]>(
          () => queryAny(Unit, SQL`SELECT * FROM units WHERE id >= 10 ORDER BY id`),
        ),
        map(units => {
          expect(units).toHaveLength(2);
          expect(units[0]).toMatchObject({ id: 10, name: "tx first" });
          expect(units[1]).toMatchObject({ id: 11, name: "tx second" });
        }),
      ),
    ));

  test("two rows inserted inside a rolled back transaction cannot be found", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, TransactionError<QueryNoneError> | UnexpectedRightError>(),
        chain(x),
        mapLeft(_ => _ as QueryAnyError),
        chain(() => queryAny(Unit, SQL`SELECT * FROM units WHERE id >= 10 ORDER BY id`)),
        map(units => {
          expect(units).toHaveLength(0);
        }),
      ),
    ));
});

const t = withTransaction(
  pipe(
    queryNone(SQL`INSERT INTO units (id, name) VALUES (10, 'tx first')`),
    chain(() => queryNone(SQL`INSERT INTO units (id, name) VALUES (11, 'tx second')`)),
    chain(() => queryNone(SQL`SELECT * FROM units WHERE id = 10`)),
  ),
);
const x = () =>
  pipe(
    t,
    fold(
      err => {
        if (isRowCountError(err)) {
          return right<
            ConnectedEnvironment,
            TransactionError<QueryNoneError> | UnexpectedRightError,
            void
          >((undefined as any) as void);
        }

        return left(err);
      },
      () => left(new UnexpectedRightError()),
    ),
  );
