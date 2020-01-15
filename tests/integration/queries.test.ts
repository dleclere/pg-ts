import { Either } from "fp-ts/lib/Either";
import { head, NonEmptyArray, tail } from "fp-ts/lib/NonEmptyArray";
import { fold as foldO } from "fp-ts/lib/Option";
import { pipe } from "fp-ts/lib/pipeable";
import { ask, fold, leftTask, map, right, rightTask } from "fp-ts/lib/ReaderTaskEither";
import { of } from "fp-ts/lib/Task";
import * as t from "io-ts";
import {
  camelCasedQueries,
  ConnectedEnvironment,
  isRowCountError,
  isRowValidationError,
  PgRowCountError,
  PgRowValidationError,
  SQL,
} from "../../src";
import { connectionTest } from "./support/testTypes";
import { Unit, Void } from "./support/types";

const { queryNone, queryOne, queryAny, queryOneOrMore, queryOneOrNone } = camelCasedQueries;

describe("queries", () => {
  test("queryNone with a query that returns 0 rows returns void", () =>
    connectionTest(queryNone(SQL`SELECT * FROM units WHERE id = 999`)));

  test("queryNone with a query that returns > 0 rows returns PgRowCountError", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, void>(),
        map(() => {
          const foo = pipe(
            queryNone(SQL`SELECT * FROM units`),
            fold<ConnectedEnvironment, unknown, unknown, Either<void, unknown>>(
              error => {
                if (isRowCountError(error)) {
                  expect(error).toMatchObject({ expected: "0", received: ">= 1" } as Partial<
                    PgRowCountError
                  >);
                  return rightTask(of(error));
                }

                expect(error).toBeInstanceOf(PgRowCountError);

                return leftTask(of(Void));
              },
              () => fail(new Error("Query should have raised a PgRowCountError.")),
            ),
          );

          return foo;
        }),
      ),
    ));

  test("queryOne with a query that returns 1 parseable row returns a single parsed type", () =>
    connectionTest(
      pipe(
        queryOne(Unit, SQL`SELECT * FROM units WHERE id = 2`),
        map(unit => expect(unit).toMatchObject({ id: 2, name: "Bike" })),
      ),
    ));

  test("queryOne with a query that returns 1 unparseable row returns a validation error", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, void>(),
        map(() =>
          pipe(
            queryOne(Unit, SQL`SELECT 'foo' as id, 1 as name FROM units WHERE id = 2`),
            fold(
              err => {
                if (!isRowValidationError(err)) {
                  return fail(new Error("Query should have raised a PgRowValidationError."));
                }

                return rightTask(of(err.errors));
              },
              () => fail(new Error("Query should have raised a PgRowValidationError.")),
            ),
          ),
        ),
      ),
    ));

  test("queryOne with a query that returns 0 rows returns PgRowCountError", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, void>(),
        map(() =>
          pipe(
            queryOne(Unit, SQL`SELECT * FROM units WHERE id = 999`),
            fold(
              error => {
                if (!isRowCountError(error)) {
                  return fail(new Error("Query should have raised a PgRowCountError."));
                }

                expect(error).toMatchObject({ expected: "1", received: "0" } as Partial<
                  PgRowCountError
                >);

                return rightTask(of(error));
              },
              () => fail(new Error("Query should have raised a PgRowCountError.")),
            ),
          ),
        ),
      ),
    ));

  test("queryOne with a query that returns > 1 rows returns PgRowCountError", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, void>(),
        map(() =>
          pipe(
            queryOne(Unit, SQL`SELECT * FROM units`),
            fold(
              error => {
                if (!isRowCountError(error)) {
                  fail(new Error("Query should have raised a PgRowCountError."));
                }

                expect(error).toMatchObject({ expected: "1", received: "> 1" } as Partial<
                  PgRowCountError
                >);

                return rightTask(of(error));
              },
              () => fail(new Error("Query should have raised a PgRowCountError.")),
            ),
          ),
        ),
      ),
    ));

  test("queryOneOrMore with a query that returns 1 parseable row returns an array of a single parsed type", () =>
    connectionTest(
      pipe(
        queryOneOrMore(Unit, SQL`SELECT * FROM units WHERE id = 2`),
        map(units => {
          expect(head(units)).toMatchObject({ id: 2, name: "Bike" });
          expect(tail(units)).toHaveLength(0);
        }),
      ),
    ));

  test("queryOneOrMore with a query that returns 2 parseable rows returns an array of two parsed types", () =>
    connectionTest(
      pipe(
        queryOneOrMore(Unit, SQL`SELECT * FROM units WHERE name = 'Car' ORDER BY id`),
        map(units => {
          expect(units).toHaveLength(2);
          expect(units[0]).toMatchObject({ id: 1, name: "Car" });
          expect(units[1]).toMatchObject({ id: 4, name: "Car" });
        }),
      ),
    ));

  test("queryOneOrMore with a query that returns 0 rows returns PgRowCountError", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, void>(),
        map(() =>
          pipe(
            queryOneOrMore(Unit, SQL`SELECT * FROM units WHERE id = 0`),
            fold(
              error => {
                if (!isRowCountError(error)) {
                  fail(new Error("Query should have raised a PgRowCountError."));
                }

                expect(error).toMatchObject({ expected: ">= 1", received: "0" } as Partial<
                  PgRowCountError
                >);

                return rightTask(of(error));
              },
              () => fail(new Error("Query should have raised a PgRowCountError.")),
            ),
          ),
        ),
      ),
    ));

  test("queryOneOrNone with a query that returns 1 parseable row returns a Some of a single parsed type", () =>
    connectionTest(
      pipe(
        queryOneOrNone(Unit, SQL`SELECT * FROM units WHERE id = 2`),
        map(unitO => {
          return pipe(
            unitO,
            foldO(
              () => fail(new Error("Query should have returned a Some.")),
              unit => {
                expect(unit).toMatchObject({ id: 2, name: "Bike" });
              },
            ),
          );
        }),
      ),
    ));

  test("queryOneOrNone with a query that returns 0 rows returns a None", () =>
    connectionTest(
      pipe(
        queryOneOrNone(Unit, SQL`SELECT * FROM units WHERE id = 0`),
        map(unitO => {
          return pipe(
            unitO,
            foldO(
              () => {
                return;
              },
              () => {
                fail(new Error("Query should have returned a None."));
              },
            ),
          );
        }),
      ),
    ));

  test("queryOneOrNone with a query that returns 2 rows returns PgRowCountError", () =>
    connectionTest(
      pipe(
        ask<ConnectedEnvironment, void>(),
        map(() =>
          pipe(
            queryOneOrNone(Unit, SQL`SELECT * FROM units WHERE name = 'Car'`),
            fold(
              error => {
                if (!isRowCountError(error)) {
                  fail(new Error("Query should have raised a PgRowCountError."));
                }

                expect(error).toMatchObject({ expected: "0 or 1", received: "> 1" } as Partial<
                  PgRowCountError
                >);

                return rightTask<ConnectedEnvironment, unknown, PgRowCountError>(of(error));
              },
              () => fail(new Error("Query should have raised a PgRowCountError.")),
            ),
          ),
        ),
      ),
    ));

  test("queryAny with a query that returns 0 rows returns an empty array", () =>
    connectionTest(
      pipe(
        camelCasedQueries.queryAny(Unit, SQL`SELECT * FROM units WHERE id = 0`),
        map(units => {
          expect(units).toHaveLength(0);
        }),
      ),
    ));

  test("queryAny with a query that returns 1 rows returns an array of 1 parsed row", () =>
    connectionTest(
      pipe(
        queryAny(Unit, SQL`SELECT * FROM units WHERE id = 1`),
        map(units => {
          expect(units).toHaveLength(1);
          expect(units[0]).toMatchObject({ id: 1, name: "Car" });
        }),
      ),
    ));

  test("queryAny with a query that returns 2 rows returns an array of 2 parsed rows", () =>
    connectionTest(
      pipe(
        queryAny(Unit, SQL`SELECT * FROM units WHERE name = 'Car' ORDER BY id`),
        map(units => {
          expect(units).toHaveLength(2);
          expect(units[0]).toMatchObject({ id: 1, name: "Car" });
          expect(units[1]).toMatchObject({ id: 4, name: "Car" });
        }),
      ),
    ));

  test("queryAny with a json parameter that has a NonEmptyArray inside", () =>
    connectionTest(
      pipe(
        queryAny(t.any, SQL`SELECT ${{ foo: [1, 2] as NonEmptyArray<number> }}::json`),
        map(results => {
          expect(results).toEqual([{ json: { foo: [1, 2] } }]);
        }),
      ),
    ));

  test("queryAny with two empty arrays with different type assertions", () =>
    connectionTest(
      pipe(
        queryAny(t.any, SQL`SELECT ${[]}::uuid[] AS a, ${[]}::int[] as b`),
        map(results => {
          expect(results).toEqual([{ a: [], b: [] }]);
        }),
      ),
    ));

  test("queryOneOrNone with a query that does not select enough columns to satisfy the row type", () =>
    connectionTest(
      pipe(
        queryOneOrNone(Unit, SQL`SELECT name FROM units LIMIT 1`),
        fold(error => {
          expect(error).toBeInstanceOf(PgRowValidationError);
          return right(Void);
        }, fail),
      ),
    ));
});
