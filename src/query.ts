import { head } from "fp-ts/lib/Array";
import {
  chain as chainE,
  Either,
  fold as foldE,
  fromPredicate,
  map as mapE,
  mapLeft as mapLeftE,
  right,
} from "fp-ts/lib/Either";
import { constant, identity, Predicate } from "fp-ts/lib/function";
import { NonEmptyArray } from "fp-ts/lib/NonEmptyArray";
import { fold as foldO, none, Option, some } from "fp-ts/lib/Option";
import { pipe } from "fp-ts/lib/pipeable";
import { ask, chain, fromTaskEither, ReaderTaskEither } from "fp-ts/lib/ReaderTaskEither";
import { chain as chainTE, fromEither } from "fp-ts/lib/TaskEither";
import * as t from "io-ts";
import { QueryConfig } from "pg";
import {
  makeRowValidationError,
  PgDriverQueryError,
  PgRowCountError,
  PgRowValidationError,
} from "./errors";
import {
  ConnectedEnvironment,
  Connection,
  connectionLens,
  QueryResult,
  RowTransformer,
} from "./types";
import { defaultCamelCaser } from "./utils/camelify";

const executeQuery = (query: QueryConfig, context: t.mixed) => (connection: Connection) =>
  connection.query(query, context);

const isNoneResult: Predicate<QueryResult> = ({ rows }) => rows.length === 0;
const isNonEmptyResult: Predicate<QueryResult> = ({ rows }) => rows.length > 0;
const isOneResult: Predicate<QueryResult> = ({ rows }) => rows.length === 1;
const isOneOrNoneResult: Predicate<QueryResult> = _ => isNoneResult(_) || isOneResult(_);

const expectedAtLeastOneErrorFailure = (query: QueryConfig, context: t.mixed) =>
  new PgRowCountError(query, ">= 1", "0", context);

const expectedNoneFoundSomeErrorFailure = (query: QueryConfig, context: t.mixed) =>
  new PgRowCountError(query, "0", ">= 1", context);

const expectedOneFoundManyErrorFailure = (query: QueryConfig, context: t.mixed) =>
  new PgRowCountError(query, "1", "> 1", context);

const expectedOneFoundNoneErrorFailure = (query: QueryConfig, context: t.mixed) =>
  new PgRowCountError(query, "1", "0", context);

const expectedOneOrNoneErrorFailure = (query: QueryConfig, context: t.mixed) =>
  new PgRowCountError(query, "0 or 1", "> 1", context);

export type QueryAnyError = PgDriverQueryError | PgRowValidationError;
export type QueryNoneError = PgDriverQueryError | PgRowCountError;
export type QueryOneError = PgDriverQueryError | PgRowValidationError | PgRowCountError;
export type QueryOneOrMoreError = PgDriverQueryError | PgRowValidationError | PgRowCountError;
export type QueryOneOrNoneError = PgDriverQueryError | PgRowValidationError | PgRowCountError;

const queryAny = (transformer: RowTransformer = identity) => <A = any>(
  type: t.Type<A, any, t.mixed>,
  query: QueryConfig,
  context?: t.mixed,
): ReaderTaskEither<ConnectedEnvironment, QueryAnyError, A[]> =>
  pipe(
    ask<ConnectedEnvironment, QueryAnyError>(),
    chain(environment => {
      const connection = connectionLens.get(environment);
      return fromTaskEither(
        pipe(
          executeQuery(query, context)(connection),
          chainTE<QueryAnyError, QueryResult, A[]>(qr => {
            const transformedRows = transformer(qr.rows);

            return fromEither(
              mapLeftE(makeRowValidationError(type, transformedRows, context))(
                t.array(type).decode(transformedRows),
              ),
            );
          }),
        ),
      );
    }),
  );

const queryNone = (
  query: QueryConfig,
  context?: t.mixed,
): ReaderTaskEither<ConnectedEnvironment, QueryNoneError, void> =>
  pipe(
    ask<ConnectedEnvironment, QueryNoneError>(),
    chain(environment => {
      const connection = connectionLens.get(environment);
      return fromTaskEither(
        pipe(
          executeQuery(query, context)(connection),
          chainTE<QueryNoneError, QueryResult, void>(result => {
            return fromEither(
              // tslint:disable-next-line: no-empty
              mapE(() => {})(
                fromPredicate(
                  isNoneResult,
                  constant(expectedNoneFoundSomeErrorFailure(query, context)),
                )(result),
              ),
            );
          }),
        ),
      );
    }),
  );

const queryOne = (transformer: RowTransformer = identity) => <A = any>(
  type: t.Type<A, any, t.mixed>,
  query: QueryConfig,
  context?: t.mixed,
): ReaderTaskEither<ConnectedEnvironment, QueryOneError, A> =>
  pipe(
    ask<ConnectedEnvironment, QueryOneError>(),
    chain(environment => {
      const connection = connectionLens.get(environment);
      return fromTaskEither(
        pipe(
          executeQuery(query, context)(connection),
          chainTE<QueryOneError, QueryResult, A>(result =>
            fromEither(
              pipe(
                fromPredicate(isOneResult, r =>
                  foldE(
                    constant(expectedOneFoundManyErrorFailure(query, context)),
                    constant(expectedOneFoundNoneErrorFailure(query, context)),
                  )(fromPredicate(isNoneResult, identity)(r)),
                )(result),
                mapE(({ rows }) => transformer(rows)[0]),
                chainE<QueryOneError, unknown, A>(row =>
                  mapLeftE(makeRowValidationError(type, row, context))(type.decode(row)),
                ),
              ),
            ),
          ),
        ),
      );
    }),
  );

const queryOneOrMore = (transformer: RowTransformer = identity) => <A = any>(
  type: t.Type<A, any, t.mixed>,
  query: QueryConfig,
  context?: t.mixed,
): ReaderTaskEither<ConnectedEnvironment, QueryOneOrMoreError, NonEmptyArray<A>> =>
  pipe(
    ask<ConnectedEnvironment, QueryOneOrMoreError>(),
    chain(environment => {
      const connection = connectionLens.get(environment);
      return fromTaskEither(
        pipe(
          executeQuery(query, context)(connection),
          chainTE(result =>
            fromEither(
              pipe(
                fromPredicate(
                  isNonEmptyResult,
                  constant(expectedAtLeastOneErrorFailure(query, context)),
                )(result),
                mapE(({ rows }) => transformer(rows)),
                chainE<QueryOneOrMoreError, unknown[], A[]>(rows =>
                  mapLeftE(makeRowValidationError(type, rows, context))(t.array(type).decode(rows)),
                ),
                mapE(rows => rows as NonEmptyArray<A>),
              ),
            ),
          ),
        ),
      );
    }),
  );

const queryOneOrNone = (transformer: RowTransformer = identity) => <A = any>(
  type: t.Type<A, any, t.mixed>,
  query: QueryConfig,
  context?: t.mixed,
): ReaderTaskEither<ConnectedEnvironment, QueryOneOrNoneError, Option<A>> =>
  pipe(
    ask<ConnectedEnvironment, QueryOneOrNoneError>(),
    chain(environment => {
      const connection = connectionLens.get(environment);
      return fromTaskEither(
        pipe(
          executeQuery(query, context)(connection),
          chainTE(result =>
            fromEither(
              pipe(
                fromPredicate(
                  isOneOrNoneResult,
                  constant(expectedOneOrNoneErrorFailure(query, context)),
                )(result),
                mapE(({ rows }) => transformer(rows)),
                chainE<QueryOneOrNoneError, unknown[], Option<A>>(rows =>
                  pipe(
                    head(rows),
                    foldO<unknown, Either<t.Errors, Option<A>>>(
                      () => right(none),
                      row =>
                        pipe(
                          type.decode(row),
                          mapE(_ => some(_)),
                        ),
                    ),
                    mapLeftE(makeRowValidationError(type, rows, context)),
                  ),
                ),
              ),
            ),
          ),
        ),
      );
    }),
  );

export const configurableQueries = {
  queryAny,
  queryNone,
  queryOne,
  queryOneOrMore,
  queryOneOrNone,
};

export const camelCasedQueries = {
  queryAny: queryAny(defaultCamelCaser),
  queryNone,
  queryOne: queryOne(defaultCamelCaser),
  queryOneOrMore: queryOneOrMore(defaultCamelCaser),
  queryOneOrNone: queryOneOrNone(defaultCamelCaser),
};
