import { constant } from "fp-ts/lib/function";
import { map, TaskEither, tryCatch } from "fp-ts/lib/TaskEither";
import * as pg from "pg";
import { parse } from "postgres-array";
import { makeTypeParserSetupError, PgTypeParserSetupError } from "./errors";
import { parseInterval } from "./pgTypes/interval";
import { TypeParser, TypeParsers } from "./types";
import { SQL } from "./utils/sql";

interface RowType {
  oid: number;
  typarray: number;
  typname: string;
}

const typeQuery = (name: string) => SQL`
  SELECT typname, oid, typarray
  FROM pg_type
  WHERE typname = ${name}
  ORDER BY oid;`;

const arrayParser = (typeParser: TypeParser<any>) => (input: string) => parse(input, typeParser);

export const setupParsers = (pool: pg.Pool) => (
  parsers: TypeParsers,
): TaskEither<PgTypeParserSetupError, pg.Pool> => {
  const parserSet: TypeParsers = { interval: parseInterval, ...parsers };
  const queries = Object.keys(parserSet).map(name => pool.query(typeQuery(name)));

  return map(constant(pool))(
    tryCatch(
      () =>
        Promise.all(queries)
          .then(results => results.map<RowType>(({ rows: [type] }) => type))
          .then(types => {
            types.map(type => {
              const parser = parserSet[type.typname];

              pg.types.setTypeParser(type.oid, parser);

              if (type.typarray) {
                pg.types.setTypeParser(type.typarray, arrayParser(parser));
              }
            });
          }),
      makeTypeParserSetupError,
    ),
  );
};
