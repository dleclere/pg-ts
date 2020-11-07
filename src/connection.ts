import { tryCatch, TaskEither } from "fp-ts/lib/TaskEither";
import { Either, left as leftE, right as rightE } from "fp-ts/lib/Either";
import { mixed } from "io-ts";
import { QueryConfig } from "pg";
import * as pg from "pg";
import { makeDriverQueryError } from "./errors";
import { Connection } from "./types";
import { CopyStreamQuery } from "pg-copy-streams";
import * as stream from "stream";
import { Lazy } from "fp-ts/lib/function";

export const wrapPoolClient = (poolClient: pg.PoolClient): Connection => ({
  query: (config: QueryConfig, context: mixed) =>
    tryCatch(() => poolClient.query(config), makeDriverQueryError(config, context)),

  release: err => poolClient.release(err),

  copyFrom: (query: CopyStreamQuery) => (
    data: Lazy<stream.Readable>,
  ): TaskEither<Error, undefined> => () =>
    new Promise<Either<Error, undefined>>((resolve, reject) => {
      const readStream = data();
      const writeStream = poolClient.query(query);
      const onError = (e: Error) => {
        writeStream.removeAllListeners();
        readStream.removeAllListeners();
        reject(leftE(e));
      };
      writeStream.once("error", onError);
      readStream.once("error", onError);
      writeStream.once("finish", () => {
        writeStream.removeAllListeners();
        readStream.removeAllListeners();
        resolve(rightE(void 0));
      });
      readStream.pipe(writeStream);
    }),
});
