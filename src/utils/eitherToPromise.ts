import { Either, fold } from "fp-ts/lib/Either";
import { pipe } from "fp-ts/lib/pipeable";

export const eitherToPromise = <L, R>(either: Either<L, R>): Promise<R> =>
  pipe(
    either,
    fold(
      l => Promise.reject<R>(l),
      r => Promise.resolve<R>(r),
    )
  );
