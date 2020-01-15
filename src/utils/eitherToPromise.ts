import { Either, fold } from "fp-ts/lib/Either";

export const eitherToPromise = <L, R>(either: Either<L, R>) =>
  fold(
    l => Promise.reject(l),
    r => Promise.resolve(r),
  )(either);
