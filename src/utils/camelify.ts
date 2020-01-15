import { constFalse, constTrue, not, Predicate } from "fp-ts/lib/function";
import { filter, fold, fromNullable } from "fp-ts/lib/Option";
import { pipe } from "fp-ts/lib/pipeable";
import { mixed } from "io-ts";
import { camelCase, fromPairs, isArray, isDate, isObject as _isObject, toPairs } from "lodash";
import { RowTransformer } from "../types";

export interface CamelifyOptions {
  exclude: Predicate<string>;
  keyMapper: (s: string) => string;
}

const isMappable: Predicate<mixed> = x =>
  pipe(fromNullable(x), filter(isObject), filter(not(isDate)), fold(constFalse, constTrue));

const defaultOptions: CamelifyOptions = {
  exclude: (k: string) => k.startsWith("_"),
  keyMapper: camelCase,
};

const isObject = (x: mixed): x is object => _isObject(x);

const transform = (options: CamelifyOptions) => {
  const transformer = (x: mixed): mixed => {
    const { exclude, keyMapper } = options;

    if (!isMappable(x)) {
      return x;
    }

    if (isArray(x)) {
      return x.map(transformer);
    }

    if (isObject(x)) {
      return {
        ...fromPairs(toPairs(x).map(([k, v]) => [exclude(k) ? k : keyMapper(k), transformer(v)])),
      };
    }

    return x;
  };

  return transformer;
};

export const makeCamelCaser = (options?: Partial<CamelifyOptions>) => {
  const transformer = transform({ ...defaultOptions, ...options });
  return (xs: mixed[]): mixed[] => xs.map(transformer);
};

export const defaultCamelCaser: RowTransformer = makeCamelCaser();
