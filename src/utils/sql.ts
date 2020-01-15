import { isNone, isSome } from "fp-ts/lib/Option";
import { mixed } from "io-ts";
import { forOwn, isEqual, isPlainObject } from "lodash";
import * as pg from "pg";

const normaliseValue = (value: any): any => {
  if (value == null) {
    return value;
  }

  if (isNone(value)) {
    return normaliseValue(undefined);
  }

  if (isSome(value)) {
    return normaliseValue(value.value);
  }

  if (Array.isArray(value)) {
    return value.map(normaliseValue);
  }

  if (isPlainObject(value)) {
    const next: any = {};

    forOwn(value, (v, k) => {
      // tslint:disable-next-line:no-expression-statement no-object-mutation
      next[k] = normaliseValue(v);
    });

    return next;
  }

  return value;
};

type IndexGetter = (value: mixed) => string;

export const SQLFragment = (parts: TemplateStringsArray, ...inValues: mixed[]) => (
  getValueIndex: IndexGetter,
): string =>
  parts.reduce((prev, curr, valIdx) => `${prev}${getValueIndex(inValues[valIdx - 1])}${curr}`);

export const SQL = (parts: TemplateStringsArray, ...inValues: any[]): pg.QueryConfig => {
  const outValues: mixed[] = [];

  const getValueIndex = (value: mixed): string => {
    const normalisedValue = normaliseValue(value);

    if (normalisedValue == null) {
      return `NULL`;
    }

    const found = outValues.findIndex(
      o => !Array.isArray(normalisedValue) && isEqual(o, normalisedValue),
    );

    if (found > -1) {
      return `$${found + 1}`;
    }
    outValues.push(normalisedValue);
    return `$${outValues.length}`;
  };

  const outText = parts.reduce((prev, curr) => {
    const value = inValues.shift();
    return typeof value === "function"
      ? `${prev}${value(getValueIndex)}${curr}`
      : `${prev}${getValueIndex(value)}${curr}`;
  });

  return { text: outText, values: outValues };
};

export const named = (name: string, query: pg.QueryConfig) => ({
  ...query,
  name,
});
