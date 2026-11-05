import { describe, it, expect } from 'vitest';
import { Type } from '@sinclair/typebox';
import { validateToolArguments } from './validation';

describe('validateToolArguments', () => {
  const schema = Type.Object({
    name: Type.String(),
    count: Type.Number(),
  });

  it('returns args unchanged when they already match the schema', () => {
    const args = { name: 'hello', count: 3 };
    expect(validateToolArguments('myTool', schema, args)).toEqual(args);
  });

  it('coerces string numbers to numeric type via Value.Convert', () => {
    const args = { name: 'hello', count: '42' };
    const result = validateToolArguments('myTool', schema, args) as { name: string; count: number };
    expect(result.count).toBe(42);
    expect(typeof result.count).toBe('number');
  });

  it('throws a formatted error when args cannot be coerced into the schema', () => {
    const args = { name: 'hello', count: 'not-a-number' };
    expect(() => validateToolArguments('myTool', schema, args)).toThrow(
      /Tool "myTool" received invalid arguments:/,
    );
  });

  it('throws mentioning the offending path when a required field is missing', () => {
    const args = { name: 'hello' };
    expect(() => validateToolArguments('myTool', schema, args)).toThrow(
      /Tool "myTool" received invalid arguments:/,
    );
  });
});
