import { serialize, deserialize } from '../../src/level-1/serializer';

describe('Serializer', () => {
  describe('scalars', () => {
    const cases = [
      ['null', null, null],
      ['string', 'string', 'string'],
      ['number', 1, 1],
      ['boolean', true, true],
    ] as [string, unknown, unknown][];

    test.each(cases)('serialize %p', (_, input, output) => {
      expect(serialize(input)).toEqual(output);
    });

    test.each(cases)('deserialize %p', (_, output, input) => {
      expect(deserialize(input)).toEqual(output);
    });
  });

  describe('built-in object types', () => {
    const cases = [
      [
        'Map',
        new Map([
          ['one', 1],
          ['two', 2],
        ]),
        {
          __t: 'Map',
          __v: [
            ['one', 1],
            ['two', 2],
          ],
        },
      ],
      [
        'Set',
        new Set(['one', 'two', 'three']),
        { __t: 'Set', __v: ['one', 'two', 'three'] },
      ],
      [
        'Buffer',
        Buffer.from([90, 115, 109, 187, 242, 216, 94, 110]),
        {
          __t: 'Buffer',
          __v: [90, 115, 109, 187, 242, 216, 94, 110],
        },
      ],
      [
        'Date',
        new Date('2022-12-25T04:27:49.988Z'),
        { __t: 'Date', __v: 1671942469988 },
      ],
    ] as [string, unknown, unknown][];

    test.each(cases)('serialize %p', (_, input, output) => {
      expect(serialize(input)).toEqual(output);
    });

    test.each(cases)('deserialize %p', (_, output, input) => {
      expect(deserialize(input)).toEqual(output);
    });
  });

  describe('nested objects', () => {
    const cases = [
      [
        {
          null: null,
          string: 'string',
          number: 1,
          boolean: true,
          Date: new Date('2022-12-25T04:27:49.988Z'),
          Buffer: Buffer.from([90, 115, 109, 187, 242, 216, 94, 110]),
          Set: new Set(['one', 'two', 'three']),
          Map: new Map([
            ['one', 1],
            ['two', 2],
          ]),
          object: {
            null: null,
            string: 'string',
            number: 1,
            boolean: true,
            Date: new Date('2022-12-25T04:27:49.988Z'),
            Buffer: Buffer.from([90, 115, 109, 187, 242, 216, 94, 110]),
            Set: new Set(['one', 'two', 'three']),
            Map: new Map([
              ['one', 1],
              ['two', 2],
            ]),
          },
          array: [
            null,
            'string',
            1,
            true,
            new Date('2022-12-25T04:27:49.988Z'),
            Buffer.from([90, 115, 109, 187, 242, 216, 94, 110]),
            new Set(['one', 'two', 'three']),
            new Map([
              ['one', 1],
              ['two', 2],
            ]),
            {
              null: null,
              string: 'string',
              number: 1,
              boolean: true,
            },
          ],
        },
        {
          null: null,
          string: 'string',
          number: 1,
          boolean: true,
          Date: { __t: 'Date', __v: 1671942469988 },
          Buffer: {
            __t: 'Buffer',
            __v: [90, 115, 109, 187, 242, 216, 94, 110],
          },
          Set: { __t: 'Set', __v: ['one', 'two', 'three'] },
          Map: {
            __t: 'Map',
            __v: [
              ['one', 1],
              ['two', 2],
            ],
          },
          object: {
            null: null,
            string: 'string',
            number: 1,
            boolean: true,
            Date: { __t: 'Date', __v: 1671942469988 },
            Buffer: {
              __t: 'Buffer',
              __v: [90, 115, 109, 187, 242, 216, 94, 110],
            },
            Set: { __t: 'Set', __v: ['one', 'two', 'three'] },
            Map: {
              __t: 'Map',
              __v: [
                ['one', 1],
                ['two', 2],
              ],
            },
          },
          array: [
            null,
            'string',
            1,
            true,
            { __t: 'Date', __v: 1671942469988 },
            {
              __t: 'Buffer',
              __v: [90, 115, 109, 187, 242, 216, 94, 110],
            },
            { __t: 'Set', __v: ['one', 'two', 'three'] },
            {
              __t: 'Map',
              __v: [
                ['one', 1],
                ['two', 2],
              ],
            },
            {
              null: null,
              string: 'string',
              number: 1,
              boolean: true,
            },
          ],
        },
      ],
    ] as [unknown, unknown][];

    test.each(cases)('serialize nested object', (input, output) => {
      expect(serialize(input)).toEqual(output);
    });

    test.each(cases)('deserialize nested object', (output, input) => {
      expect(deserialize(input)).toEqual(output);
    });
  });
});
