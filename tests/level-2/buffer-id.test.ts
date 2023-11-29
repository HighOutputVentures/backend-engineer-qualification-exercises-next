import * as R from 'ramda';
import delay from '@highoutput/delay';
import generate from '../../src/level-2/buffer-id';

describe('ObjectId', () => {
  test('generate unique ids', () => {
    const COUNT = 10;
    const ids = new Set(
      R.times(() => generate(0).toString('hex'), COUNT)
    );

    expect(ids.size).toBe(COUNT);
  });

  test('generate ids with different timestamps', async () => {
    const one = generate(0).toString('hex');
    await delay(1500);
    const two = generate(0).toString('hex');
    expect(one.substring(2, 10)).not.toEqual(two.substring(2, 10));
  });

  test('generate ids with the same machine ids', () => {
    const one = generate(0).toString('hex');
    const two = generate(0).toString('hex');
    expect(one.substring(11, 20)).toEqual(two.substring(11, 20));
  });

  test('generate ids with the different counter', () => {
    const one = generate(0).toString('hex');
    const two = generate(0).toString('hex');
    expect(one.substring(21, 26)).not.toEqual(two.substring(21, 26));
  });

  test('clone', async () => {
    const one = generate(1);
    const two = R.clone(one);
    expect(one.equals(two)).toBe(true);
  });
});
