import { describe, expect, it, vi } from 'vitest';

import { createRunConfigFileStore } from './runConfigFileStore';

describe(createRunConfigFileStore.name, () => {
  it('starts with no file', () => {
    expect(createRunConfigFileStore(fakeStorage()).getSnapshot()).toBeNull();
  });

  it('keeps the file and tells subscribers', () => {
    const store = createRunConfigFileStore(fakeStorage());
    const listener = vi.fn();
    store.subscribe(listener);

    store.save({ fileName: 'a.yaml', text: '- model: x' });

    expect(store.getSnapshot()).toEqual({ fileName: 'a.yaml', text: '- model: x' });
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('gives the same snapshot object until the file changes, as a React external store needs', () => {
    const store = createRunConfigFileStore(fakeStorage());
    store.save({ fileName: 'a.yaml', text: 't' });

    expect(store.getSnapshot()).toBe(store.getSnapshot());
  });

  it('is still there after a reload', () => {
    const storage = fakeStorage();
    createRunConfigFileStore(storage).save({ fileName: 'a.yaml', text: 't' });

    expect(createRunConfigFileStore(storage).getSnapshot()).toEqual({ fileName: 'a.yaml', text: 't' });
  });

  it('removes the file', () => {
    const storage = fakeStorage();
    const store = createRunConfigFileStore(storage);
    store.save({ fileName: 'a.yaml', text: 't' });

    store.clear();

    expect(store.getSnapshot()).toBeNull();
    expect(createRunConfigFileStore(storage).getSnapshot()).toBeNull();
  });

  it('stops notifying a subscriber that unsubscribed', () => {
    const store = createRunConfigFileStore(fakeStorage());
    const listener = vi.fn();
    store.subscribe(listener)();

    store.save({ fileName: 'a.yaml', text: 't' });

    expect(listener).not.toHaveBeenCalled();
  });

  it('ignores stored text that is not a saved file', () => {
    expect(createRunConfigFileStore(fakeStorage('not json')).getSnapshot()).toBeNull();
    expect(createRunConfigFileStore(fakeStorage('{"fileName":1}')).getSnapshot()).toBeNull();
  });

  it('keeps working for the session when the browser will not store it', () => {
    const storage = {
      getItem: () => null,
      setItem: () => {
        throw new Error('quota');
      },
      removeItem: () => undefined,
    };
    const store = createRunConfigFileStore(storage);

    store.save({ fileName: 'a.yaml', text: 't' });

    expect(store.getSnapshot()).toEqual({ fileName: 'a.yaml', text: 't' });
  });

  it('works without any storage', () => {
    const store = createRunConfigFileStore(null);
    store.save({ fileName: 'a.yaml', text: 't' });

    expect(store.getSnapshot()?.fileName).toBe('a.yaml');
  });
});

function fakeStorage(initial?: string) {
  const items = new Map<string, string>();
  const key = 'context-quality.run-config-file';
  if (initial !== undefined) {
    items.set(key, initial);
  }
  return {
    getItem: (name: string) => items.get(name) ?? null,
    setItem: (name: string, value: string) => void items.set(name, value),
    removeItem: (name: string) => void items.delete(name),
  };
}
