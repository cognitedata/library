import { describe, expect, it } from 'vitest';

import { MockRunService } from './MockRunService';

const INPUT = { extraLinks: [{ model: 'a:B:v1', links: ['V.p'] }], scope: { dimensions: 'all' as const, configured: true } };

describe(MockRunService.name, () => {
  it('starts a run that is running at first and then completes', async () => {
    const service = new MockRunService();

    const { executionId } = await service.start(INPUT);

    await expect(service.getStatus(executionId)).resolves.toBe('running');
    await expect(service.getStatus(executionId)).resolves.toBe('completed');
    await expect(service.getStatus(executionId)).resolves.toBe('completed');
  });

  it('gives each run its own id and progress', async () => {
    const service = new MockRunService();
    const first = await service.start(INPUT);
    const second = await service.start(INPUT);

    await service.getStatus(first.executionId);
    await service.getStatus(first.executionId);

    expect(second.executionId).not.toBe(first.executionId);
    await expect(service.getStatus(second.executionId)).resolves.toBe('running');
  });

  it('does not know a run it did not start', async () => {
    await expect(new MockRunService().getStatus('nope')).rejects.toMatchObject({ kind: 'request-failed' });
  });

  it('has a run going on until it has been asked how it is going', async () => {
    const service = new MockRunService();
    expect(await service.findRunning()).toBe(false);

    const { executionId } = await service.start(INPUT);
    expect(await service.findRunning()).toBe(true);

    await service.getStatus(executionId);
    expect(await service.findRunning()).toBe(false);
  });
});
