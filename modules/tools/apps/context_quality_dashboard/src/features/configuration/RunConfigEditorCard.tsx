import { Button } from '@cognite/aura/components/button';
import { Card, CardContent } from '@cognite/aura/components/card';
import { Progress } from '@cognite/aura/components/progress';
import { Tabs, TabsList, TabsPanel, TabsTrigger } from '@cognite/aura/components/tabs';
import { useRef, useState, type ChangeEvent } from 'react';

import { RunRequestButton } from '../runs/RunRequestButton';

import { RunConfigFileReviewDialog } from './RunConfigFileReviewDialog';
import { RunConfigFileUploadPanel } from './RunConfigFileUploadPanel';
import { runConfigFileByteLength } from './runConfigFileUploadPresentation';
import type { RunConfigFileViewModel, RunPhase } from './useRunConfigFileViewModel';

type EditorTab = 'upload' | 'paste';

function fill(template: string, values: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => values[name] ?? '');
}

type RunConfigEditorCardProps = {
  viewModel: RunConfigFileViewModel;
  /** Translation key for the primary run button. */
  runButtonLabelKey?: string;
  hidePrimaryRunButton?: boolean;
  hideInlineRunStatus?: boolean;
  /** Renders without an outer card (e.g. nested inside onboarding). */
  embedded?: boolean;
  /** Full-width compact upload dropzone. */
  compactUpload?: boolean;
};

export function RunConfigEditorCard({
  viewModel,
  runButtonLabelKey = 'configuration.run.button',
  hidePrimaryRunButton = false,
  hideInlineRunStatus = false,
  embedded = false,
  compactUpload = false,
}: RunConfigEditorCardProps) {
  const input = useRef<HTMLInputElement>(null);
  const [tab, setTab] = useState<EditorTab>('upload');
  const [isUploading, setIsUploading] = useState(false);
  const [reviewOpen, setReviewOpen] = useState(false);
  const { t, fileName, text, result, showViewConfiguration } = viewModel;

  const ingestFile = (selected: File): void => {
    setIsUploading(true);
    void viewModel.upload(selected).finally(() => {
      setIsUploading(false);
      setReviewOpen(true);
    });
  };

  const hasDraft = fileName !== null || text.trim() !== '';

  const onChange = (event: ChangeEvent<HTMLInputElement>): void => {
    const selected = event.target.files?.[0];
    if (selected !== undefined) {
      ingestFile(selected);
    }
    event.target.value = '';
  };

  const onTabChange = (value: string | number | null): void => {
    if (value === 'upload' || value === 'paste') {
      setTab(value);
    }
  };

  const pasteMinHeightClass = compactUpload ? 'min-h-[200px] max-h-[220px]' : 'min-h-64';

  const editorBody = (
    <>
      <input
        ref={input}
        type="file"
        accept=".yaml,.yml,.json,application/json,text/yaml"
        className="hidden"
        data-testid="run-config-file-input"
        onChange={onChange}
      />

      <Tabs value={tab} onValueChange={onTabChange} className={`w-full ${embedded ? 'pt-0' : 'pt-5'}`}>
        <div className={embedded ? 'w-full' : 'px-5'}>
          <TabsList size="small" className={embedded ? 'w-full max-w-full' : undefined}>
            <TabsTrigger value="upload">{t('configuration.file.tab.upload')}</TabsTrigger>
            <TabsTrigger value="paste">{t('configuration.file.tab.paste')}</TabsTrigger>
          </TabsList>
        </div>

        <TabsPanel value="upload" className={`mt-3 w-full ${embedded ? 'px-0' : 'px-5'}`}>
          <RunConfigFileUploadPanel
            fileName={fileName}
            fileByteLength={runConfigFileByteLength(text)}
            isUploading={isUploading}
            onBrowse={() => input.current?.click()}
            onRemove={viewModel.remove}
            onFile={ingestFile}
            t={t}
            layout={compactUpload ? 'compact' : 'default'}
          />
        </TabsPanel>

        <TabsPanel value="paste" className="mt-3 w-full self-stretch">
          <div
            className={`w-full rounded-lg border border-dashed border-border bg-muted/20 ${embedded ? '' : 'border-y border-x-0 border-solid bg-muted/40'}`}
          >
            <textarea
              data-testid="run-config-editor"
              aria-label={t('configuration.file.tab.paste')}
              className={`box-border block w-full min-w-0 resize-y border-0 bg-transparent px-4 py-3 font-mono text-sm text-foreground outline-none focus-visible:ring-0 ${pasteMinHeightClass}`}
              placeholder={t('configuration.file.editorPlaceholder')}
              value={viewModel.text}
              onChange={(event) => viewModel.setText(event.target.value)}
            />
          </div>
        </TabsPanel>
      </Tabs>

      {result !== null ? (
        <div className={`mt-3 flex flex-wrap items-center gap-2 ${embedded ? '' : 'px-5'}`}>
          {viewModel.canRun && !hidePrimaryRunButton && viewModel.models !== null ? (
            <RunRequestButton
              request={{ kind: 'everything' }}
              models={viewModel.models}
              label={t(runButtonLabelKey)}
              runningLabel={t('configuration.run.runningButton')}
              variant="default"
              runEnabled={viewModel.runEnabled}
              disabledTooltip={viewModel.runDisabledTooltip}
            />
          ) : null}
          {showViewConfiguration ? (
            <Button variant="outline" size="sm" onClick={() => setReviewOpen(true)}>
              {t('configuration.file.viewReview')}
            </Button>
          ) : null}
        </div>
      ) : !embedded && viewModel.canRun && !hidePrimaryRunButton && viewModel.models !== null ? (
        <div className="mt-4 flex flex-wrap items-center gap-2 px-5">
          <RunRequestButton
            request={{ kind: 'everything' }}
            models={viewModel.models}
            label={t(runButtonLabelKey)}
            runningLabel={t('configuration.run.runningButton')}
            variant="default"
            runEnabled={viewModel.runEnabled}
            disabledTooltip={viewModel.runDisabledTooltip}
          />
        </div>
      ) : null}

      <div className={embedded ? 'mt-2' : 'px-5 pb-5'}>
        {viewModel.canRun && !hideInlineRunStatus ? <RunStatus viewModel={viewModel} /> : null}
      </div>
    </>
  );

  return (
    <>
      {embedded ? (
        <div className="flex w-full flex-col items-stretch" data-testid="run-config-editor-card">
          {editorBody}
        </div>
      ) : (
        <Card
          className="gap-0 border border-border bg-card-background p-0 shadow-none"
          data-testid="run-config-editor-card"
        >
          <CardContent className="flex w-full flex-col items-stretch gap-0 p-0">{editorBody}</CardContent>
        </Card>
      )}

      <RunConfigFileReviewDialog
        open={reviewOpen && hasDraft}
        onOpenChange={setReviewOpen}
        result={result}
        t={t}
      />
    </>
  );
}

function RunStatus({ viewModel }: { viewModel: RunConfigFileViewModel }) {
  const { t, phase, startError, result } = viewModel;
  const inProgress = phase === 'starting' || phase === 'running';
  const idleRunHint = result?.ok !== true ? t('configuration.run.needsFile') : null;
  const text: Record<RunPhase, string | null> = {
    idle: idleRunHint,
    starting: t('configuration.run.starting'),
    running: t('configuration.run.running'),
    completed: null,
    failed: t('configuration.run.failed'),
    'not-started': startError === null ? null : fill(t(startError.key), { message: startError.message }),
  };
  const message = text[phase];
  if (message === null && !inProgress) {
    return null;
  }
  if (inProgress) {
    return (
      <div className="space-y-2" data-testid="run-status-block">
        <Progress
          value={null}
          size="sm"
          aria-label={t('configuration.run.progressLabel')}
          data-testid="run-config-progress"
        />
      </div>
    );
  }
  const bad = phase === 'failed' || phase === 'not-started';
  return (
    <div className="space-y-2" data-testid="run-status-block">
      {message === null ? null : (
        <p
          role="status"
          data-testid="run-status"
          className={`text-sm ${bad ? 'text-destructive-foreground' : 'text-muted-foreground'}`}
        >
          {message}
        </p>
      )}
    </div>
  );
}
