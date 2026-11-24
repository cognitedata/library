import { BreadcrumbItem, BreadcrumbList } from '@cognite/aura/components/breadcrumb';
import { Button } from '@cognite/aura/components/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from '@cognite/aura/components/dropdown-menu';
import { Tabs, TabsList, TabsTrigger } from '@cognite/aura/components/tabs';
import {
  Topbar,
  TopbarBreadcrumbs,
  TopbarCenter,
  TopbarLeft,
  TopbarRight,
} from '@cognite/aura/components/topbar';
import { IconSettings } from '@cognite/aura/icons';


import type { PageId } from '../../app/state/types';
import type { LanguageCode } from '../../i18n/messages';

import { appContentHorizontalInsetClassName } from './appContentHorizontalInset';
import { isPageIdValue, TAB_PAGE_IDS } from './pageIds';

type ThemeMode = 'light' | 'dark';

type AppTopbarProps = {
  page: PageId;
  language: LanguageCode;
  themeMode: ThemeMode;
  showPageTabs?: boolean;
  onPageChange: (page: PageId) => void;
  onLanguageChange: (language: LanguageCode) => void;
  onThemeChange: (mode: ThemeMode) => void;
  t: (key: string) => string;
};

export function AppTopbar({
  page,
  language,
  themeMode,
  showPageTabs = true,
  onPageChange,
  onLanguageChange,
  onThemeChange,
  t,
}: AppTopbarProps) {
  return (
    <Topbar
      className={`app-topbar h-auto min-h-14 border-b py-4 ${appContentHorizontalInsetClassName}`}
    >
      <TopbarLeft>
        <TopbarBreadcrumbs>
          <BreadcrumbList>
            <BreadcrumbItem>
              <h1
                data-slot="app-title"
                className="text-sm font-medium leading-[18px] tracking-tight text-foreground"
              >
                {t('app.shortTitle')}
              </h1>
            </BreadcrumbItem>
          </BreadcrumbList>
        </TopbarBreadcrumbs>
      </TopbarLeft>

      <TopbarCenter className="justify-start pl-4">
        {showPageTabs ? (
          <Tabs
            value={page}
            onValueChange={(value) => {
              if (typeof value === 'string' && isPageIdValue(value)) {
                onPageChange(value);
              }
            }}
          >
            <TabsList size="small">
              {TAB_PAGE_IDS.map((pageId) => (
                <TabsTrigger key={pageId} value={pageId}>
                  {t(`nav.${pageId}`)}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
        ) : null}
      </TopbarCenter>

      <TopbarRight>
        <DropdownMenu>
          <DropdownMenuTrigger
            render={
              <Button
                variant="ghost"
                size="icon-sm"
                className="text-inherit hover:bg-accent-background"
                aria-label="Settings"
              >
                <IconSettings />
              </Button>
            }
          />
          <DropdownMenuContent align="end" className="w-48">
            <DropdownMenuGroup>
              <DropdownMenuLabel>Language</DropdownMenuLabel>
              <DropdownMenuRadioGroup
                value={language}
                onValueChange={(value) => {
                  if (isLanguageCode(value)) {
                    onLanguageChange(value);
                  }
                }}
              >
                <DropdownMenuRadioItem value="en">English</DropdownMenuRadioItem>
                <DropdownMenuRadioItem value="ja">Japanese</DropdownMenuRadioItem>
              </DropdownMenuRadioGroup>
            </DropdownMenuGroup>
            <DropdownMenuGroup>
              <DropdownMenuLabel>Theme</DropdownMenuLabel>
              <DropdownMenuRadioGroup
                value={themeMode}
                onValueChange={(value) => {
                  if (isThemeMode(value)) {
                    onThemeChange(value);
                  }
                }}
              >
                <DropdownMenuRadioItem value="light">Light mode</DropdownMenuRadioItem>
                <DropdownMenuRadioItem value="dark">Dark mode</DropdownMenuRadioItem>
              </DropdownMenuRadioGroup>
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>
      </TopbarRight>
    </Topbar>
  );
}

function isLanguageCode(value: unknown): value is LanguageCode {
  return value === 'en' || value === 'ja';
}

function isThemeMode(value: unknown): value is ThemeMode {
  return value === 'light' || value === 'dark';
}
