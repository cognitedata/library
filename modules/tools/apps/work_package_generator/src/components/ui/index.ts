/**
 * Shared UI Components
 *
 * Stateless, reusable UI primitives that can be used across all features.
 * Components are sourced from Aura design system where available,
 * with local fallbacks for components not yet available in Aura.
 *
 * @example
 * ```tsx
 * import { Button, Badge, Input, Table } from '@/components/ui';
 * ```
 */

// Aura components (preferred - consistent with Cognite Design System).
// Accordion.Trigger is wrapped in Accordion.Header (an h3).
// ResourceTypeAccordionItem uses `[&>h3]` so the Add button sits beside it.
export {
	Accordion,
	AccordionContent,
	AccordionItem,
	AccordionTrigger,
	Badge,
	Button,
	badgeVariants,
	buttonVariants,
	CheckboxItemControl,
	Dialog,
	DialogClose,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
	DialogTrigger,
	Input,
	Loader,
	Select,
	SelectContent,
	SelectGroup,
	SelectItem,
	SelectLabel,
	SelectScrollDownButton,
	SelectScrollUpButton,
	SelectSeparator,
	SelectTrigger,
	SelectValue,
	Textarea,
} from "@cognite/aura/components";
// Native table primitives. @cognite/aura 1.47 does not export a Table.
export {
	Table,
	TableBody,
	TableCaption,
	TableCell,
	TableFooter,
	TableHead,
	TableHeader,
	TableRow,
} from "./table";
