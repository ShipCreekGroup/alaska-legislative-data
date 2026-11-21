import { Tabs as K } from '@kobalte/core/tabs';
import { splitProps, type ComponentProps } from 'solid-js';
import { cn } from '~/lib/utils';

export const Tabs = K;
export function TabsList(props: ComponentProps<typeof K.List>) {
  const [l, r] = splitProps(props, ['class']);
  return (
    <K.List class={cn('inline-flex h-9 items-center rounded-lg bg-muted p-1 text-muted-foreground', l.class)} {...r} />
  );
}
export function TabsTrigger(props: ComponentProps<typeof K.Trigger>) {
  const [l, r] = splitProps(props, ['class']);
  return (
    <K.Trigger
      class={cn(
        'inline-flex h-7 items-center rounded-md px-3 text-sm font-medium whitespace-nowrap data-[selected]:bg-card data-[selected]:text-foreground data-[selected]:shadow-xs',
        l.class,
      )}
      {...r}
    />
  );
}
export function TabsContent(props: ComponentProps<typeof K.Content>) {
  const [l, r] = splitProps(props, ['class']);
  return <K.Content class={cn('mt-4', l.class)} {...r} />;
}
