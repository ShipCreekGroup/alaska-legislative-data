import { splitProps, type JSX } from 'solid-js';
import { Dynamic } from 'solid-js/web';
import { cn } from '~/lib/utils';

const make = (tag: 'div' | 'h3' | 'p', base: string) => (props: JSX.HTMLAttributes<HTMLDivElement>) => {
  const [local, rest] = splitProps(props, ['class']);
  return <Dynamic component={tag as 'div'} class={cn(base, local.class)} {...rest} />;
};

export const Card = make('div', 'rounded-xl border bg-card text-card-foreground shadow-xs');
export const CardHeader = make('div', 'flex flex-col gap-1.5 p-5 pb-3');
export const CardTitle = make('h3', 'font-display text-base font-bold leading-tight');
export const CardDescription = make('p', 'text-sm text-muted-foreground');
export const CardContent = make('div', 'p-5 pt-0');
