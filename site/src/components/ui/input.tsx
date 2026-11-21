import { splitProps, type JSX } from 'solid-js';
import { cn } from '~/lib/utils';

export function Input(props: JSX.InputHTMLAttributes<HTMLInputElement>) {
  const [local, rest] = splitProps(props, ['class']);
  return (
    <input
      class={cn(
        'flex h-9 w-full min-w-0 rounded-md border border-input bg-card px-3 py-1 text-sm shadow-xs placeholder:text-muted-foreground',
        local.class,
      )}
      {...rest}
    />
  );
}
