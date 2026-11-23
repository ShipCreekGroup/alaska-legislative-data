import { splitProps, type JSX } from 'solid-js';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '~/lib/utils';

export const badgeVariants = cva(
  'inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-semibold whitespace-nowrap',
  {
    variants: {
      variant: {
        default: 'border-transparent bg-primary text-primary-foreground',
        secondary: 'border-transparent bg-secondary text-secondary-foreground',
        outline: 'text-foreground',
        yea: 'border-transparent bg-yea/15 text-yea',
        nay: 'border-transparent bg-nay/15 text-nay',
        gold: 'border-transparent bg-gold/25 text-foreground',
      },
    },
    defaultVariants: { variant: 'default' },
  },
);

export function Badge(props: JSX.HTMLAttributes<HTMLSpanElement> & VariantProps<typeof badgeVariants>) {
  const [local, rest] = splitProps(props, ['class', 'variant']);
  return <span class={cn(badgeVariants({ variant: local.variant }), local.class)} {...rest} />;
}
