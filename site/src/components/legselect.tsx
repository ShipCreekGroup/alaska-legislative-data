import { For } from 'solid-js';
import { legName, legYears } from '~/lib/format';

/** A plain <select> inside the page's GET search form; changing it submits the form. */
export function LegislatureSelect(props: { value: number; options: number[] }) {
  return (
    <select
      name="legislature"
      aria-label="Legislature"
      class="h-9 rounded-md border border-input bg-card px-2 text-sm"
      onChange={(e) => e.currentTarget.form?.requestSubmit()}
    >
      <For each={props.options}>
        {(n) => (
          <option value={n} selected={n === props.value}>
            {legName(n)} ({legYears(n)})
          </option>
        )}
      </For>
    </select>
  );
}

export function ChamberSelect(props: { value?: string }) {
  return (
    <select
      name="chamber"
      aria-label="Chamber"
      class="h-9 rounded-md border border-input bg-card px-2 text-sm"
      onChange={(e) => e.currentTarget.form?.requestSubmit()}
    >
      <option value="" selected={!props.value}>
        Both chambers
      </option>
      <option value="H" selected={props.value === 'H'}>
        House
      </option>
      <option value="S" selected={props.value === 'S'}>
        Senate
      </option>
    </select>
  );
}
