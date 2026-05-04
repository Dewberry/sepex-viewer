"use client";

import { Controller } from "react-hook-form";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";

export default function DynamicInputField({
  input,
  control,
  rawJsonText,
  setRawJsonText
}) {
  const dataType = input.input?.literalDataDomain?.dataType;
  const valueDef = input.input?.literalDataDomain?.valueDefinition || {};
  const required = (input.minOccurs || 0) > 0;

  return (
    <div>
      <label className="mb-1.5 block text-sm font-medium">
        {input.title || input.id}
        {required && <span className="ml-1 text-status-failed">*</span>}
      </label>
      {input.description && (
        <p className="mb-2 text-xs text-muted-foreground">
          {input.description}
        </p>
      )}

      <Controller
        name={input.id}
        control={control}
        render={({ field }) => {
          if (dataType === "string") {
            return (
              <Input
                {...field}
                value={field.value ?? ""}
                placeholder={`Enter ${(input.title || input.id).toLowerCase()}…`}
                className="font-mono"
              />
            );
          }
          if (dataType === "integer") {
            if (Array.isArray(valueDef.possibleValues)) {
              return (
                <select
                  value={field.value ?? ""}
                  onChange={(e) =>
                    field.onChange(
                      e.target.value === "" ? undefined : Number(e.target.value)
                    )
                  }
                  onBlur={field.onBlur}
                  className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                >
                  <option value="">Select…</option>
                  {valueDef.possibleValues.map((v) => (
                    <option key={v} value={v}>
                      {v}
                    </option>
                  ))}
                </select>
              );
            }
            return (
              <Input
                type="number"
                value={field.value ?? ""}
                onChange={(e) =>
                  field.onChange(
                    e.target.value === "" ? undefined : Number(e.target.value)
                  )
                }
                onBlur={field.onBlur}
                placeholder="Enter integer…"
              />
            );
          }
          // object / list — JSON textarea preserving raw text while typing
          const raw =
            rawJsonText[input.id] ??
            (field.value ? JSON.stringify(field.value, null, 2) : "");
          return (
            <Textarea
              rows={4}
              value={raw}
              onChange={(e) => {
                const text = e.target.value;
                setRawJsonText((p) => ({ ...p, [input.id]: text }));
                if (text === "") {
                  field.onChange(undefined);
                  return;
                }
                try {
                  field.onChange(JSON.parse(text));
                } catch {
                  // keep last good parsed value while user is mid-typing
                }
              }}
              onBlur={field.onBlur}
              placeholder={
                dataType === "object"
                  ? '{ "key": "value" }'
                  : '[ "item1", "item2" ]'
              }
              className="font-mono"
            />
          );
        }}
      />
    </div>
  );
}
