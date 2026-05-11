"use client";

import { AlertCircle, Check } from "lucide-react";
import DynamicInputField from "@/app/(dashboard)/builder/_components/DynamicInputField";

export default function InputsEditor({
  processDetail,
  control,
  payloadJSON,
  payloadYAML,
  editorTab,
  setEditorTab,
  rawJsonText,
  setRawJsonText,
  isFormValid,
  filledRequiredCount,
  requiredCount,
  onSubmit
}) {
  return (
    <div className="overflow-hidden rounded-lg border bg-card">
      <div className="flex border-b">
        {["form", "json", "yaml"].map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setEditorTab(t)}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              editorTab === t
                ? "border-b-2 border-dewberry-teal bg-accent text-accent-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            {t.toUpperCase()}
          </button>
        ))}
      </div>
      <div className="max-h-[600px] overflow-y-auto p-4">
        {editorTab === "form" && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              onSubmit();
            }}
            className="space-y-6"
          >
            {(processDetail.inputs || []).map((input) => (
              <DynamicInputField
                key={input.id}
                input={input}
                control={control}
                rawJsonText={rawJsonText}
                setRawJsonText={setRawJsonText}
              />
            ))}
          </form>
        )}

        {editorTab === "json" && (
          <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs">
            {payloadJSON}
          </pre>
        )}
        {editorTab === "yaml" && (
          <pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs">
            {payloadYAML}
          </pre>
        )}
      </div>

      <div
        className={`flex items-center gap-2 border-t px-4 py-2.5 text-sm ${
          isFormValid
            ? "border-status-successful/20 bg-status-successful/10 text-status-successful-fg"
            : "border-status-running/20 bg-status-running/10 text-status-running-fg"
        }`}
      >
        {isFormValid ? (
          <Check className="h-4 w-4" />
        ) : (
          <AlertCircle className="h-4 w-4" />
        )}
        <span className="font-medium">
          {isFormValid
            ? "Valid"
            : `${filledRequiredCount} of ${requiredCount} required field${
                requiredCount === 1 ? "" : "s"
              } filled`}
        </span>
      </div>
    </div>
  );
}
