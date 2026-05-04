"use client";

import { useState } from "react";
import { Loader2, Save, Send } from "lucide-react";
import { Button } from "@/components/ui/button";
import LoadTemplatesPopover from "@/app/(dashboard)/builder/_components/LoadTemplatesPopover";
import SaveTemplateDialog from "@/app/(dashboard)/builder/_components/SaveTemplateDialog";

export default function ActionBar({
  canSave,
  canSubmit,
  isSubmitting,
  templates,
  onSaveTemplate,
  onLoadTemplate,
  onDeleteTemplate,
  onSubmit
}) {
  const [showSave, setShowSave] = useState(false);
  const [showLoad, setShowLoad] = useState(false);

  return (
    <div className="sticky bottom-4 rounded-lg border bg-card p-4 shadow-lg">
      <div className="flex flex-wrap items-center gap-2">
        <Button
          variant="outline"
          onClick={() => setShowSave(true)}
          className="gap-2"
          disabled={!canSave}
        >
          <Save className="h-4 w-4" />
          Save
        </Button>

        <LoadTemplatesPopover
          open={showLoad}
          onOpenChange={setShowLoad}
          templates={templates}
          onUse={onLoadTemplate}
          onDelete={onDeleteTemplate}
        />

        <div className="ml-auto">
          <Button
            onClick={onSubmit}
            disabled={!canSubmit || isSubmitting}
            className="gap-2 bg-dewberry-magenta text-white hover:bg-dewberry-magenta/90"
          >
            {isSubmitting ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
            Submit
          </Button>
        </div>
      </div>

      <SaveTemplateDialog
        open={showSave}
        onOpenChange={setShowSave}
        onSave={onSaveTemplate}
      />
    </div>
  );
}
