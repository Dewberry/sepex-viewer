"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { LayoutGrid } from "lucide-react";
import { useSession } from "next-auth/react";
import { toast } from "sonner";
import ActionBar from "@/app/(dashboard)/builder/_components/ActionBar";
import ExecutionModeCard from "@/app/(dashboard)/builder/_components/ExecutionModeCard";
import InputsEditor from "@/app/(dashboard)/builder/_components/InputsEditor";
import PayloadPreview from "@/app/(dashboard)/builder/_components/PayloadPreview";
import ProcessInfoCard from "@/app/(dashboard)/builder/_components/ProcessInfoCard";
import ProcessPicker from "@/app/(dashboard)/builder/_components/ProcessPicker";
import RecentPayloadsPopover from "@/app/(dashboard)/builder/_components/RecentPayloadsPopover";
import SubmitterCard from "@/app/(dashboard)/builder/_components/SubmitterCard";
import TagsCard from "@/app/(dashboard)/builder/_components/TagsCard";
import usePayloadForm from "@/app/(dashboard)/builder/_hooks/usePayloadForm";
import useProcessDetailQuery from "@/app/(dashboard)/builder/_hooks/useProcessDetailQuery";
import useProcessesQuery from "@/app/(dashboard)/builder/_hooks/useProcessesQuery";
import useRecentJobsQuery from "@/app/(dashboard)/builder/_hooks/useRecentJobsQuery";
import useSavedTemplates from "@/app/(dashboard)/builder/_hooks/useSavedTemplates";
import useSubmitJobMutation from "@/app/(dashboard)/builder/_hooks/useSubmitJobMutation";
import { buildPayload } from "@/app/(dashboard)/builder/_utils/schema";
import { Form } from "@/components/ui/form";
import { Skeleton } from "@/components/ui/skeleton";
import { getJob } from "@/lib/sepex";

const DEFAULT_USER_EMAIL =
  process.env.NEXT_PUBLIC_SEPEX_USER_EMAIL || "dev@dewberry.local";

export default function BuilderPage() {
  const router = useRouter();
  const { data: session } = useSession();
  const sessionEmail = session?.user?.email || DEFAULT_USER_EMAIL;

  const [selectedProcessId, setSelectedProcessId] = useState(null);
  const [editorTab, setEditorTab] = useState("form");
  const [executionMode, setExecutionMode] = useState("async");
  const [submitter, setSubmitter] = useState(sessionEmail);
  const [tags, setTags] = useState([]);
  const [showRecent, setShowRecent] = useState(false);

  useEffect(() => {
    if (session?.user?.email) setSubmitter(session.user.email);
  }, [session?.user?.email]);

  const processesQuery = useProcessesQuery();
  const processList = useMemo(
    () => processesQuery.data?.processes || [],
    [processesQuery.data]
  );

  const processDetailQuery = useProcessDetailQuery(selectedProcessId);
  const processDetail = processDetailQuery.data;

  const recentJobsQuery = useRecentJobsQuery(sessionEmail);
  const recentJobs = recentJobsQuery.data?.jobs || [];

  const {
    templates,
    save: saveTemplate,
    remove: removeTemplate
  } = useSavedTemplates();

  const submitMutation = useSubmitJobMutation();

  const {
    form,
    isFormValid,
    requiredCount,
    filledRequiredCount,
    payloadJSON,
    payloadYAML,
    rawJsonText,
    setRawJsonText,
    loadInputs,
    resetAll
  } = usePayloadForm({ processDetail, tags });

  const selectedProcessInfo = processList.find(
    (p) => p.id === selectedProcessId
  );

  const handleSelectProcess = (id) => {
    setSelectedProcessId(id);
    resetAll();
  };

  const applyLoadedPayload = (inputs, nextTags, processID) => {
    setSelectedProcessId(processID || null);
    loadInputs(inputs);
    setTags(nextTags || []);
  };

  const handleUseRecent = async (job) => {
    setShowRecent(false);
    let inputs = job.inputs;
    if (!inputs) {
      try {
        const full = await getJob(job.jobID);
        inputs = full?.inputs || {};
      } catch (err) {
        toast.error(err?.message || "Could not load that payload");
        return;
      }
    }
    applyLoadedPayload(inputs, job.tags || [], job.processID);
  };

  const handleSaveTemplate = (name) => {
    saveTemplate({
      name,
      processID: selectedProcessId,
      inputs: form.getValues(),
      tags
    });
    toast.success("Template saved.");
  };

  const handleLoadTemplate = (template) => {
    applyLoadedPayload(
      template.inputs || {},
      template.tags || [],
      template.processID
    );
  };

  const triggerSubmit = form.handleSubmit((values) => {
    if (!selectedProcessId) return;
    submitMutation.mutate({
      processID: selectedProcessId,
      payload: buildPayload(processDetail, values, tags),
      async: executionMode === "async",
      userEmail: submitter
    });
  });

  return (
    <div className="mx-auto max-w-[1600px] space-y-6 px-4 py-6 lg:px-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold">Builder</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Compose, validate, and submit job payloads
          </p>
        </div>
        <RecentPayloadsPopover
          open={showRecent}
          onOpenChange={setShowRecent}
          jobs={recentJobs}
          isLoading={recentJobsQuery.isLoading}
          isError={recentJobsQuery.isError}
          onUse={handleUseRecent}
          onViewAll={() => router.push("/jobs")}
        />
      </div>

      <ProcessPicker
        processes={processList}
        selected={selectedProcessInfo}
        isLoading={processesQuery.isLoading}
        isError={processesQuery.isError}
        onSelect={handleSelectProcess}
      />

      <ProcessInfoCard process={selectedProcessInfo} />

      {selectedProcessId && processDetailQuery.isLoading && !processDetail && (
        <div className="grid gap-6 lg:grid-cols-2">
          <Skeleton className="h-[400px] w-full" />
          <Skeleton className="h-[400px] w-full" />
        </div>
      )}

      {selectedProcessId && processDetail && (
        <Form {...form}>
          <div className="grid gap-6 lg:grid-cols-2">
            <InputsEditor
              processDetail={processDetail}
              control={form.control}
              payloadJSON={payloadJSON}
              payloadYAML={payloadYAML}
              editorTab={editorTab}
              setEditorTab={setEditorTab}
              rawJsonText={rawJsonText}
              setRawJsonText={setRawJsonText}
              isFormValid={isFormValid}
              filledRequiredCount={filledRequiredCount}
              requiredCount={requiredCount}
              onSubmit={triggerSubmit}
            />

            <div className="space-y-4">
              <PayloadPreview payloadJSON={payloadJSON} />
              <SubmitterCard value={submitter} onChange={setSubmitter} />
              <TagsCard
                tags={tags}
                onAdd={(t) =>
                  setTags((prev) => (prev.includes(t) ? prev : [...prev, t]))
                }
                onRemove={(t) => setTags((prev) => prev.filter((x) => x !== t))}
              />
              <ExecutionModeCard
                mode={executionMode}
                onChange={setExecutionMode}
              />
              <ActionBar
                canSave={Boolean(selectedProcessId)}
                canSubmit={isFormValid}
                isSubmitting={submitMutation.isPending}
                templates={templates}
                onSaveTemplate={handleSaveTemplate}
                onLoadTemplate={handleLoadTemplate}
                onDeleteTemplate={removeTemplate}
                onSubmit={triggerSubmit}
              />
            </div>
          </div>
        </Form>
      )}

      {!selectedProcessId && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-muted">
            <LayoutGrid className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-2 text-lg font-semibold">
            Select a process to begin
          </h3>
          <p className="max-w-md text-sm text-muted-foreground">
            Choose a registered OGC Process from the dropdown above to build and
            submit an execution payload.
          </p>
          {!processesQuery.isLoading && (
            <p className="mt-3 text-xs text-muted-foreground">
              {processList.length}{" "}
              {processList.length === 1 ? "process" : "processes"} available
            </p>
          )}
        </div>
      )}
    </div>
  );
}
