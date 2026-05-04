"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Search } from "lucide-react";
import { useSession } from "next-auth/react";
import { useForm } from "react-hook-form";
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
import useSavedTemplates from "@/app/(dashboard)/builder/_hooks/useSavedTemplates";
import {
  buildPayload,
  buildYaml,
  buildZodSchema
} from "@/app/(dashboard)/builder/_utils/schema";
import { Form } from "@/components/ui/form";
import { Skeleton } from "@/components/ui/skeleton";
import {
  executeProcess,
  getJob,
  getProcess,
  listJobs,
  listProcesses
} from "@/lib/sepex";

const DEFAULT_USER_EMAIL =
  process.env.NEXT_PUBLIC_SEPEX_USER_EMAIL || "dev@dewberry.local";

export default function BuilderPage() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { data: session } = useSession();
  const sessionEmail = session?.user?.email || DEFAULT_USER_EMAIL;

  const [selectedProcessId, setSelectedProcessId] = useState(null);
  const [editorTab, setEditorTab] = useState("form");
  const [executionMode, setExecutionMode] = useState("async");
  const [submitter, setSubmitter] = useState(sessionEmail);
  const [tags, setTags] = useState([]);
  const [showRecent, setShowRecent] = useState(false);
  const [rawJsonText, setRawJsonText] = useState({});

  const {
    templates,
    save: saveTemplate,
    remove: removeTemplate
  } = useSavedTemplates();

  useEffect(() => {
    if (session?.user?.email) setSubmitter(session.user.email);
  }, [session?.user?.email]);

  // ── Queries ──────────────────────────────────────────────
  const processesQuery = useQuery({
    queryKey: ["processes"],
    queryFn: () => listProcesses()
  });
  const processList = useMemo(
    () => processesQuery.data?.processes || [],
    [processesQuery.data]
  );

  const processDetailQuery = useQuery({
    queryKey: ["process", selectedProcessId],
    queryFn: () => getProcess(selectedProcessId),
    enabled: Boolean(selectedProcessId)
  });
  const processDetail = processDetailQuery.data;

  const recentJobsQuery = useQuery({
    queryKey: ["jobs", "recent", sessionEmail],
    queryFn: () => listJobs({ limit: 10, submitter: sessionEmail })
  });
  const recentJobs = recentJobsQuery.data?.jobs || [];

  // ── Form ─────────────────────────────────────────────────
  const schema = useMemo(
    () => buildZodSchema(processDetail?.inputs),
    [processDetail]
  );
  const form = useForm({
    resolver: zodResolver(schema),
    mode: "onChange",
    defaultValues: {}
  });
  const watchedValues = form.watch();

  useEffect(() => {
    if (processDetail) form.trigger();
  }, [processDetail, form]);

  // ── Submit mutation ──────────────────────────────────────
  const submitMutation = useMutation({
    mutationFn: ({ processID, payload, async: isAsync, userEmail }) =>
      executeProcess(processID, {
        inputs: payload.inputs,
        tags: payload.tags,
        async: isAsync,
        userEmail
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
      const jobID = data?.jobID;
      if (jobID) {
        toast.success(
          variables.async
            ? `Submitted — job ${jobID.slice(0, 8)}`
            : `Completed — job ${jobID.slice(0, 8)}`
        );
        router.push(`/jobs/${jobID}`);
      } else {
        toast.success("Submitted");
      }
    },
    onError: (err) => toast.error(err?.message || "Submission failed")
  });

  // ── Derived ──────────────────────────────────────────────
  const selectedProcessInfo = processList.find(
    (p) => p.id === selectedProcessId
  );

  const requiredInputs = (processDetail?.inputs || []).filter(
    (i) => (i.minOccurs || 0) > 0
  );
  const filledRequiredCount = requiredInputs.filter((i) => {
    const v = watchedValues[i.id];
    return v !== undefined && v !== null && v !== "";
  }).length;
  const requiredCount = requiredInputs.length;
  const isFormValid =
    Boolean(processDetail) &&
    requiredCount === filledRequiredCount &&
    form.formState.isValid;

  const livePayload = buildPayload(processDetail, watchedValues, tags);
  const payloadJSON = JSON.stringify(livePayload, null, 2);
  const payloadYAML = buildYaml(livePayload);

  // ── Handlers ─────────────────────────────────────────────
  const handleSelectProcess = (id) => {
    setSelectedProcessId(id);
    form.reset({});
    setRawJsonText({});
  };

  const applyLoadedInputs = (inputs, nextTags, processID) => {
    setSelectedProcessId(processID || null);
    form.reset(inputs || {});
    setTags(nextTags || []);
    const seed = {};
    if (inputs) {
      for (const [k, v] of Object.entries(inputs)) {
        if (v && typeof v === "object") seed[k] = JSON.stringify(v, null, 2);
      }
    }
    setRawJsonText(seed);
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
    applyLoadedInputs(inputs, job.tags || [], job.processID);
  };

  const handleSaveTemplate = (name) => {
    saveTemplate({
      name,
      processID: selectedProcessId,
      inputs: watchedValues,
      tags
    });
    toast.success("Template saved.");
  };

  const handleLoadTemplate = (template) => {
    applyLoadedInputs(
      template.inputs || {},
      template.tags || [],
      template.processID
    );
  };

  const onValidSubmit = (values) => {
    if (!selectedProcessId) return;
    submitMutation.mutate({
      processID: selectedProcessId,
      payload: buildPayload(processDetail, values, tags),
      async: executionMode === "async",
      userEmail: submitter
    });
  };
  const triggerSubmit = form.handleSubmit(onValidSubmit);

  // ── Render ───────────────────────────────────────────────
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
            <Search className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="mb-2 text-lg font-semibold">
            Select a process to get started
          </h3>
          <p className="max-w-md text-sm text-muted-foreground">
            Choose a registered process from the picker above to compose and
            validate a job payload.
          </p>
        </div>
      )}
    </div>
  );
}
