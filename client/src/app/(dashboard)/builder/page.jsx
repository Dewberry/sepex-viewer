"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { useSession } from "next-auth/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useForm, Controller } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { toast } from "sonner";
import {
  AlertCircle,
  ArrowRight,
  Check,
  CheckCircle2,
  ChevronDown,
  Clock,
  FolderOpen,
  Info,
  Loader2,
  Plus,
  Save,
  Search,
  Send,
  Trash2,
  X,
  XCircle
} from "lucide-react";

import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Skeleton } from "@/components/ui/skeleton";
import { Form } from "@/components/ui/form";

import {
  executeProcess,
  getJob,
  getProcess,
  listJobs,
  listProcesses
} from "@/lib/sepex";

const STORAGE_KEY = "sepex-viewer:builder-templates:v1";
const DEFAULT_USER_EMAIL =
  process.env.NEXT_PUBLIC_SEPEX_USER_EMAIL || "dev@dewberry.local";
const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:5050";

function loadSavedTemplates() {
  if (typeof window === "undefined") return [];
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

function persistSavedTemplates(templates) {
  if (typeof window === "undefined") return;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(templates));
}

function getRelativeTime(dateString) {
  if (!dateString) return "";
  const ms = Date.now() - new Date(dateString).getTime();
  const sec = Math.max(1, Math.floor(ms / 1000));
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  return `${Math.floor(hr / 24)}d ago`;
}

function StatusIcon({ status, className = "h-4 w-4" }) {
  if (status === "successful")
    return <CheckCircle2 className={`${className} text-status-successful`} />;
  if (status === "running")
    return <Loader2 className={`${className} animate-spin text-status-running`} />;
  if (status === "failed")
    return <XCircle className={`${className} text-status-failed`} />;
  return <Clock className={`${className} text-status-accepted`} />;
}

// TODO: deeper schema derivation. For v1 we coerce strings/integers and treat
// object/list as freeform — JSON parsing in the textarea is the only validation.
function buildZodSchema(processInputs) {
  if (!processInputs?.length) return z.object({});
  const shape = {};
  for (const input of processInputs) {
    const dataType = input.input?.literalDataDomain?.dataType;
    const required = (input.minOccurs || 0) > 0;
    let field;
    if (dataType === "string") {
      field = z.string();
      field = required ? field.min(1) : field.optional();
    } else if (dataType === "integer") {
      const numeric = z.coerce.number();
      field = required ? numeric : numeric.optional();
    } else {
      field = required
        ? z.any().refine((v) => v != null && v !== "")
        : z.any().optional();
    }
    shape[input.id] = field;
  }
  return z.object(shape);
}

function buildPayload(processDetail, values, tags) {
  const payload = { inputs: {} };
  const sourceInputs = processDetail?.inputs?.length
    ? processDetail.inputs.map((i) => i.id)
    : Object.keys(values || {});
  for (const id of sourceInputs) {
    const v = values?.[id];
    if (v === undefined || v === null || v === "") continue;
    payload.inputs[id] = v;
  }
  if (tags?.length) payload.tags = tags;
  return payload;
}

function buildYaml(payload) {
  const lines = ["inputs:"];
  for (const [k, v] of Object.entries(payload.inputs || {})) {
    lines.push(`  ${k}: ${JSON.stringify(v)}`);
  }
  if (payload.tags?.length) {
    lines.push("tags:");
    for (const t of payload.tags) lines.push(`  - ${t}`);
  }
  return lines.join("\n");
}

export default function BuilderPage() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const { data: session } = useSession();
  const sessionEmail = session?.user?.email || DEFAULT_USER_EMAIL;

  const [selectedProcessId, setSelectedProcessId] = useState(null);
  const [showProcessPicker, setShowProcessPicker] = useState(false);
  const [processSearch, setProcessSearch] = useState("");
  const [editorTab, setEditorTab] = useState("form");
  const [executionMode, setExecutionMode] = useState("async");
  const [submitter, setSubmitter] = useState(sessionEmail);
  const [tags, setTags] = useState([]);
  const [tagInput, setTagInput] = useState("");
  const [showRecentPayloads, setShowRecentPayloads] = useState(false);
  const [savedTemplates, setSavedTemplates] = useState(() => loadSavedTemplates());
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [templateName, setTemplateName] = useState("");
  const [saveError, setSaveError] = useState("");
  const [showLoadPopover, setShowLoadPopover] = useState(false);
  const [deletingTemplateId, setDeletingTemplateId] = useState(null);
  const [rawJsonText, setRawJsonText] = useState({});

  useEffect(() => {
    if (session?.user?.email) setSubmitter(session.user.email);
  }, [session?.user?.email]);

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
    onError: (err) => {
      toast.error(err?.message || "Submission failed");
    }
  });

  const selectedProcessInfo = processList.find((p) => p.id === selectedProcessId);

  const filteredProcesses = useMemo(() => {
    const q = processSearch.toLowerCase();
    return processList.filter(
      (p) =>
        (p.title || "").toLowerCase().includes(q) ||
        (p.id || "").toLowerCase().includes(q)
    );
  }, [processList, processSearch]);

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

  const handleSelectProcess = (id) => {
    setSelectedProcessId(id);
    setShowProcessPicker(false);
    setProcessSearch("");
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
    setShowRecentPayloads(false);
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

  const handleAddTag = () => {
    const t = tagInput.trim();
    if (t && !tags.includes(t)) setTags((prev) => [...prev, t]);
    setTagInput("");
  };
  const handleRemoveTag = (t) => setTags((prev) => prev.filter((x) => x !== t));

  const handleSaveTemplate = () => {
    setSaveError("");
    const name = templateName.trim();
    if (!name) {
      setSaveError("Template name is required.");
      return;
    }
    if (savedTemplates.some((t) => t.name === name)) {
      setSaveError(
        "A template with that name already exists. Pick a different name."
      );
      return;
    }
    const id =
      typeof crypto !== "undefined" && crypto.randomUUID
        ? `tpl_${crypto.randomUUID()}`
        : `tpl_${Math.random().toString(36).slice(2)}`;
    const template = {
      id,
      name,
      processID: selectedProcessId || "",
      inputs: { ...watchedValues },
      tags: [...tags],
      savedAt: new Date().toISOString()
    };
    const next = [template, ...savedTemplates];
    setSavedTemplates(next);
    persistSavedTemplates(next);
    setShowSaveDialog(false);
    setTemplateName("");
    toast.success("Template saved.");
  };

  const handleLoadTemplate = (template) => {
    applyLoadedInputs(
      template.inputs || {},
      template.tags || [],
      template.processID
    );
    setShowLoadPopover(false);
  };

  const handleDeleteTemplate = (id) => {
    const next = savedTemplates.filter((t) => t.id !== id);
    setSavedTemplates(next);
    persistSavedTemplates(next);
    setDeletingTemplateId(null);
  };

  const onValidSubmit = (values) => {
    if (!selectedProcessId) return;
    const submitPayload = buildPayload(processDetail, values, tags);
    submitMutation.mutate({
      processID: selectedProcessId,
      payload: submitPayload,
      async: executionMode === "async",
      userEmail: submitter
    });
  };
  const triggerSubmit = form.handleSubmit(onValidSubmit);

  return (
    <div className="mx-auto max-w-[1600px] space-y-6 px-4 py-6 lg:px-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold">Builder</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Compose, validate, and submit job payloads
          </p>
        </div>
        <Popover open={showRecentPayloads} onOpenChange={setShowRecentPayloads}>
          <PopoverTrigger asChild>
            <Button variant="outline" size="sm" className="gap-2">
              <FolderOpen className="h-4 w-4" />
              Recent payloads
            </Button>
          </PopoverTrigger>
          <PopoverContent align="end" className="w-[360px] p-0">
            <div className="flex items-center justify-between border-b px-4 py-3">
              <h3 className="font-semibold">Recent payloads</h3>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                onClick={() => setShowRecentPayloads(false)}
                aria-label="Close"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="max-h-[400px] overflow-y-auto">
              {recentJobsQuery.isLoading ? (
                <div className="space-y-2 p-4">
                  <Skeleton className="h-12 w-full" />
                  <Skeleton className="h-12 w-full" />
                  <Skeleton className="h-12 w-full" />
                </div>
              ) : recentJobsQuery.isError ? (
                <div className="px-4 py-6 text-center text-sm text-muted-foreground">
                  Couldn&rsquo;t reach the API.
                </div>
              ) : recentJobs.length === 0 ? (
                <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                  No recent payloads yet. Submit a job to see it here.
                </div>
              ) : (
                <div className="divide-y">
                  {recentJobs.map((job) => (
                    <button
                      type="button"
                      key={job.jobID}
                      onClick={() => handleUseRecent(job)}
                      className="w-full px-4 py-3 text-left transition-colors hover:bg-accent"
                    >
                      <div className="flex items-start gap-3">
                        <StatusIcon status={job.status} />
                        <div className="min-w-0 flex-1">
                          <div className="mb-1 truncate text-sm font-medium">
                            {job.processID}
                          </div>
                          <div className="flex flex-wrap gap-1">
                            {(job.tags || []).map((tag) => (
                              <span
                                key={tag}
                                className="rounded bg-dewberry-teal/10 px-1.5 py-0.5 text-[10px] text-dewberry-teal"
                              >
                                {tag}
                              </span>
                            ))}
                          </div>
                        </div>
                        <div className="whitespace-nowrap text-xs text-muted-foreground">
                          {getRelativeTime(job.updated)}
                        </div>
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="border-t px-4 py-3">
              <button
                type="button"
                onClick={() => router.push("/jobs")}
                className="flex items-center gap-1 text-sm text-dewberry-teal hover:underline"
              >
                View all in Jobs
                <ArrowRight className="h-3 w-3" />
              </button>
            </div>
          </PopoverContent>
        </Popover>
      </div>

      <div className="relative">
        <Button
          type="button"
          variant="outline"
          onClick={() => setShowProcessPicker((v) => !v)}
          className="h-auto w-full justify-between py-3"
        >
          <div className="text-left">
            {selectedProcessInfo ? (
              <>
                <div className="font-semibold">
                  {selectedProcessInfo.title || selectedProcessInfo.id}
                </div>
                <div className="text-xs text-muted-foreground">
                  {selectedProcessInfo.id}
                  {selectedProcessInfo.version
                    ? ` · v${selectedProcessInfo.version}`
                    : ""}
                </div>
              </>
            ) : (
              <div className="text-muted-foreground">
                {processesQuery.isLoading
                  ? "Loading processes…"
                  : "Select a process..."}
              </div>
            )}
          </div>
          <ChevronDown className="ml-2 h-4 w-4" />
        </Button>

        {showProcessPicker && (
          <div className="absolute top-full z-50 mt-1 flex max-h-96 w-full flex-col overflow-hidden rounded-lg border bg-card shadow-xl">
            <div className="border-b p-2">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <input
                  type="text"
                  value={processSearch}
                  onChange={(e) => setProcessSearch(e.target.value)}
                  placeholder="Search processes..."
                  className="w-full rounded-md border bg-background py-2 pl-9 pr-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                  autoFocus
                />
              </div>
            </div>
            <div className="overflow-y-auto">
              {processesQuery.isError ? (
                <div className="px-3 py-4 text-sm text-muted-foreground">
                  Couldn&rsquo;t reach the API. Is Sepex running at{" "}
                  <code className="font-mono">{API_URL}</code>?
                </div>
              ) : processesQuery.isLoading ? (
                <div className="space-y-2 p-2">
                  <Skeleton className="h-10 w-full" />
                  <Skeleton className="h-10 w-full" />
                  <Skeleton className="h-10 w-full" />
                </div>
              ) : filteredProcesses.length === 0 ? (
                <div className="px-3 py-4 text-sm text-muted-foreground">
                  No processes found.
                </div>
              ) : (
                filteredProcesses.map((p) => (
                  <button
                    type="button"
                    key={p.id}
                    onClick={() => handleSelectProcess(p.id)}
                    className="w-full border-b px-3 py-2.5 text-left transition-colors last:border-0 hover:bg-accent"
                  >
                    <div className="font-medium">{p.title || p.id}</div>
                    <div className="text-xs text-muted-foreground">
                      {p.id}
                      {p.version ? ` · v${p.version}` : ""}
                    </div>
                  </button>
                ))
              )}
            </div>
          </div>
        )}
      </div>

      {selectedProcessInfo && (
        <div className="rounded-lg border bg-card p-4">
          <div className="mb-2 flex items-start justify-between gap-4">
            <div>
              <h3 className="font-semibold">
                {selectedProcessInfo.title || selectedProcessInfo.id}
              </h3>
              {selectedProcessInfo.description ? (
                <p className="mt-1 text-sm text-muted-foreground">
                  {selectedProcessInfo.description}
                </p>
              ) : null}
            </div>
            {selectedProcessInfo.version ? (
              <div className="font-mono text-xs text-muted-foreground">
                v{selectedProcessInfo.version}
              </div>
            ) : null}
          </div>
          {(selectedProcessInfo.jobControlOptions || []).length > 0 && (
            <div className="mt-3 flex flex-wrap gap-2">
              {selectedProcessInfo.jobControlOptions.map((opt) => (
                <span
                  key={opt}
                  className="rounded-md bg-dewberry-teal/10 px-2 py-1 text-xs font-medium text-dewberry-teal"
                >
                  {opt}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {selectedProcessId && processDetailQuery.isLoading && !processDetail && (
        <div className="grid gap-6 lg:grid-cols-2">
          <Skeleton className="h-[400px] w-full" />
          <Skeleton className="h-[400px] w-full" />
        </div>
      )}

      {selectedProcessId && processDetail && (
        <Form {...form}>
          <div className="grid gap-6 lg:grid-cols-2">
            <div className="space-y-4">
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
                        triggerSubmit();
                      }}
                      className="space-y-6"
                    >
                      {(processDetail.inputs || []).map((input) => {
                        const dataType = input.input?.literalDataDomain?.dataType;
                        const valueDef =
                          input.input?.literalDataDomain?.valueDefinition || {};
                        const required = (input.minOccurs || 0) > 0;
                        return (
                          <div key={input.id}>
                            <label className="mb-1.5 block text-sm font-medium">
                              {input.title || input.id}
                              {required && (
                                <span className="ml-1 text-status-failed">*</span>
                              )}
                            </label>
                            {input.description && (
                              <p className="mb-2 text-xs text-muted-foreground">
                                {input.description}
                              </p>
                            )}

                            <Controller
                              name={input.id}
                              control={form.control}
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
                                            e.target.value === ""
                                              ? undefined
                                              : Number(e.target.value)
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
                                          e.target.value === ""
                                            ? undefined
                                            : Number(e.target.value)
                                        )
                                      }
                                      onBlur={field.onBlur}
                                      placeholder="Enter integer…"
                                    />
                                  );
                                }
                                const raw =
                                  rawJsonText[input.id] ??
                                  (field.value
                                    ? JSON.stringify(field.value, null, 2)
                                    : "");
                                return (
                                  <Textarea
                                    rows={4}
                                    value={raw}
                                    onChange={(e) => {
                                      const text = e.target.value;
                                      setRawJsonText((p) => ({
                                        ...p,
                                        [input.id]: text
                                      }));
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
                      })}
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
                      ? "border-status-successful/20 bg-status-successful/10 text-status-successful"
                      : "border-status-running/20 bg-status-running/10 text-status-running"
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
            </div>

            <div className="space-y-4">
              <div className="rounded-lg border bg-card p-4">
                <div className="mb-3 flex items-center gap-2">
                  <Info className="h-4 w-4 text-muted-foreground" />
                  <h3 className="text-sm font-semibold">Payload Preview</h3>
                </div>
                <pre className="max-h-64 overflow-auto rounded-md bg-muted p-3 font-mono text-xs">
                  {payloadJSON}
                </pre>
              </div>

              <div className="rounded-lg border bg-card p-4">
                <label className="mb-2 block text-sm font-medium">
                  Submitter Email
                </label>
                <Input
                  type="email"
                  value={submitter}
                  onChange={(e) => setSubmitter(e.target.value)}
                  placeholder="user@example.com"
                />
              </div>

              <div className="rounded-lg border bg-card p-4">
                <label className="mb-2 block text-sm font-medium">Tags</label>
                {tags.length > 0 && (
                  <div className="mb-2 flex flex-wrap gap-2">
                    {tags.map((tag) => (
                      <span
                        key={tag}
                        className="inline-flex items-center gap-1 rounded-md bg-dewberry-teal/10 px-2 py-1 text-xs font-medium text-dewberry-teal"
                      >
                        {tag}
                        <button
                          type="button"
                          onClick={() => handleRemoveTag(tag)}
                          className="rounded-sm p-0.5 hover:bg-dewberry-teal/20"
                          aria-label={`Remove ${tag}`}
                        >
                          <X className="h-3 w-3" />
                        </button>
                      </span>
                    ))}
                  </div>
                )}
                <div className="flex gap-2">
                  <Input
                    value={tagInput}
                    onChange={(e) => setTagInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        handleAddTag();
                      }
                    }}
                    placeholder="Add tag…"
                  />
                  <Button
                    size="icon"
                    onClick={handleAddTag}
                    aria-label="Add tag"
                  >
                    <Plus className="h-3 w-3" />
                  </Button>
                </div>
              </div>

              <div className="rounded-lg border bg-card p-4">
                <label className="mb-3 block text-sm font-medium">
                  Execution Mode
                </label>
                <div className="space-y-2">
                  {[
                    {
                      id: "async",
                      label: "Async",
                      help: "Returns immediately with job ID"
                    },
                    {
                      id: "sync",
                      label: "Sync",
                      help: "Waits for completion and returns outputs"
                    }
                  ].map((mode) => (
                    <label
                      key={mode.id}
                      className="flex cursor-pointer items-center gap-3"
                    >
                      <input
                        type="radio"
                        name="execution-mode"
                        value={mode.id}
                        checked={executionMode === mode.id}
                        onChange={() => setExecutionMode(mode.id)}
                        className="h-4 w-4 border-input text-dewberry-teal focus:ring-2 focus:ring-ring"
                      />
                      <div>
                        <div className="text-sm font-medium">{mode.label}</div>
                        <div className="text-xs text-muted-foreground">
                          {mode.help}
                        </div>
                      </div>
                    </label>
                  ))}
                </div>
              </div>

              <div className="sticky bottom-4 rounded-lg border bg-card p-4 shadow-lg">
                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    variant="outline"
                    onClick={() => setShowSaveDialog(true)}
                    className="gap-2"
                    disabled={!selectedProcessId}
                  >
                    <Save className="h-4 w-4" />
                    Save
                  </Button>

                  <Popover
                    open={showLoadPopover}
                    onOpenChange={setShowLoadPopover}
                  >
                    <PopoverTrigger asChild>
                      <Button variant="outline" className="gap-2">
                        <FolderOpen className="h-4 w-4" />
                        Load
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent align="end" className="w-[360px] p-0">
                      <div className="border-b px-4 py-3">
                        <div className="mb-1 flex items-center justify-between">
                          <h3 className="font-semibold">Saved templates</h3>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-6 w-6"
                            onClick={() => setShowLoadPopover(false)}
                            aria-label="Close"
                          >
                            <X className="h-4 w-4" />
                          </Button>
                        </div>
                        <p className="text-xs italic text-muted-foreground">
                          Saved in your browser. Other devices won&rsquo;t see
                          these.
                        </p>
                      </div>
                      <div className="max-h-[400px] overflow-y-auto">
                        {savedTemplates.length === 0 ? (
                          <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                            No saved templates yet. Use Save to keep a payload
                            around for later.
                          </div>
                        ) : (
                          <div className="divide-y">
                            {savedTemplates
                              .slice()
                              .sort(
                                (a, b) =>
                                  new Date(b.savedAt).getTime() -
                                  new Date(a.savedAt).getTime()
                              )
                              .map((template) => (
                                <div key={template.id} className="px-4 py-3">
                                  {deletingTemplateId === template.id ? (
                                    <div className="flex items-center gap-2">
                                      <span className="text-sm">Delete?</span>
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        onClick={() =>
                                          handleDeleteTemplate(template.id)
                                        }
                                        className="h-7 text-xs"
                                      >
                                        Confirm
                                      </Button>
                                      <Button
                                        size="sm"
                                        variant="ghost"
                                        onClick={() =>
                                          setDeletingTemplateId(null)
                                        }
                                        className="h-7 text-xs"
                                      >
                                        Cancel
                                      </Button>
                                    </div>
                                  ) : (
                                    <div className="flex items-start gap-3">
                                      <div className="min-w-0 flex-1">
                                        <div className="mb-1 truncate text-sm font-medium">
                                          {template.name}
                                        </div>
                                        <div className="mb-1 truncate text-xs text-muted-foreground">
                                          {template.processID}
                                        </div>
                                        <div className="flex flex-wrap gap-1">
                                          {(template.tags || [])
                                            .slice(0, 3)
                                            .map((tag) => (
                                              <span
                                                key={tag}
                                                className="rounded bg-dewberry-teal/10 px-1.5 py-0.5 text-[10px] text-dewberry-teal"
                                              >
                                                {tag}
                                              </span>
                                            ))}
                                          {(template.tags || []).length > 3 && (
                                            <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                                              +{template.tags.length - 3}
                                            </span>
                                          )}
                                        </div>
                                      </div>
                                      <div className="flex flex-col items-end gap-1">
                                        <span className="whitespace-nowrap text-xs text-muted-foreground">
                                          {getRelativeTime(template.savedAt)}
                                        </span>
                                        <div className="flex gap-1">
                                          <Button
                                            size="sm"
                                            onClick={() =>
                                              handleLoadTemplate(template)
                                            }
                                            className="h-7 text-xs"
                                          >
                                            Use
                                          </Button>
                                          <Button
                                            size="sm"
                                            variant="ghost"
                                            onClick={() =>
                                              setDeletingTemplateId(template.id)
                                            }
                                            className="h-7 w-7 p-0"
                                            aria-label="Delete template"
                                          >
                                            <Trash2 className="h-3 w-3" />
                                          </Button>
                                        </div>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              ))}
                          </div>
                        )}
                      </div>
                    </PopoverContent>
                  </Popover>

                  <div className="ml-auto">
                    <Button
                      onClick={triggerSubmit}
                      disabled={!isFormValid || submitMutation.isPending}
                      className="gap-2 bg-dewberry-magenta text-white hover:bg-dewberry-magenta/90"
                    >
                      {submitMutation.isPending ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Send className="h-4 w-4" />
                      )}
                      Submit
                    </Button>
                  </div>
                </div>
              </div>
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

      <Dialog
        open={showSaveDialog}
        onOpenChange={(o) => {
          setShowSaveDialog(o);
          if (!o) {
            setTemplateName("");
            setSaveError("");
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Save Template</DialogTitle>
          </DialogHeader>
          <div className="space-y-2">
            <label className="block text-sm font-medium">Template name</label>
            <Input
              value={templateName}
              onChange={(e) => {
                setTemplateName(e.target.value);
                setSaveError("");
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  handleSaveTemplate();
                }
              }}
              placeholder="e.g. Weekly Baseline"
              autoFocus
            />
            {saveError && (
              <p className="text-xs text-status-failed">{saveError}</p>
            )}
            <p className="text-xs italic text-muted-foreground">
              Saved in your browser. Other devices won&rsquo;t see these.
            </p>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSaveDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleSaveTemplate}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
