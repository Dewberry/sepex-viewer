"use client";

import { useEffect, useMemo, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import {
  buildPayload,
  buildYaml,
  buildZodSchema
} from "@/app/(dashboard)/builder/_utils/schema";

// Owns react-hook-form state, validation against the process input schema, and
// the live JSON/YAML payload preview. Also stashes the raw text for object
// inputs so the editor can roundtrip free-form JSON without losing keystrokes.
export default function usePayloadForm({ processDetail, tags }) {
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

  const [rawJsonText, setRawJsonText] = useState({});

  useEffect(() => {
    if (processDetail) form.trigger();
  }, [processDetail, form]);

  const requiredInputs = (processDetail?.inputs || []).filter(
    (i) => (i.minOccurs || 0) > 0
  );
  const requiredCount = requiredInputs.length;
  const filledRequiredCount = requiredInputs.filter((i) => {
    const v = watchedValues[i.id];
    return v !== undefined && v !== null && v !== "";
  }).length;
  const isFormValid =
    Boolean(processDetail) &&
    requiredCount === filledRequiredCount &&
    form.formState.isValid;

  const livePayload = buildPayload(processDetail, watchedValues, tags);
  const payloadJSON = JSON.stringify(livePayload, null, 2);
  const payloadYAML = buildYaml(livePayload);

  const loadInputs = (inputs) => {
    form.reset(inputs || {});
    const seed = {};
    if (inputs) {
      for (const [k, v] of Object.entries(inputs)) {
        if (v && typeof v === "object") seed[k] = JSON.stringify(v, null, 2);
      }
    }
    setRawJsonText(seed);
  };

  const resetAll = () => {
    form.reset({});
    setRawJsonText({});
  };

  return {
    form,
    isFormValid,
    requiredCount,
    filledRequiredCount,
    livePayload,
    payloadJSON,
    payloadYAML,
    rawJsonText,
    setRawJsonText,
    loadInputs,
    resetAll
  };
}
