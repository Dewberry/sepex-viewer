import {
  jsonResponse,
  maybeDelay,
  notFound
} from "@/app/api/mock/_lib/helpers";
import { store } from "@/app/api/mock/_lib/store";

export async function GET(request, { params }) {
  await maybeDelay(request);
  const { id } = await params;
  const proc = store.processes.find((p) => p.id === id);
  if (!proc) return notFound(`Process ${id} not found`);

  const { inputs = [], outputs = [], ...info } = proc;
  return jsonResponse({
    info,
    inputs,
    outputs,
    links: [{ href: `/processes/${id}`, rel: "self", title: "this document" }]
  });
}
