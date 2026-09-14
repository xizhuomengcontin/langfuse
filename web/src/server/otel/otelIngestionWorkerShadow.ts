import { existsSync } from "node:fs";
import path from "node:path";

import {
  logger,
  recordDistribution,
  recordIncrement,
} from "@langfuse/shared/src/server";
import type { NextApiResponse } from "next";
import PQueue from "p-queue";
import Piscina from "piscina";

import type {
  OtelIngestionWorkerRequest,
  OtelIngestionWorkerResult,
} from "./otelIngestionWorker";

const MAX_WORKER_QUEUE_DEPTH = 2;

let lastProcessingDurationMs: number | undefined;
let workerPool:
  | Piscina<OtelIngestionWorkerRequest, OtelIngestionWorkerResult>
  | undefined;
let workerPreloadPromise: Promise<void> | undefined;
const workerQueue = new PQueue({ concurrency: 1 });

function getWorkerFilename(): string {
  const distDir = process.env.NEXT_DIST_DIR || ".next";
  const serverEntry = process.argv[1];
  const candidates = [
    ...(serverEntry
      ? [
          path.join(
            path.dirname(path.resolve(serverEntry)),
            "otelIngestionWorker.js",
          ),
        ]
      : []),
    path.join(process.cwd(), "web", "otelIngestionWorker.js"),
    path.join(
      process.cwd(),
      distDir,
      "standalone",
      "web",
      "otelIngestionWorker.js",
    ),
  ];
  const filename = candidates.find((candidate) => existsSync(candidate));

  if (!filename) {
    throw new Error(
      "OTel ingestion worker artifact is missing; run the web build before enabling the admission shadow",
    );
  }

  return filename;
}

function getWorkerPool(): Piscina<
  OtelIngestionWorkerRequest,
  OtelIngestionWorkerResult
> {
  workerPool ??= new Piscina({
    filename: getWorkerFilename(),
    minThreads: 1,
    maxThreads: 1,
    maxQueue: 0,
    atomics: "disabled",
  });
  return workerPool;
}

function scheduleAdmissionShadow(projectId: string, durationMs: number): void {
  if (workerQueue.pending + workerQueue.size >= MAX_WORKER_QUEUE_DEPTH) {
    recordIncrement("langfuse.ingestion.otel.worker_shadow.admission", 1, {
      outcome: "would_reject",
    });
    logger.warn("OTel ingestion worker shadow would reject request", {
      projectId,
      simulatedProcessingDurationMs: durationMs,
    });
    return;
  }

  recordIncrement("langfuse.ingestion.otel.worker_shadow.admission", 1, {
    outcome: "admitted",
  });
  workerQueue
    .add(async () => {
      const result = await getWorkerPool().run({
        type: "shadow",
        durationMs,
      });
      if (result.kind !== "shadow") {
        throw new Error(
          "OTel ingestion worker returned an invalid shadow result",
        );
      }
    })
    .catch((error: unknown) => {
      logger.error("OTel ingestion worker shadow task failed", error);
    });
}

export function startOtelIngestionWorkerAdmissionShadow(
  res: NextApiResponse,
  projectId: string,
): void {
  if (lastProcessingDurationMs !== undefined) {
    scheduleAdmissionShadow(projectId, lastProcessingDurationMs);
  }

  const startedAt = performance.now();
  let completed = false;
  function recordProcessingDuration() {
    if (completed) return;
    completed = true;
    res.off("finish", recordProcessingDuration);
    res.off("close", recordProcessingDuration);
    const processingDurationMs = Math.max(
      1,
      Math.round(performance.now() - startedAt),
    );
    lastProcessingDurationMs = processingDurationMs;
    recordDistribution(
      "langfuse.ingestion.otel.worker_shadow.real_processing_duration_ms",
      processingDurationMs,
    );
  }

  res.once("finish", recordProcessingDuration);
  res.once("close", recordProcessingDuration);
}

async function runWorkerPreload(): Promise<void> {
  const result = await getWorkerPool().run({ type: "warmup" });
  if (result.kind !== "warmup") {
    throw new Error(
      "OTel ingestion worker did not report ready during warm-up",
    );
  }
  logger.info("OTel ingestion worker shadow preloaded");
}

export function preloadOtelIngestionWorkerShadow(): Promise<void> {
  workerPreloadPromise ??= runWorkerPreload();
  return workerPreloadPromise;
}
