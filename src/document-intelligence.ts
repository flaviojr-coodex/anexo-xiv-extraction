import {
  AzureKeyCredential,
  DocumentAnalysisClient,
} from "@azure/ai-form-recognizer";
import { pino } from "pino";
import type { AzureTable } from "./handle-tables";

const logger = pino({
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
    },
  },
});

export async function analyzeDocument(path: string, pages?: string) {
  if (
    !Bun.env.AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT ||
    !Bun.env.AZURE_DOCUMENT_INTELLIGENCE_API_KEY
  ) {
    logger.error(
      "Missing required environment variables: AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT and/or AZURE_DOCUMENT_INTELLIGENCE_API_KEY",
    );
    process.exit(1);
  }

  const pdfBuffer = await Bun.file(path).arrayBuffer();

  const client = new DocumentAnalysisClient(
    Bun.env.AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT,
    new AzureKeyCredential(Bun.env.AZURE_DOCUMENT_INTELLIGENCE_API_KEY),
  );

  const result = await client
    .beginAnalyzeDocument("prebuilt-layout", pdfBuffer, { pages })
    .then((poller) => {
      logger.debug("Analysis polling started, waiting for completion...");
      return poller.pollUntilDone();
    });

  const processedResult = {
    ...omit(result, ["pages", "styles", "paragraphs"]),
    tables: result.tables?.map((table) => ({
      ...omit(table, ["spans"]),
      boundingRegions: table.boundingRegions?.map((region) =>
        omit(region, ["polygon"]),
      ),
      cells: table.cells?.map((cell) =>
        omit(cell, ["boundingRegions", "spans"]),
      ),
    })),
  };

  logger.debug(`Processed and formatted analysis result`);
  return processedResult;
}

export async function analyzeDocumentCached(path: string, pages?: string) {
  const jsonPath = path.replace(".pdf", ".json").replace(".PDF", ".json");

  if (await Bun.file(jsonPath).exists()) {
    return Bun.file(jsonPath).json() as Promise<{ tables: AzureTable[] }>;
  }
  const data = await analyzeDocument(path, pages);
  await Bun.write(jsonPath, JSON.stringify(data, null, 2));
  return data;
}

function omit<T extends Record<string, any>, K extends keyof T>(
  obj: T,
  keys: K[],
): Omit<T, K> {
  return Object.fromEntries(
    Object.entries(obj).filter(([key]) => !keys.includes(key as K)),
  ) as Omit<T, K>;
}
