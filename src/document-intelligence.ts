import {
  AzureKeyCredential,
  DocumentAnalysisClient,
} from "@azure/ai-form-recognizer";

export async function analyzeDocument(path: string, pages: string) {
  if (
    !Bun.env.AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT ||
    !Bun.env.AZURE_DOCUMENT_INTELLIGENCE_API_KEY
  ) {
    console.error("Missing environment variables");
    process.exit(1);
  }

  const pdfBuffer = await Bun.file(path).arrayBuffer();

  const client = new DocumentAnalysisClient(
    Bun.env.AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT,
    new AzureKeyCredential(Bun.env.AZURE_DOCUMENT_INTELLIGENCE_API_KEY),
  );

  const result = await client
    .beginAnalyzeDocument("prebuilt-layout", pdfBuffer, { pages })
    .then((poller) => poller.pollUntilDone());

  return {
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
}

function omit<T extends Record<string, any>, K extends keyof T>(
  obj: T,
  keys: K[],
): Omit<T, K> {
  return Object.fromEntries(
    Object.entries(obj).filter(([key]) => !keys.includes(key as K)),
  ) as Omit<T, K>;
}
