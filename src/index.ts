import { extractAndParseTable, type AzureTable } from "./handle-tables";
import { analyzeDocument } from "./document-intelligence";

async function main() {
  const data = await analyzeDocumentCached("./assets/ANEXO XIV.pdf", `3-28`);
  if (!data.tables) {
    console.error("No tables found");
    process.exit(1);
  }

  const htmlsPerPage: Record<number, string[]> = {};

  for (const table of data.tables) {
    const result = extractAndParseTable(table);
    if (!result) continue;

    const page = table.boundingRegions?.[0]?.pageNumber
      ? table.boundingRegions[0].pageNumber
      : 0;

    htmlsPerPage[page] = htmlsPerPage[page] || [];
    await writeResults(page, result);

    const { html, json } = result;
    htmlsPerPage[page].push(html);

    const lds = json.filter((item) => item.documentName.startsWith("LD-"));
    for (const item of lds) {
      console.log(page, item.documentName);
    }
  }

  await Bun.write(`./tables/index.html`, joinHTMLPageTables(htmlsPerPage));
}

main();

async function analyzeDocumentCached(path: string, pages: string) {
  const jsonPath = path.replace(".pdf", ".json");
  if (await Bun.file(jsonPath).exists()) {
    return Bun.file(jsonPath).json() as Promise<{ tables: AzureTable[] }>;
  }
  const data = await analyzeDocument(path, pages);
  await Bun.write(jsonPath, JSON.stringify(data, null, 2));
  return data;
}

async function writeResults(
  page: number,
  result: NonNullable<ReturnType<typeof extractAndParseTable>>,
) {
  const { csv, html, json } = result;

  await Bun.write(`./tables/table_${page}.html`, html);
  await Bun.write(`./tables/table_${page}.csv`, csv);
  await Bun.write(`./tables/table_${page}.json`, JSON.stringify(json, null, 2));
}

function joinHTMLPageTables(htmlsPerPage: Record<number, string[]>) {
  return Object.entries(htmlsPerPage)
    .map(([page, htmls]) => `<h1>Page ${page}</h1> <br/>${htmls.join("<br/>")}`)
    .join("<br/><br/>");
}
