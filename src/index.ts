import { extractAndParseTable, type AzureTable } from "./handle-tables";
import { analyzeDocument } from "./document-intelligence";
import { csv2json } from "./csv2json";

async function main() {
  const data = await analyzeDocumentCached("ANEXO XIV.pdf", `3-28`);
  if (!data.tables) {
    console.error("No tables found");
    process.exit(1);
  }

  const pageIndexes: Record<number, number> = {};
  const htmlsPerPage: Record<number, string[]> = {};

  for (const table of data.tables) {
    const result = extractAndParseTable(table);
    if (!result) continue;

    const page = table.boundingRegions?.[0]?.pageNumber
      ? table.boundingRegions[0].pageNumber
      : 0;
    pageIndexes[page] = pageIndexes[page] ? pageIndexes[page] + 1 : 1;
    htmlsPerPage[page] = htmlsPerPage[page] || [];

    const { csv, html } = result;
    htmlsPerPage[page].push(html);

    const json = csv2json(csv, {
      ITEM: "item",
      REVISÃO: "revision",
      NÚMERO: "documentName",
      NUMERO: "documentName",
      "TÍTULO DO DOCUMENTO": "title",
    });

    const outputName = `table_${page}_${pageIndexes[page]}`;
    await Bun.write(`./tables/${outputName}.html`, html);
    await Bun.write(`./tables/${outputName}.csv`, csv);
    await Bun.write(
      `./tables/${outputName}.json`,
      JSON.stringify(json, null, 2),
    );
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

function joinHTMLPageTables(htmlsPerPage: Record<number, string[]>) {
  return Object.entries(htmlsPerPage)
    .map(([page, htmls]) => `<h1>Page ${page}</h1> <br/>${htmls.join("<br/>")}`)
    .join("<br/><br/>");
}
