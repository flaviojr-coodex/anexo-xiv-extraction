import { tableToCSV, tableToHTML, type AzureTable } from "./handle-tables";
import { analyzeDocument } from "./document-intelligence";

async function main() {
  const data = await analyzeDocumentCached("ANEXO XIV.pdf", `3-28`);
  if (!data.tables) {
    console.error("No tables found");
    process.exit(1);
  }

  const pageIndexes: Record<number, number> = {};
  const htmlsPerPage: Record<number, string[]> = {};

  for (const table of data.tables) {
    const page = table.boundingRegions?.[0]?.pageNumber
      ? table.boundingRegions[0].pageNumber
      : 0;

    let csv = tableToCSV(table);
    let strCsv = csv.join("\n");
    let rowsOffset = 0;

    if (!strCsv.includes("TÍTULO DO DOCUMENTO")) continue; // Relevant documents table
    if (!csv[0]?.includes("TÍTULO DO DOCUMENTO")) {
      while (!csv[0]?.includes("TÍTULO DO DOCUMENTO")) {
        csv.shift();
        strCsv = csv.join("\n");
        rowsOffset++;
      }
    }

    // if (csv[0].split(",").length === 4) continue;

    // Handle bad format on table columns
    const desiredColumns = 4;
    let columnFix = undefined;

    if (csv[0].split(",").length !== desiredColumns) {
      columnFix = {
        desiredColumns,
        skipColumns: 2,
        joinUntil: -1,
      };

      const options = {
        skipRows: rowsOffset,
        columnFix,
      };

      csv = tableToCSV(table, options);
      strCsv = csv.join("\n");
    }

    const html = tableToHTML(table, { skipRows: rowsOffset, columnFix });

    pageIndexes[page] = pageIndexes[page] ? pageIndexes[page] + 1 : 1;
    htmlsPerPage[page] = htmlsPerPage[page] || [];
    htmlsPerPage[page].push(html);

    const outputName = `table_${page}_${pageIndexes[page]}`;

    await Bun.write(`./tables/${outputName}.html`, html);
    await Bun.write(`./tables/${outputName}.csv`, strCsv);
  }

  await Bun.write(
    `./tables/index.html`,
    Object.entries(htmlsPerPage)
      .map(
        ([page, htmls]) => `<h1>Page ${page}</h1> <br/>${htmls.join("<br/>")}`,
      )
      .join("<br/><br/>"),
  );
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
