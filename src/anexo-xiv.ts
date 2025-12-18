import { csv2json } from "./csv2json";
import { analyzeDocumentCached } from "./document-intelligence";
import { tableToCSV, tableToHTML, type AzureTable } from "./handle-tables";

export async function handleAnexoXIVCached() {
  const data = await analyzeDocumentCached("./assets/ANEXO XIV.pdf", `3-28`);
  if (!data.tables) {
    console.error("No tables found");
    process.exit(1);
  }

  const htmlsPerPage: Record<number, string[]> = {};
  const rowsPerPage: Record<
    number,
    NonNullable<ReturnType<typeof extractAndParseAnexoXIVTable>>["json"]
  > = {};

  for (const table of data.tables) {
    const result = extractAndParseAnexoXIVTable(table);
    if (!result) continue;

    const page = table.boundingRegions?.[0]?.pageNumber
      ? table.boundingRegions[0].pageNumber
      : 0;

    htmlsPerPage[page] = htmlsPerPage[page] || [];
    await writeResults(page, result);

    const { html, json } = result;
    htmlsPerPage[page].push(html);
    rowsPerPage[page] = json;
  }

  await Bun.write(`./tables/xiv/index.html`, joinHTMLPageTables(htmlsPerPage));

  return rowsPerPage;
}

function extractAndParseAnexoXIVTable(table: AzureTable) {
  let csv = tableToCSV(table);
  let strCsv = csv.join("\n");
  let rowsOffset = 0;

  if (!strCsv.includes("TÍTULO DO DOCUMENTO")) return; // Relevant documents table
  if (!csv[0]?.includes("TÍTULO DO DOCUMENTO")) {
    while (!csv[0]?.includes("TÍTULO DO DOCUMENTO")) {
      csv.shift();
      strCsv = csv.join("\n");
      rowsOffset++;
    }
  }

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

  const json = csv2json(csv, {
    // ITEM: "item",
    // REVISÃO: "revision",
    NÚMERO: "documentName",
    NUMERO: "documentName",
    // "TÍTULO DO DOCUMENTO": "title",
  });

  return {
    json,
    csv: strCsv,
    html: tableToHTML(table, { skipRows: rowsOffset, columnFix }),
  };
}

async function writeResults(
  page: number,
  result: NonNullable<ReturnType<typeof extractAndParseAnexoXIVTable>>,
) {
  const { csv, html, json } = result;

  // await Bun.write(`./tables/xiv/table_${page}.html`, html);
  await Bun.write(`./tables/xiv/table_${page}.csv`, csv);
  await Bun.write(
    `./tables/xiv/table_${page}.json`,
    JSON.stringify(json, null, 2),
  );
}

function joinHTMLPageTables(htmlsPerPage: Record<number, string[]>) {
  return Object.entries(htmlsPerPage)
    .map(([page, htmls]) => `<h1>Page ${page}</h1> <br/>${htmls.join("<br/>")}`)
    .join("<br/><br/>");
}
