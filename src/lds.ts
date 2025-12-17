import { csv2json } from "./csv2json";
import { analyzeDocumentCached } from "./document-intelligence";
import { tableToCSV, tableToHTML, type AzureTable } from "./handle-tables";

export async function handleLDCached(path: string) {
  const data = await analyzeDocumentCached(path);
  if (!data.tables) {
    console.error("No tables found in" + path);
    return;
  }

  const htmlsPerPage: Record<number, string[]> = {};
  const rowsPerPage: Record<
    number,
    NonNullable<ReturnType<typeof extractAndParseTable>>["json"]
  > = {};

  for (const table of data.tables) {
    const result = extractAndParseTable(table);
    if (!result) continue;

    const page = table.boundingRegions?.[0]?.pageNumber
      ? table.boundingRegions[0].pageNumber
      : 0;

    htmlsPerPage[page] = htmlsPerPage[page] || [];
    await writeResults(path, page, result);

    const { html, json } = result;
    htmlsPerPage[page].push(html);
    rowsPerPage[page] = json;
  }

  if (!Object.keys(htmlsPerPage).length) {
    console.error("No tables found in" + path);
    return;
  }

  await Bun.write(
    `./tables/${path.split("/").pop()}/index.html`,
    joinHTMLPageTables(htmlsPerPage),
  );

  return rowsPerPage;
}

function extractAndParseTable(table: AzureTable) {
  let csv = tableToCSV(table);
  let strCsv = csv.join("\n");
  let rowsOffset = 0;

  const match = "titulo do documento";
  if (!normalizeLower(strCsv).includes(match)) return; // Relevant documents table
  if (csv[0] && !normalizeLower(csv[0]).includes(match)) {
    while (!normalizeLower(csv[0]).includes(match)) {
      csv.shift();
      strCsv = csv.join("\n");
      rowsOffset++;
    }
  }

  // Handle bad format on table columns
  const desiredColumns = 4;
  let columnFix = undefined;

  // if (csv[0].split(",").length !== desiredColumns) {
  //   columnFix = {
  //     desiredColumns,
  //     skipColumns: 2,
  //     joinUntil: -1,
  //   };

  //   const options = {
  //     skipRows: rowsOffset,
  //     columnFix,
  //   };

  //   csv = tableToCSV(table, options);
  //   strCsv = csv.join("\n");
  // }

  const json = csv2json(csv, {
    "Nº DO DOCUMENTO": "documentName",
    NÚMERO: "documentName",
    NUMERO: "documentName",
  });

  return {
    json,
    csv: strCsv,
    html: tableToHTML(table, { skipRows: rowsOffset, columnFix }),
  };
}

async function writeResults(
  path: string,
  page: number,
  result: NonNullable<ReturnType<typeof extractAndParseTable>>,
) {
  const { csv, html, json } = result;
  const parsedPath = path.split("/").pop();

  try {
    await Bun.write(`./tables/${parsedPath}/table_${page}.html`, html);
    await Bun.write(`./tables/${parsedPath}/table_${page}.csv`, csv);
    await Bun.write(
      `./tables/${parsedPath}/table_${page}.json`,
      JSON.stringify(json, null, 2),
    );
  } catch (error) {
    console.error(`Error writing results for page ${page}:`, error);
  }
}

function joinHTMLPageTables(htmlsPerPage: Record<number, string[]>) {
  return Object.entries(htmlsPerPage)
    .map(([page, htmls]) => `<h1>Page ${page}</h1> <br/>${htmls.join("<br/>")}`)
    .join("<br/><br/>");
}

function normalizeLower(str: string) {
  return str
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}
