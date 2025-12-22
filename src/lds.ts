import { tableToCSV, tableToHTML, type AzureTable } from "./handle-tables";
import { analyzeDocumentCached } from "./document-intelligence";
import { fixMergedRevisionColumns } from "./fix-revision";
import { csv2json } from "./csv2json";

const DOCUMENT_NAME_COLUMN_MAP = {
  "NUMERO DO DOCUMENTO PETROBRAS": "documentName",
  "Nº PETROBRAS ( N-1710 )": "documentName",
  "Nº PETROBRÁS ( N-1710 )": "documentName",
  "Nº PETROBRAS (N-1710 )": "documentName",
  "Numero do Documento": "documentName",
  "NUMERO DO DOCUMENTO": "documentName",
  "NÚMERO DO DOCUMENTO": "documentName",
  ". Nº DO DOCUMENTO": "documentName",
  "NÚM. DO DOCUMENTO": "documentName",
  "NUMERO DOCUMENTO": "documentName",
  "NÚMERO DOCUMENTO": "documentName",
  "DOCUMENTO N-1710": "documentName",
  "NÚMERO PETROBRAS": "documentName",
  "IN° DO DOCUMENTO": "documentName",
  "Nº DO DOCUMENTO": "documentName",
  "Nº N-1710 1710": "documentName",
  "Número DIGIMAT": "documentName",
  "NºDO DOCUMENTO": "documentName",
  "CODIGO N-1710": "documentName",
  "CÓDIGO N-1710": "documentName",
  "Nº PETROBRAS": "documentName",
  "Nº DOCUMENTO": "documentName",
  "Nr. COMPERJ": "documentName",
  "O DOCUMENTO": "documentName",
  "Nº Cliente": "documentName",
  "Número CBM": "documentName",
  "Nº N-1710": "documentName",
  "Nr.N1710": "documentName",
  "Nº 1710": "documentName",
  Número: "documentName",
  CÓDIGO: "documentName",
  NÚMERO: "documentName",
  NUMERO: "documentName",
};

const REVISION_COLUMN_MAP = {
  "Numero da Revisão": "revision",
  "REV. (ATUAL)": "revision",
  "REV (ATUAL)": "revision",
  "REV TATE": "revision",
  REVISAO: "revision",
  REVISÃO: "revision",
  "BEY.": "revision",
  "BEV.": "revision",
  "Rev.": "revision",
  "FEV.": "revision",
  "REV.": "revision",
  Bev: "revision",
  BEV: "revision",
  REY: "revision",
  Rev: "revision",
  REV: "revision",
  Rey: "revision",
  Re: "revision",
};

const COLUMN_NAMES = { ...DOCUMENT_NAME_COLUMN_MAP, ...REVISION_COLUMN_MAP };

const excludedColumns = ["O DOCUMENTO"];

const matches = [
  ...Object.keys(DOCUMENT_NAME_COLUMN_MAP).filter(
    (key) => !excludedColumns.includes(key),
  ),
  ...[...Object.keys(REVISION_COLUMN_MAP), ...excludedColumns].flatMap((c) => [
    `"${c}",`,
    `,"${c}",`,
    `,"${c}"`,
  ]),
];

const desiredColumnNames = Array.from(new Set(Object.values(COLUMN_NAMES)));

export async function handleLDCached(path: string) {
  const data = await analyzeDocumentCached(path);
  if (!data.tables) {
    console.error("No tables found in" + path);
    return;
  }

  data.tables = fixTableByName(path, data.tables);

  const htmlsPerPage: Record<number, string[]> = {};
  const rowsPerPage: Record<
    number,
    NonNullable<ReturnType<typeof extractAndParseTable>>["json"]
  > = {};

  const folder = path.split("/").pop()!;

  for (const table of data.tables) {
    const result = extractAndParseTable(table, folder);
    if (!result) continue;

    const page = table.boundingRegions?.[0]?.pageNumber
      ? table.boundingRegions[0].pageNumber
      : 0;

    htmlsPerPage[page] = htmlsPerPage[page] || [];
    await writeResults(folder, page, result);

    const { html, json } = result;

    htmlsPerPage[page].push(html);
    rowsPerPage[page] = json;
  }

  if (!Object.keys(htmlsPerPage).length) {
    console.error(`[${folder}] No tables found on document`);
    return;
  }

  await Bun.write(
    `./tables/${folder}/index.html`,
    joinHTMLPageTables(htmlsPerPage),
  );

  return rowsPerPage;
}

function hasMatch(matches: string[], search: string) {
  return matches.some((match) =>
    normalizeLower(search).includes(normalizeLower(match)),
  );
}

function extractAndParseTable(table: AzureTable, folder: string) {
  let csv = tableToCSV(table);
  let strCsv = csv.join("\n");
  let rowsOffset = 0;

  if (!hasMatch(matches, strCsv)) return; // Relevant documents table
  if (csv[0] && !hasMatch(matches, csv[0])) {
    const MAX_ITERATIONS = 200;
    let iterationCount = 0;

    while (!hasMatch(matches, csv[0])) {
      csv.shift();
      strCsv = csv.join("\n");
      rowsOffset++;

      if (iterationCount++ > MAX_ITERATIONS)
        throw new Error("Maximum iterations reached");
    }
  }

  const json = csv2json(csv, COLUMN_NAMES);
  if (
    !json.every((row) => desiredColumnNames.every((column) => column in row))
  ) {
    const missingJsonFields = Array.from(
      new Set(Object.values(COLUMN_NAMES).filter((key) => !(key in json[0]!))),
    );

    if (missingJsonFields.length === 1 && missingJsonFields[0] === "revision") {
      // const page = table.boundingRegions?.[0]?.pageNumber || 0;
      // console.error(
      //   `[${folder}] Cannot find mapped columns ${missingJsonFields} | PAGE ${page}`,
      // );
      // console.log(JSON.stringify(json.slice(0, 3), null, 2));
    }

    return;
  }

  return {
    json,
    csv: strCsv,
    html: tableToHTML(table, { skipRows: rowsOffset }),
  };
}

async function writeResults(
  folder: string,
  page: number,
  result: NonNullable<ReturnType<typeof extractAndParseTable>>,
) {
  const { csv, html, json } = result;

  try {
    // await Bun.write(`./tables/${folder}/table_${page}.html`, html);
    await Bun.write(`./tables/${folder}/table_${page}.csv`, csv);
    await Bun.write(
      `./tables/${folder}/table_${page}.json`,
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

function fixTableByName(name: string, tables: AzureTable[]): AzureTable[] {
  if (name.includes("LD-5400.00-5606-744-AFK-503=A")) {
    if (!tables[4] || !tables[4].cells[4]) {
      console.warn(
        "[LD-5400.00-5606-744-AFK-503=A] Could not find table 4 to fix extraction",
      );
      return tables;
    }

    const newTables = [...tables];

    const corrrectSecondCell = newTables[4]!.cells[4]!;
    corrrectSecondCell.columnSpan = 16;

    const correctHeaderCells = [
      "", // blank - OCR error
      "Nº",
      "EAP",
      "Nº DO DOCUMENTO",
      "TITULO DO DOCUMENTO",
      "REV.",
      "QT. FOLHA",
      "Nº DOCUMENTO FORNECIMENTO",
      "TAG",
      "", // blank - OCR error
      "DATA PREVISTA",
      "GRDT TRANSMITIDA",
      "DATA GRDT TRANSMITIDA",
      "STATUS",
      "V ou I",
      "", // blank - OCR error
    ].map((content, i) => ({
      kind: "columnHeader",
      columnIndex: i,
      columnSpan: 1,
      rowIndex: 2,
      rowSpan: 1,
      content,
    })) as AzureTable["cells"];

    newTables[4]!.cells = [
      ...correctHeaderCells,
      corrrectSecondCell,
      ...tables[4].cells
        .slice(6)
        .map((cell) => ({ ...cell, rowIndex: cell.rowIndex + 1 })),
    ];

    return newTables;
  }

  if (name.includes("LD-5400.00-5131-811-WIC-002=D")) {
    const newTables = [...tables];
    newTables[5]!.cells[3]!.content = "REV.";
    newTables.splice(2, 1);
    return newTables;
  }

  if (name.includes("LD-5400.00-5606-940-VWI-501=N")) {
    const newTables = [...tables];
    newTables[5]!.cells[6]!.content = "REV.";
    newTables.splice(2, 1);
    return newTables;
  }

  if (name.includes("LD-5400.00-5151-940-VWI-501=L")) {
    const newTables = [...tables];
    newTables[5]!.cells[7]!.content = "REV.";
    newTables.splice(2, 1);
    return newTables;
  }

  if (name.includes("LD-5400.00-5131-947-MBV-001=B")) {
    const newTables = [...tables];
    return newTables.slice(5);
  }

  if (name.includes("LD-5400.00-5156-940-VWI-501=N")) {
    const newTables = [...tables];
    newTables[5]!.cells[8]!.content = "REV.";
    return newTables.slice(5);
  }

  if (name.includes("LD-5400.00-5147-970-XCO-001=A")) {
    const newTables = [...tables];
    newTables[3]!.cells = [
      ...newTables[3]!.cells
        .filter(
          (c) =>
            c.rowIndex >= 4 && (c.columnIndex === 3 || c.columnIndex === 4),
        )
        .map((c) => ({
          ...c,
          rowIndex: c.rowIndex - 4,
          columnIndex: c.columnIndex - 3,
        })),
      ...newTables[4]!.cells
        .filter(
          (c) =>
            c.rowIndex >= 2 && (c.columnIndex === 4 || c.columnIndex === 5),
        )
        .map((c) => ({
          ...c,
          rowIndex: c.rowIndex - 1,
          columnIndex: c.columnIndex - 4,
        })),
    ];

    return [newTables[3]!];
  }

  if (name.includes("LD-5400.00-5131-811-FRB-002=C")) {
    const newTables = [...tables];
    newTables[4]!.cells[3]!.content = "REV.";
    return newTables;
  }

  if (name.includes("LD-5400.00-5131-812-FRB-003=B")) {
    const newTables = [...tables];
    newTables[4]!.cells[3]!.content = "REV.";
    return newTables;
  }

  if (name.includes("LD-5400.00-5147-940-XCO-001=0")) {
    const newTables = [...tables];
    newTables[3]!.cells = [
      ...newTables[3]!.cells
        .slice(13)
        .map((c) => ({ ...c, rowIndex: c.rowIndex - 4 })),
      ...newTables[4]!.cells.slice(22),
    ]
      .filter((c) => c.columnIndex === 3 || c.columnIndex === 4)
      .map((cell) => ({
        ...cell,
        columnIndex: cell.columnIndex - 3,
      }));

    newTables[3]!.columnCount = 2;
    newTables[3]!.rowCount = newTables[3]!.cells.length / 2;

    return [newTables[3]!];
  }

  if (name.includes("LD-5400.00-5147-769-WDD-501=E")) {
    const newTables = [...tables];
    newTables[4]!.cells.splice(20, 0, {
      content: "Nº DO DOCUMENTO",
      columnIndex: 3,
      rowIndex: 1,
    });

    newTables[4]!.cells = newTables[4]!.cells
      .filter((c) => c.rowIndex !== 0)
      .map((c) => ({ ...c, rowIndex: c.rowIndex - 1 }));

    return newTables;
  }

  const skipHeadersDocs: Record<string, number> = {
    "LD-5400.00-5606-940-PHN-004=0": 2,
    "LD-5400.00-5000-940-PHN-104=0": 2,
  };

  if (Object.keys(skipHeadersDocs).some((doc) => name.includes(doc))) {
    const key = Object.keys(skipHeadersDocs).find((doc) => name.includes(doc));
    if (!tables[4]) {
      console.warn(`[${key}] Could not find table 4 to fix extraction`);
      return tables;
    }

    const skip = skipHeadersDocs[key!]!;

    const newTables = [...tables];

    newTables[4]!.cells = newTables[4]!.cells
      .filter((c) => c.rowIndex > skip)
      .map((c) => ({ ...c, rowIndex: c.rowIndex - skip + 1 }));

    return newTables;
  }

  const revisionMergedFixes: Record<
    string,
    { revisionIndex: number; revisionContentIndex: 0 | 1; tableIndex: number[] }
  > = {
    "LD-5400.00-5604-814-FRB-001=B": {
      revisionContentIndex: 0,
      revisionIndex: 3,
      tableIndex: [4],
    },
    "LD-5400.00-0000-940-ORG-001=A": {
      revisionContentIndex: 1,
      revisionIndex: 16,
      tableIndex: [7],
    },
    "LD-5400.00-5604-831-FRB-001=H": {
      revisionContentIndex: 0,
      revisionIndex: 3,
      tableIndex: [6],
    },
    "LD-5400.00-5147-726-ABF-502=A": {
      revisionContentIndex: 0,
      revisionIndex: 4,
      tableIndex: [4],
    },
    "LD-5400.00-5604-852-FRB-001=D": {
      revisionContentIndex: 0,
      revisionIndex: 3,
      tableIndex: [5],
    },
    "LD-5400.00-0000-940-PHN-005=0": {
      revisionContentIndex: 0,
      revisionIndex: 2,
      tableIndex: [6],
    },
    "LD-5400.00-5131-831-FRB-002=F.PDF": {
      revisionContentIndex: 0,
      revisionIndex: 3,
      tableIndex: [5],
    },
    "LD-5400.00-5606-175-HEH-501=0_CONSOLIDADO": {
      revisionContentIndex: 0,
      revisionIndex: 4,
      tableIndex: [3, 5, 9],
    },
    "LD-5400.00-5131-732-RSQ-003=D": {
      revisionContentIndex: 0,
      revisionIndex: 9,
      tableIndex: [6],
    },
  };

  if (Object.keys(revisionMergedFixes).some((k) => name.includes(k))) {
    return fixMergedRevisionColumns({ name, tables, revisionMergedFixes });
  }

  return tables;
}
