import type {
  AnalyzeResult,
  AnalyzedDocument,
} from "@azure/ai-form-recognizer";

type RawAzureTable = Required<
  AnalyzeResult<AnalyzedDocument>
>["tables"][number];

type AzureCell = {
  kind?: string;
  rowIndex: number;
  columnIndex: number;
  rowSpan?: number;
  columnSpan?: number;
  content?: string;
};

export type AzureTable = {
  rowCount: number;
  columnCount: number;
  cells: AzureCell[];
  boundingRegions: RawAzureTable["boundingRegions"];
};

type ColumnFixOptions = {
  desiredColumns: number;
  skipColumns: number;
  joinUntil: number;
};

type GridCell = {
  content: string;
  kind?: AzureCell["kind"];
};

type ConvertOptions = {
  skipRows?: number;
  columnFix?: ColumnFixOptions;
};

export function extractAndParseTable(table: AzureTable) {
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

  return {
    csv: strCsv,
    html: tableToHTML(table, { skipRows: rowsOffset, columnFix }),
  };
}

function tableToCSV(table: AzureTable, options?: ConvertOptions): string[] {
  const skipRows = options?.skipRows ?? 0;

  let grid = buildFlatGrid(table, skipRows);

  if (options?.columnFix) {
    grid = normalizeColumns(grid, options.columnFix);
  }

  return grid
    .map((row) =>
      row
        .map(({ content }) => {
          const cell = content
            .replaceAll(":unselected:", "")
            .replaceAll(":selected:", "")
            .replace(/"/g, '""')
            .trim();

          if (cell === "") return "";
          return `"${cell}"`;
        })
        .join(",")
        .trim(),
    )
    .filter((row) => !!row.trim());
}

function tableToHTML(table: AzureTable, options?: ConvertOptions): string {
  const skipRows = options?.skipRows ?? 0;

  let grid = buildFlatGrid(table, skipRows);

  if (options?.columnFix) {
    grid = normalizeColumns(grid, options.columnFix);
  }

  let html = `<table style="width:100%;border-collapse: collapse;border: 1px solid black;" border="1">\n`;

  for (const row of grid) {
    html += "  <tr>\n";

    for (const cell of row) {
      let tag = "td";
      let scope = "";

      if (cell.kind === "columnHeader") {
        tag = "th";
        scope = ' scope="col"';
      } else if (cell.kind === "rowHeader") {
        tag = "th";
        scope = ' scope="row"';
      }

      html += `    <${tag}${scope} style="padding: 5px;">${escapeHtml(cell.content)}</${tag}>\n`;
    }

    html += "  </tr>\n";
  }

  html += "</table>";
  return html.replaceAll(":unselected:", "").replaceAll(":selected:", "");
}

function normalizeColumns(
  grid: GridCell[][],
  options: ColumnFixOptions,
): GridCell[][] {
  if (!grid.length) return grid;

  if (grid[0]!.length === options.desiredColumns) {
    return grid;
  }

  return grid.map((row) => {
    const columns = row.filter((c) => c.content.trim());

    const left = columns.slice(0, options.skipColumns);

    const middleCells = columns.slice(options.skipColumns, options.joinUntil);
    const middleKind = middleCells.find((c) => c.kind)?.kind;
    const middleContent = getUniqueValues(
      middleCells.map((c) => c.content.trim()),
    ).join(" ");

    const right = columns.slice(options.joinUntil);

    return [...left, { content: middleContent, kind: middleKind }, ...right];
  });
}

function getUniqueValues<T>(array: T[]): T[] {
  return Array.from(new Set(array));
}

function buildFlatGrid(table: AzureTable, skipRows: number): GridCell[][] {
  const rows = table.rowCount - skipRows;
  const cols = table.columnCount;

  const grid: GridCell[][] = Array.from({ length: rows }, () =>
    Array.from({ length: cols }, () => ({ content: "", kind: undefined })),
  );

  for (const cell of table.cells) {
    const rs = cell.rowSpan ?? 1;
    const cs = cell.columnSpan ?? 1;

    for (let dr = 0; dr < rs; dr++) {
      for (let dc = 0; dc < cs; dc++) {
        const r = cell.rowIndex + dr - skipRows;
        const c = cell.columnIndex + dc;

        if (r < 0 || r >= rows) continue;
        if (c < 0 || c >= cols) continue;

        grid[r]![c] = {
          content: cell.content ?? "",
          kind: cell.kind,
        };
      }
    }
  }

  return grid;
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
