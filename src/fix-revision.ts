import type { AzureCell, AzureTable } from "./handle-tables";

type RevisionMergedFix = {
  tableIndex: number[];
  revisionIndex: number;
  revisionContentIndex: 0 | 1;
};

function splitRevisionAndRest(
  raw: string | null | undefined,
  revisionContentIndex: 0 | 1,
): { revision: string; rest: string } | null {
  const content = (raw ?? "").trim();
  if (!content) return null;

  if (!/\s/.test(content)) return null;

  if (revisionContentIndex === 0) {
    const m = content.match(/^(\S+)\s+([\s\S]+)$/);
    if (!m) return null;
    return { revision: m[1]!, rest: m[2]?.trim()! };
  }

  const m = content.match(/^([\s\S]+?)\s+(\S+)$/);
  if (!m) return null;
  return { revision: m[2]!, rest: m[1]?.trim()! };
}

export function fixMergedRevisionColumns(params: {
  name: string;
  tables: AzureTable[];
  revisionMergedFixes: Record<string, RevisionMergedFix>;
}): AzureTable[] {
  const { name, tables, revisionMergedFixes } = params;

  const key =
    Object.keys(revisionMergedFixes)
      .filter((k) => name.includes(k))
      .sort((a, b) => b.length - a.length)[0] ?? null;

  if (!key) return tables;

  const fix = revisionMergedFixes[key];
  if (!fix) return tables;

  const { revisionIndex, revisionContentIndex, tableIndex: tableIndices } = fix;

  let newTables = [...tables];

  for (const tIndex of tableIndices) {
    const table = newTables[tIndex];
    if (!table) {
      console.warn(`[${key}] Could not find table ${tIndex} to fix extraction`);
      continue;
    }

    if (
      !Array.isArray(table.cells) ||
      revisionIndex < 0 ||
      revisionIndex >= table.cells.length
    ) {
      console.warn(
        `[${key}] Invalid revisionIndex (${revisionIndex}) for table ${tIndex}`,
      );
      continue;
    }

    const workingTable: AzureTable = {
      ...table,
      cells: table.cells.map((c) => ({ ...c })),
    };

    const oldColumnCount = workingTable.columnCount;

    const revisionCellProbe = workingTable.cells[revisionIndex];
    if (!revisionCellProbe) {
      newTables[tIndex] = workingTable;
      continue;
    }

    const revisionCol = revisionCellProbe.columnIndex;
    const insertCol = revisionCol + 1;

    const rows = new Map<number, AzureCell[]>();
    for (const cell of workingTable.cells) {
      const list = rows.get(cell.rowIndex) ?? [];
      list.push(cell);
      rows.set(cell.rowIndex, list);
    }

    const rowIndices = Array.from(rows.keys()).sort((a, b) => a - b);

    for (const r of rowIndices) {
      const rowCells = rows.get(r)!;

      const revCell = rowCells.find((c) => c.columnIndex === revisionCol);

      let restContent = "";
      if (revCell?.content) {
        const split = splitRevisionAndRest(
          revCell.content,
          revisionContentIndex,
        );
        if (split) {
          revCell.content = split.revision;
          restContent = split.rest;
        }
      }

      for (const c of rowCells) {
        if (c.columnIndex > revisionCol) c.columnIndex += 1;
      }

      rowCells.push({
        rowIndex: r,
        columnIndex: insertCol,
        content: restContent,
      });

      const newColumnCount = oldColumnCount + 1;

      const byCol = new Map<number, AzureCell>();
      for (const c of rowCells) {
        const existing = byCol.get(c.columnIndex);
        if (!existing) {
          byCol.set(c.columnIndex, c);
        } else {
          const existingText = (existing.content ?? "").toString().trim();
          const newText = (c.content ?? "").toString().trim();
          if (!existingText && newText) byCol.set(c.columnIndex, c);
        }
      }

      for (let col = 0; col < newColumnCount; col++) {
        if (!byCol.has(col)) {
          byCol.set(col, {
            rowIndex: r,
            columnIndex: col,
            content: "",
          });
        }
      }

      rowCells.length = 0;
      rowCells.push(...Array.from(byCol.values()));
      rowCells.sort((a, b) => a.columnIndex - b.columnIndex);
    }

    workingTable.columnCount = oldColumnCount + 1;

    const rebuilt: AzureCell[] = [];
    for (const r of rowIndices) {
      rebuilt.push(...rows.get(r)!);
    }

    workingTable.cells = rebuilt;

    newTables[tIndex] = workingTable;
  }

  return newTables;
}

type DocumentMergedFix = {
  tableIndex: number[];
  documentColumnIndex: number;
};

function splitDocumentAndDescription(raw: string): { document: string; description: string } | null {
  const content = (raw ?? "").trim();
  if (!content || !/\s/.test(content)) return null;
  const m = content.match(/^([A-Z0-9\-\.]+)\s+([\s\S]+)$/i);
  if (!m) return null;
  return { document: m[1]!, description: m[2]?.trim()! };
}

export function fixMergedDocumentColumns(params: {
  name: string;
  tables: AzureTable[];
  documentMergedFixes: Record<string, DocumentMergedFix>;
}): AzureTable[] {
  const { name, tables, documentMergedFixes } = params;

  const key =
    Object.keys(documentMergedFixes)
      .filter((k) => name.includes(k))
      .sort((a, b) => b.length - a.length)[0] ?? null;

  if (!key) return tables;

  const fix = documentMergedFixes[key];
  if (!fix) return tables;

  const { documentColumnIndex, tableIndex: tableIndices } = fix;

  let newTables = [...tables];

  for (const tIndex of tableIndices) {
    const table = newTables[tIndex];
    if (!table) {
      console.warn(`[${key}] Could not find table ${tIndex} to fix extraction`);
      continue;
    }

    const workingTable: AzureTable = {
      ...table,
      cells: table.cells.map((c) => ({ ...c })),
    };

    const oldColumnCount = workingTable.columnCount;
    const insertCol = documentColumnIndex + 1;

    const rows = new Map<number, AzureCell[]>();
    for (const cell of workingTable.cells) {
      const list = rows.get(cell.rowIndex) ?? [];
      list.push(cell);
      rows.set(cell.rowIndex, list);
    }

    const rowIndices = Array.from(rows.keys()).sort((a, b) => a - b);

    for (const r of rowIndices) {
      const rowCells = rows.get(r)!;

      const docCell = rowCells.find((c) => c.columnIndex === documentColumnIndex);

      let descriptionContent = "";
      if (docCell?.content) {
        const split = splitDocumentAndDescription(docCell.content);
        if (split) {
          docCell.content = split.document;
          descriptionContent = split.description;
        }
      }

      for (const c of rowCells) {
        if (c.columnIndex > documentColumnIndex) c.columnIndex += 1;
      }

      rowCells.push({
        rowIndex: r,
        columnIndex: insertCol,
        content: descriptionContent,
      });

      const newColumnCount = oldColumnCount + 1;

      const byCol = new Map<number, AzureCell>();
      for (const c of rowCells) {
        const existing = byCol.get(c.columnIndex);
        if (!existing) {
          byCol.set(c.columnIndex, c);
        } else {
          const existingText = (existing.content ?? "").toString().trim();
          const newText = (c.content ?? "").toString().trim();
          if (!existingText && newText) byCol.set(c.columnIndex, c);
        }
      }

      for (let col = 0; col < newColumnCount; col++) {
        if (!byCol.has(col)) {
          byCol.set(col, {
            rowIndex: r,
            columnIndex: col,
            content: "",
          });
        }
      }

      rowCells.length = 0;
      rowCells.push(...Array.from(byCol.values()));
      rowCells.sort((a, b) => a.columnIndex - b.columnIndex);
    }

    workingTable.columnCount = oldColumnCount + 1;

    const rebuilt: AzureCell[] = [];
    for (const r of rowIndices) {
      rebuilt.push(...rows.get(r)!);
    }

    workingTable.cells = rebuilt;

    newTables[tIndex] = workingTable;
  }

  return newTables;
}
