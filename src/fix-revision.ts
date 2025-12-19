import type { AzureCell, AzureTable } from "./handle-tables";

type RevisionMergedFix = {
  tableIndex: number;
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

  const { revisionIndex, revisionContentIndex, tableIndex } = fix;

  const table = tables[tableIndex];
  if (!table) {
    console.warn(
      `[${key}] Could not find table ${tableIndex} to fix extraction`,
    );
    return tables;
  }

  if (
    !Array.isArray(table.cells) ||
    revisionIndex < 0 ||
    revisionIndex >= table.cells.length
  ) {
    console.warn(
      `[${key}] Invalid revisionIndex (${revisionIndex}) for table ${tableIndex}`,
    );
    return tables;
  }

  const newTables = tables.map((t, idx) => {
    if (idx !== tableIndex) return t;
    return {
      ...t,
      cells: t.cells.map((c) => ({ ...c })),
    };
  });

  const newTable = newTables[tableIndex]!;
  const oldColumnCount = newTable.columnCount;

  const revisionCellProbe = newTable.cells[revisionIndex];
  if (!revisionCellProbe) return newTables;

  const revisionCol = revisionCellProbe.columnIndex;
  const insertCol = revisionCol + 1;

  const rows = new Map<number, AzureCell[]>();
  for (const cell of newTable.cells) {
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
      const split = splitRevisionAndRest(revCell.content, revisionContentIndex);
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
        rowCells.push({
          rowIndex: r,
          columnIndex: col,
          content: "",
        });
      }
    }

    const finalByCol = new Map<number, AzureCell>();
    for (const c of rowCells) {
      const existing = finalByCol.get(c.columnIndex);
      if (!existing) finalByCol.set(c.columnIndex, c);
      else {
        const existingText = (existing.content ?? "").toString().trim();
        const newText = (c.content ?? "").toString().trim();
        if (!existingText && newText) finalByCol.set(c.columnIndex, c);
      }
    }

    rowCells.length = 0;
    rowCells.push(...Array.from(finalByCol.values()));

    rowCells.sort((a, b) => a.columnIndex - b.columnIndex);
  }

  newTable.columnCount = oldColumnCount + 1;

  const rebuilt: AzureCell[] = [];
  for (const r of rowIndices) {
    const rowCells = rows.get(r)!;
    rowCells.sort((a, b) => a.columnIndex - b.columnIndex);
    rebuilt.push(...rowCells);
  }
  newTable.cells = rebuilt;

  return newTables;
}
