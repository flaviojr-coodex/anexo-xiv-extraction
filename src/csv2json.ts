function parseCsvRow(row: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < row.length; i++) {
    const char = row[i];
    const next = row[i + 1];

    if (char === '"' && inQuotes && next === '"') {
      // Escaped quote
      current += '"';
      i++;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  result.push(current);
  return result;
}

function unquote(value: string | undefined): string | undefined {
  if (value == null) return value;
  return value.startsWith('"') && value.endsWith('"')
    ? value.slice(1, -1).replace(/""/g, '"')
    : value;
}

export function csv2json<U extends string, T extends Record<U, string>>(
  csv: string[] | string,
  columnNameMap: Record<string, U>,
): Array<T> {
  if (typeof csv === "string") csv = csv.split(/\r?\n/);
  if (!csv || csv.length <= 1) return [];

  const headers = parseCsvRow(csv[0]!).map(unquote) as string[];
  const rows = csv.slice(1).filter(Boolean).map(parseCsvRow);

  return rows.map((row) =>
    headers.reduce<Record<U, string | undefined>>(
      (acc, header, i) => {
        const key: U = columnNameMap[header] || (header as U);
        acc[key] = unquote(row[i]);
        return acc;
      },
      {} as Record<U, string | undefined>,
    ),
  ) as Array<T>;
}
