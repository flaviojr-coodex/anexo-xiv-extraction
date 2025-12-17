async function main() {
  const raw = await Bun.file("./assets/ANEXO XIV.pdf.json").json();
  const result = raw.analyzeResult;

  const parsed = {
    ...omit(result, ["pages", "styles", "paragraphs", "sections"]),
    tables: result.tables?.map((table: any) => ({
      ...omit(table, ["spans"]),
      boundingRegions: table.boundingRegions?.map((region: any) =>
        omit(region, ["polygon"]),
      ),
      cells: table.cells?.map((cell: any) =>
        omit(cell, ["boundingRegions", "spans", "elements"]),
      ),
    })),
  };
  await Bun.write("./assets/ANEXO XIV.json", JSON.stringify(parsed, null, 2));
}

main();

function omit<T extends Record<string, any>, K extends keyof T>(
  obj: T,
  keys: K[],
): Omit<T, K> {
  return Object.fromEntries(
    Object.entries(obj).filter(([key]) => !keys.includes(key as K)),
  ) as Omit<T, K>;
}
