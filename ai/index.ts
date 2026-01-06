import { pdfToPng } from "pdf-to-png-converter";
import { createAzure } from "@ai-sdk/azure";
import { generateText } from "ai";

import { pino } from "pino";

const logger = pino({
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
    },
  },
});

const azure = createAzure({
  apiKey: process.env.AZURE_API_KEY!,
  resourceName: process.env.AZURE_RESOURCE_NAME!,
});

async function main() {
  const SYSTEM_PROMPT = await Bun.file("./ai/prompt.md").text();
  const inputList = await Bun.file("./ai/process.json").json();

  for (const inputFilename of inputList) {
    const inputFile = await Bun.file(`./assets/${inputFilename}`).arrayBuffer();
    const pngs = await pdfToPng(inputFile, {
      pagesToProcess: [1, 2, 3],
      useSystemFonts: true,
    });

    if (pngs.some((png) => !png.content)) {
      throw new Error(
        `[${inputFilename}] Missing content for one or more PNGs`,
      );
    }

    for (const png of pngs) {
      await Bun.write(
        `./ai/pngs/${inputFilename}-${png.pageNumber}.png`,
        png.content!,
      );
    }

    const { text } = await generateText({
      model: azure("gpt-5-chat"),
      messages: [
        { role: "system", content: SYSTEM_PROMPT.trim() },
        {
          role: "user",
          content: pngs.map((png) => ({ type: "image", image: png.content! })),
        },
      ],
    });

    const jsonResponse = JSON.parse(
      text.replace("```json", "").replace(/```/g, ""),
    );

    if (!Array.isArray(jsonResponse)) {
      throw new Error(`[${inputFilename}] Invalid JSON response - ${text}`);
    }

    await Bun.write(`./ai/responses/${inputFilename}.txt`, jsonResponse);
    logger.info(`Input filename to check: ${inputFilename}`);
    logger.info(`AI found columns: ${jsonResponse.join(", ")}`);

    const azureOutput = await Bun.file(
      `./assets/${inputFilename.replace(/\.pdf$/i, ".json")}`,
    ).json();

    const matchingCells: Array<[number, { content: string }[]]> =
      azureOutput.tables
        .map((table: { cells: { content: string }[] }, index: number) => {
          return [
            index,
            table.cells.filter((cell: { content: string }) => {
              return jsonResponse.some((aiHeader: string) =>
                cell.content.includes(aiHeader),
              );
            }),
          ];
        })
        .filter(
          ([, cell]: [number, { content: string }[]]) =>
            cell && cell.length > 0,
        );

    logger.info(
      `Found matching cells at ${matchingCells.map(([index, cells]) => `Table ${index + 1}, ${cells.map((cell) => `\`${cell.content}\``).join(", ")}`).join(", ")}`,
    );
    console.log();
  }
}

main();
