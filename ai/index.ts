import { pdfToPng } from "pdf-to-png-converter";
import { createAzure } from "@ai-sdk/azure";
import { generateText } from "ai";

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
      model: azure("gpt-5-mini"),
      messages: [
        { role: "system", content: SYSTEM_PROMPT.trim() },
        {
          role: "user",
          content: pngs.map((png) => ({ type: "image", image: png.content! })),
        },
      ],
    });

    await Bun.write(`./ai/responses/${inputFilename}.txt`, text);
    console.log(inputFilename);
    console.log(text);
  }
}

main();
