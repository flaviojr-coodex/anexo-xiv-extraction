import { readdir } from "fs/promises";
import mongoose from "mongoose";

import { handleAnexoXIVCached } from "./anexo-xiv";
import { handleLDCached } from "./lds";
import { csv2json } from "./csv2json";

const s3 = new Bun.S3Client({ region: "us-east-1", bucket: "heftos-ged" });
const processedLDs = new Set<string>();

mongoose
  .connect(process.env.MONGO_URI!, { dbName: "heftos-ged" })
  .then(main)
  .catch(console.error)
  .finally(mongoose.disconnect);

async function main() {
  const rowsPerPage = await handleAnexoXIVCached();
  await handleLDsRecursive(rowsPerPage);
}

async function handleLDsRecursive(
  rowsPerPage: NonNullable<
    Awaited<ReturnType<typeof handleAnexoXIVCached | typeof handleLDCached>>
  >,
) {
  await handleDownloadMentionedLDs(rowsPerPage);

  const assets = await readdir("./assets");
  for (const filename of assets) {
    if (!isLDPdf(filename)) continue;
    if (processedLDs.has(filename)) continue;
    processedLDs.add(filename);

    const result = await handleLDCached(`./assets/${filename}`);
    if (!result) {
      console.error(`No tables found in ${filename}`);
      continue;
    }

    await handleLDsRecursive(result);
  }
}

async function handleDownloadMentionedLDs(
  rowsPerPage: NonNullable<
    Awaited<ReturnType<typeof handleAnexoXIVCached | typeof handleLDCached>>
  >,
) {
  if (Object.values(rowsPerPage).flat().length === 0) return;

  const documentsCollection = mongoose.connection.db!.collection<{
    blobPath: string;
  }>("documents");

  const lds = Object.fromEntries(
    Object.entries(rowsPerPage).map(([page, rows]) => [
      page,
      rows.filter((item) => item.documentName.startsWith("LD-")),
    ]),
  );

  const assets = await readdir("./assets");
  const existingLDs = assets.filter((asset) => asset.startsWith("LD-"));
  const missingLDs = Object.values(lds)
    .flat()
    .filter((row) => !existingLDs.some((ld) => ld.includes(row.documentName)));

  if (!missingLDs || missingLDs.length === 0) return;

  const documents = await documentsCollection
    .find(
      {
        $and: [
          {
            blobPath: {
              $regex: missingLDs.map((row) => row.documentName).join("|"),
              $options: "i",
            },
          },
          { blobPath: { $regex: "\\.pdf$", $options: "i" } },
        ],
      },
      { projection: { _id: 1, blobPath: 1 } },
    )
    .toArray();

  if (!documents || documents.length === 0) {
    console.error(`No documents found with ${missingLDs.length} missing LDs`);
    console.error(missingLDs.map((ld) => ld.documentName).join(", "));
    return;
  }

  for (const row of missingLDs) {
    const document = documents.find((doc) =>
      doc.blobPath.includes(row.documentName),
    );

    if (!document) {
      console.error(`No document found for ${row.documentName}`);
      continue;
    }

    console.log(`Downloading ${document.blobPath} to assets`);
    const file = await s3.file(document.blobPath).arrayBuffer();
    await Bun.write(`./assets/${document.blobPath.split("/").pop()}`, file);
  }
}

function isLDPdf(filename: string) {
  return filename.startsWith("LD-") && /\.(pdf)$/i.test(filename);
}
