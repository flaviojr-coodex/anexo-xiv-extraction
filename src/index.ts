import { readdir } from "fs/promises";
import mongoose from "mongoose";

import { handleAnexoXIVCached } from "./anexo-xiv";
import { handleLDCached } from "./lds";
import { csv2json } from "./csv2json";

const s3 = new Bun.S3Client({ region: "us-east-1", bucket: "heftos-ged" });

mongoose
  .connect(process.env.MONGO_URI!, { dbName: "heftos-ged" })
  .then(main)
  .catch(console.error)
  .finally(mongoose.disconnect);

async function main() {
  const rowsPerPage = await handleAnexoXIVCached();
  await handleDownloadMentionedLDs(rowsPerPage);

  const assets = await readdir("./assets");
  for (const filename of assets) {
    if (
      !filename.startsWith("LD-") ||
      !filename.endsWith(".pdf") ||
      !filename.endsWith(".PDF")
    )
      continue;

    const result = await handleLDCached(`./assets/${filename}`);
    if (!result) {
      console.error(`No tables found in ${filename}`);
      continue;
    }
  }
}

async function handleDownloadMentionedLDs(
  rowsPerPage: Awaited<ReturnType<typeof handleAnexoXIVCached>>,
) {
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
