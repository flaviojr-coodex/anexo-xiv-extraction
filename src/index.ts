import { readdir } from "fs/promises";
import mongoose from "mongoose";

import { handleAnexoXIVCached } from "./anexo-xiv";
import { handleLDCached } from "./lds";

const s3 = new Bun.S3Client({ region: "us-east-1", bucket: "heftos-ged" });

const ignoredLDs = new Set<string>([
  // Single empty page
  "LD-5400.00-5606-700-HJK-502=0.PDF",
  "LD-5400.00-5136-700-HJK-001=0.PDF",
  "LD-5400.00-5136-700-HJK-002=0.PDF",
  "LD-5400.00-5136-700-HJK-003=0.PDF",
  "LD-5400.00-5135-700-HJK-002=0.PDF",
  "LD-5400.00-5135-700-HJK-004=0.PDF",
  "LD-5400.00-6825-700-HJK-002=0.PDF",
  "LD-5400.00-6825-313-VCE-001=C.PDF", // CANCELADO
  "LD-5400.00-5606-642-LDW-001=C_CONSOLIDADO.pdf",
  "LD-5400.00-5950-811-EJL-001=A_CONSOLIDADO.PDF",
  // Unstructured table in document, no need to reprocess since it only refers to itself
  "LD-5400.00-6825-833-KSV-001=0_CONSOLIDADO.PDF",
]);

const gedMissingLDs = new Set<string>();
const processedLDs = new Set<string>();

async function main() {
  const rowsPerPage = await handleAnexoXIVCached();
  await handleLDsRecursive("ANEXO XIV", rowsPerPage);

  console.error(
    `No documents found on DB for ${gedMissingLDs.size} missing LDs: ${Array.from(gedMissingLDs).join(", ")}`,
  );
}

async function handleLDsRecursive(
  source: string,
  rowsPerPage: NonNullable<
    Awaited<ReturnType<typeof handleAnexoXIVCached | typeof handleLDCached>>
  >,
) {
  await handleDownloadMentionedLDs(source, rowsPerPage);

  const assets = await readdir("./assets");
  for (const filename of assets) {
    if (!isLDPdf(filename)) continue;
    if (ignoredLDs.has(filename)) continue;
    if (processedLDs.has(filename)) continue;
    processedLDs.add(filename);

    const result = await handleLDCached(`./assets/${filename}`);
    if (!result) continue;

    await handleLDsRecursive(filename, result);
  }
}

async function handleDownloadMentionedLDs(
  source: string,
  rowsPerPage: NonNullable<
    Awaited<ReturnType<typeof handleAnexoXIVCached | typeof handleLDCached>>
  >,
) {
  if (Object.values(rowsPerPage).flat().length === 0) return;

  // for (const page in rowsPerPage) {
  //   for (const row of rowsPerPage[page]!) {
  //     if (!row?.documentName) {
  //       console.error(`[${source}] Invalid document name in row`);
  //       console.log(JSON.stringify(row, null, 2));
  //       console.log(JSON.stringify(rowsPerPage[Number(page)]?.[1], null, 2));
  //       return;
  //     }
  //   }
  // }

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

  const documentsCollection = mongoose.connection.db!.collection<{
    blobPath: string;
  }>("documents");

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
    missingLDs.forEach((ld) => gedMissingLDs.add(ld.documentName));
    return;
  }

  for (const row of missingLDs) {
    const document = documents.find((doc) =>
      doc.blobPath.includes(row.documentName),
    );

    if (!document) {
      gedMissingLDs.add(row.documentName);
      continue;
    }

    console.log(`[${source}] Downloading ${document.blobPath} to assets`);
    const file = await s3.file(document.blobPath).arrayBuffer();
    await Bun.write(`./assets/${document.blobPath.split("/").pop()}`, file);
  }
}

function isLDPdf(filename: string) {
  return filename.startsWith("LD-") && /\.(pdf)$/i.test(filename);
}

mongoose
  .connect(process.env.MONGO_URI!, { dbName: "heftos-ged" })
  .then(main)
  .catch(console.error)
  .finally(mongoose.disconnect);
