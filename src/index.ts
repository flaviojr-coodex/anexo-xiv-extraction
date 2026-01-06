import { readdir } from "fs/promises";
import mongoose from "mongoose";

import { handleAnexoXIVCached } from "./anexo-xiv";
import { handleLDCached } from "./lds";
import { pino } from "pino";

const logger = pino({
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
    },
  },
});

const s3 = new Bun.S3Client({ region: "us-east-1", bucket: "heftos-ged" });

const ignoredLDs = new Set<string>([
  "LD-5400.00-4710-746-RLM-002=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-4710-970-FE3-001=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-5412-940-VWI-001=A.PDF", // CANCELADO - NO TABLES
  "LD-5400.00-4710-312-VCE-300=A.PDF", // CANCELADO - NO TABLES
  "LD-5400.00-4710-713-EHJ-001=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-4710-713-EHJ-003=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-4710-713-EHJ-009=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-4700-712-BIZ-501=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-4710-812-YAS-304=A.pdf", // CANCELADO - NO TABLES
  "LD-5400.00-4710-392-IKW-300=A.pdf", // Weird table, only refers to itself
  "LD-5400.00-1231-940-PPC-301=K.pdf", // Weird table, only refers to itself
  "LD-5400.00-4700-737-WD1-501=A.pdf", // Weird table, only refers to itself
  "LD-5400.00-4710-814-MHG-002=0.PDF", // Weird table, only refers to itself
  "LD-5400.00-4710-700-T1A-301=A.pdf", // Weird table, no LDs mentioned
  "LD-5400.00-4710-947-SQQ-002=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-947-SQQ-302=0.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-800-EYQ-001=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-800-EYQ-301=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-746-RLM-050=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-746-RLM-302=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-229-HJI-001=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-855-MBV-311=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-947-SQQ-004=A.pdf", // Bad format - Only refers to itself
  "LD-5400.00-4710-800-FE3-005=0_COMENTADO.pdf", // Bad format - Only refers to itself
]);

const gedMissingLDs = new Set<string>();
const processedLDs = new Set<string>();

async function main() {
  // logger.info("Starting ANEXO XIV extraction process");
  const rowsPerPage = await handleAnexoXIVCached();
  // logger.debug("ANEXO XIV cached, proceeding to handle LDs recursively");
  await handleLDsRecursive("ANEXO XIV", rowsPerPage);

  logger.error(
    `No documents found on DB for ${gedMissingLDs.size} missing LDs`,
  );

  console.dir(Array.from(gedMissingLDs), { depth: null });
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
  if (Object.values(rowsPerPage).flat().length === 0) {
    // logger.debug(`[${source}] No rows found to process`);
    return;
  }

  // for (const page in rowsPerPage) {
  //   for (const row of rowsPerPage[page]!) {
  //     if (!row?.documentName) {
  //       logger.error(`[${source}] Invalid document name in row`);
  //       logger.debug(JSON.stringify(row, null, 2));
  //       logger.debug(JSON.stringify(rowsPerPage[Number(page)]?.[1], null, 2));
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

  if (!missingLDs || missingLDs.length === 0) {
    // logger.debug(`[${source}] All LD documents already exist in assets`);
    return;
  }

  // START MANUAL
  // missingLDs.push({
  //   documentName: "LD-5400.00-4700-737-WD1-501",
  //   revision: "A",
  // });
  // END - MANUAL

  // logger.info(
  //   `[${source}] Found ${missingLDs.length} missing LD documents. Searching MongoDB...`,
  // );

  const documentsCollection = mongoose.connection.db!.collection<{
    blobPath: string;
  }>("documents");

  // logger.debug(
  //   `[${source}] Querying MongoDB for documents matching: ${missingLDs.map((row) => row.documentName).join("|")}`,
  // );

  const documents = await documentsCollection
    .find(
      {
        $and: [
          {
            blobPath: {
              $regex: missingLDs
                .map((row) => row.documentName.trim())
                .join("|"),
              $options: "i",
            },
          },
          { blobPath: { $regex: "\\.pdf$", $options: "i" } },
          { blobPath: { $regex: "^EPC-5", $options: "i" } },
        ],
      },
      { projection: { _id: 1, blobPath: 1 } },
    )
    .toArray();

  // logger.debug(
  //   `[${source}] MongoDB query returned ${documents?.length || 0} documents`,
  // );

  if (!documents || documents.length === 0) {
    // logger.warn(
    //   `[${source}] No documents found in MongoDB for ${missingLDs.length} missing LDs`,
    // );
    missingLDs.forEach((ld) => gedMissingLDs.add(ld.documentName));
    return;
  }

  for (const row of missingLDs) {
    const document = documents.find((doc) =>
      doc.blobPath.includes(row.documentName),
    );

    if (!document) {
      // logger.debug(
      //   `[${source}] Document not found in MongoDB for ${row.documentName}`,
      // );
      gedMissingLDs.add(row.documentName);
      continue;
    }

    const filename = document.blobPath.split("/").pop()!;
    // logger.info(`[${source}] Downloading ${document.blobPath} to assets`);
    // logger.debug(`[${source}] Downloading to ./assets/${filename}`);

    const file = await s3.file(document.blobPath).arrayBuffer();
    await Bun.write(`./assets/${filename}`, file);

    // logger.debug(
    //   `[${source}] Successfully downloaded ${filename} (${file.byteLength} bytes)`,
    // );
  }

  // logger.info(
  //   `[${source}] Download complete. Processed ${missingLDs.length - gedMissingLDs.size} documents`,
  // );
}

function isLDPdf(filename: string) {
  return filename.startsWith("LD-") && /\.(pdf)$/i.test(filename);
}

mongoose
  .connect(process.env.MONGO_URI!, { dbName: "heftos-ged" })
  .then(main)
  .catch(console.error)
  .finally(mongoose.disconnect);
