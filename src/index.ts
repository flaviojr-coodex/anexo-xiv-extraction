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

const ignoredLDs = new Set<string>([]);

const gedMissingLDs = new Set<string>();
const processedLDs = new Set<string>();

async function main() {
  logger.info("Starting ANEXO XIV extraction process");
  const rowsPerPage = await handleAnexoXIVCached();
  logger.debug("ANEXO XIV cached, proceeding to handle LDs recursively");
  await handleLDsRecursive("ANEXO XIV", rowsPerPage);

  logger.error(
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
  logger.debug(`Found ${assets.length} files in assets directory`);

  for (const filename of assets) {
    if (!isLDPdf(filename)) continue;
    if (ignoredLDs.has(filename)) continue;
    if (processedLDs.has(filename)) continue;
    processedLDs.add(filename);

    logger.debug(`Processing LD file: ${filename}`);
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
    logger.debug(`[${source}] No rows found to process`);
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
    logger.debug(`[${source}] All LD documents already exist in assets`);
    return;
  }

  logger.info(
    `[${source}] Found ${missingLDs.length} missing LD documents. Searching MongoDB...`,
  );

  const documentsCollection = mongoose.connection.db!.collection<{
    blobPath: string;
  }>("documents");

  logger.debug(
    `[${source}] Querying MongoDB for documents matching: ${missingLDs.map((row) => row.documentName).join("|")}`,
  );

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
          { blobPath: { $regex: "^EPC-5", $options: "i" } },
        ],
      },
      { projection: { _id: 1, blobPath: 1 } },
    )
    .toArray();

  logger.debug(
    `[${source}] MongoDB query returned ${documents?.length || 0} documents`,
  );

  if (!documents || documents.length === 0) {
    logger.warn(
      `[${source}] No documents found in MongoDB for ${missingLDs.length} missing LDs`,
    );
    missingLDs.forEach((ld) => gedMissingLDs.add(ld.documentName));
    return;
  }

  for (const row of missingLDs) {
    const document = documents.find((doc) =>
      doc.blobPath.includes(row.documentName),
    );

    if (!document) {
      logger.debug(
        `[${source}] Document not found in MongoDB for ${row.documentName}`,
      );
      gedMissingLDs.add(row.documentName);
      continue;
    }

    const filename = document.blobPath.split("/").pop()!;
    logger.info(`[${source}] Downloading ${document.blobPath} to assets`);
    logger.debug(`[${source}] Downloading to ./assets/${filename}`);

    const file = await s3.file(document.blobPath).arrayBuffer();
    await Bun.write(`./assets/${filename}`, file);

    logger.debug(
      `[${source}] Successfully downloaded ${filename} (${file.byteLength} bytes)`,
    );
  }

  logger.info(
    `[${source}] Download complete. Processed ${missingLDs.length - gedMissingLDs.size} documents`,
  );
}

function isLDPdf(filename: string) {
  return filename.startsWith("LD-") && /\.(pdf)$/i.test(filename);
}

mongoose
  .connect(process.env.MONGO_URI!, { dbName: "heftos-ged" })
  .then(main)
  .catch(console.error)
  .finally(mongoose.disconnect);
