const fs = require("fs");
const path = require("path");

/**
 * Recursively reads all JSON files from a folder and merges them into a single array
 * @param {string} folderPath - Path to the folder containing JSON files
 * @param {Array<string>} desiredKeys - Array of keys to extract from each object (optional)
 * @returns {Promise<Array>} - Array containing all merged JSON data
 */
async function mergeJsonFiles(
  folderPath: string,
  desiredKeys: Array<string> | null = null,
) {
  let mergedData: any[] = [];

  function filterObject<T extends Record<string, any>, K extends keyof T>(
    obj: T,
    keys: Array<K>,
  ): Omit<T, K> {
    if (!keys) return obj;
    return keys.reduce((filtered, key) => {
      if (key in obj) {
        filtered[key] = obj[key];
      }
      return filtered;
    }, {} as T);
  }

  async function traverse(currentPath: string) {
    try {
      const files = await fs.promises.readdir(currentPath);

      for (const file of files) {
        const filePath = path.join(currentPath, file);
        const stat = await fs.promises.stat(filePath);

        if (stat.isDirectory()) {
          // Recursively process subdirectories
          await traverse(filePath);
        } else if (path.extname(file) === ".json") {
          // Read and parse JSON file
          try {
            const fileContent = await fs.promises.readFile(filePath, "utf-8");
            const jsonData = JSON.parse(fileContent);

            // Get filename without extension for source tracking
            const sourceFileName = path.dirname(filePath).split("/").pop();

            // Filter by desired keys if specified
            if (desiredKeys) {
              if (Array.isArray(jsonData)) {
                const filteredArray = jsonData.map((item) => {
                  const filtered =
                    typeof item === "object"
                      ? filterObject(item, desiredKeys)
                      : item;
                  if (typeof filtered === "object") {
                    filtered.source = sourceFileName;
                  }
                  return filtered;
                });
                mergedData = mergedData.concat(filteredArray);
              } else if (typeof jsonData === "object") {
                const filtered = filterObject(jsonData, desiredKeys);
                filtered.source = sourceFileName;
                mergedData.push(filtered);
              }
            } else {
              // No filtering, merge all data
              if (Array.isArray(jsonData)) {
                const arrayWithSource = jsonData.map((item) => {
                  if (typeof item === "object") {
                    item.source = sourceFileName;
                  }
                  return item;
                });
                mergedData = mergedData.concat(arrayWithSource);
              } else if (typeof jsonData === "object") {
                jsonData.source = sourceFileName;
                mergedData.push(jsonData);
              }
            }

            console.log(`✓ Merged: ${filePath}`);
          } catch (error) {
            console.error(`✗ Error parsing ${filePath}:`, error.message);
          }
        }
      }
    } catch (error) {
      console.error(`✗ Error reading directory ${currentPath}:`, error.message);
    }
  }

  await traverse(folderPath);
  return mergedData;
}

// Usage example
(async () => {
  try {
    // Option 1: Extract only specific keys
    const desiredKeys = ["documentName", "revision"]; // Specify the keys you want
    const result = await mergeJsonFiles("./tables", desiredKeys);

    await fs.promises.writeFile(
      "merged_output.json",
      JSON.stringify(result, null, 2),
    );
  } catch (error) {
    console.error("Error:", error.message);
  }
})();
