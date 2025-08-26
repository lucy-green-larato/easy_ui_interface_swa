// Extracts text from uploaded PDFs (assumes user OCR’d when needed).
const pdfParse = require("pdf-parse");

const MAX_TEXT_PER_FILE = 14000;

async function parseOne(file) {
  try {
    const data = await pdfParse(file.buffer);
    const raw = (data.text || "").replace(/\u0000/g, "").trim();
    const scanned = raw.length < 1000; // very low text → likely image-only
    return {
      filename: file.filename,
      pages: data.numpages || null,
      scannedLikely: scanned,
      text: (raw || "").slice(0, MAX_TEXT_PER_FILE)
    };
  } catch (e) {
    return { filename: file.filename, error: String(e?.message || e) };
  }
}

async function buildPdfPack(files = []) {
  const pdfs = files.filter(f => /pdf/i.test(f.contentType || f.mimetype || ""));
  const out = [];
  for (const f of pdfs.slice(0, 2)) out.push(await parseOne(f));
  return out;
}

module.exports = { buildPdfPack };
