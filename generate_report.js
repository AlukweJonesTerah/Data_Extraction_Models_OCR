const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  HeadingLevel, AlignmentType, BorderStyle, WidthType, ShadingType,
  PageNumber, PageBreak, Header, Footer
} = require('docx');

const dataPath = process.argv[2];
const outputPath = process.argv[3];
const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));

const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };
const headerBorder = { style: BorderStyle.SINGLE, size: 1, color: "1F4E79" };
const headerBorders = { top: headerBorder, bottom: headerBorder, left: headerBorder, right: headerBorder };

function cell(text, isHeader = false, width = 2340, align = AlignmentType.LEFT) {
  return new TableCell({
    borders: isHeader ? headerBorders : borders,
    width: { size: width, type: WidthType.DXA },
    shading: isHeader
      ? { fill: "1F4E79", type: ShadingType.CLEAR }
      : { fill: "FFFFFF", type: ShadingType.CLEAR },
    margins: { top: 80, bottom: 80, left: 120, right: 120 },
    children: [new Paragraph({
      alignment: align,
      children: [new TextRun({
        text: String(text),
        bold: isHeader,
        color: isHeader ? "FFFFFF" : "000000",
        size: isHeader ? 20 : 18,
        font: "Arial"
      })]
    })]
  });
}

function statusCell(text) {
  const isSuccess = text === "SUCCESS";
  const isFailed = text === "FAILED";
  const fill = isSuccess ? "E2EFDA" : isFailed ? "FCE4D6" : "FFF2CC";
  const color = isSuccess ? "375623" : isFailed ? "9C0006" : "7D6608";
  return new TableCell({
    borders,
    width: { size: 1560, type: WidthType.DXA },
    shading: { fill, type: ShadingType.CLEAR },
    margins: { top: 80, bottom: 80, left: 120, right: 120 },
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({ text, bold: true, color, size: 18, font: "Arial" })]
    })]
  });
}

function spacer() {
  return new Paragraph({ children: [new TextRun({ text: "", size: 20 })] });
}

function heading1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    children: [new TextRun({ text, font: "Arial", size: 32, bold: true, color: "1F4E79" })],
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "1F4E79", space: 1 } },
    spacing: { before: 360, after: 200 }
  });
}

function heading2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    children: [new TextRun({ text, font: "Arial", size: 26, bold: true, color: "2E75B6" })],
    spacing: { before: 280, after: 120 }
  });
}

function kv(label, value) {
  return new Paragraph({
    spacing: { after: 80 },
    children: [
      new TextRun({ text: label + ": ", bold: true, size: 20, font: "Arial", color: "1F4E79" }),
      new TextRun({ text: String(value), size: 20, font: "Arial" })
    ]
  });
}

// ── GPU Section ──────────────────────────────────────────────────────────────
const gpu = data.gpu;
const gpuRows = [
  ["GPU Name", gpu.name],
  ["Driver Version", gpu.driver_version],
  ["CUDA Version", gpu.cuda_version],
  ["Total VRAM", gpu.total_vram],
  ["Available VRAM", gpu.available_vram],
  ["Temperature at Start", gpu.temperature],
  ["Power Limit", gpu.power_limit],
  ["Compute Capability", gpu.compute],
  ["PCI Bus ID", gpu.pci_id],
].map(([k, v]) => new TableRow({
  children: [
    cell(k, false, 3120),
    cell(v, false, 6240)
  ]
}));

const gpuTable = new Table({
  width: { size: 9360, type: WidthType.DXA },
  columnWidths: [3120, 6240],
  rows: [
    new TableRow({ children: [cell("Specification", true, 3120), cell("Value", true, 6240)] }),
    ...gpuRows
  ]
});

// ── Summary Section ───────────────────────────────────────────────────────────
const s = data.summary;
const successRate = s.processed > 0 ? ((s.success / s.processed) * 100).toFixed(1) : "0.0";
const avgTime = s.processed > 0 ? (s.total_seconds / s.processed).toFixed(2) : "0.00";

const summaryTable = new Table({
  width: { size: 9360, type: WidthType.DXA },
  columnWidths: [3120, 6240],
  rows: [
    new TableRow({ children: [cell("Metric", true, 3120), cell("Value", true, 6240)] }),
    new TableRow({ children: [cell("Total Files Found", false, 3120), cell(s.total, false, 6240)] }),
    new TableRow({ children: [cell("Total Processed", false, 3120), cell(s.processed, false, 6240)] }),
    new TableRow({ children: [cell("Successful Extractions", false, 3120), cell(s.success, false, 6240)] }),
    new TableRow({ children: [cell("Failed / Manual Review", false, 3120), cell(s.failed, false, 6240)] }),
    new TableRow({ children: [cell("Success Rate", false, 3120), cell(successRate + "%", false, 6240)] }),
    new TableRow({ children: [cell("Total Processing Time", false, 3120), cell(s.total_time, false, 6240)] }),
    new TableRow({ children: [cell("Avg Time Per Document", false, 3120), cell(avgTime + " seconds", false, 6240)] }),
    new TableRow({ children: [cell("Run Date / Time", false, 3120), cell(data.run_datetime, false, 6240)] }),
  ]
});

// ── Per-File Table ────────────────────────────────────────────────────────────
const fileRows = data.files.map((f, i) => new TableRow({
  children: [
    cell(String(i + 1), false, 480, AlignmentType.CENTER),
    cell(f.filename, false, 3600),
    cell(f.extracted_value, false, 2160),
    cell(f.model_used, false, 1560),
    statusCell(f.status),
    cell(f.duration + "s", false, 960, AlignmentType.CENTER),
  ]
}));

const fileTable = new Table({
  width: { size: 9360, type: WidthType.DXA },
  columnWidths: [480, 3600, 2160, 1560, 1560, 960],
  rows: [
    new TableRow({
      children: [
        cell("#", true, 480, AlignmentType.CENTER),
        cell("Filename", true, 3600),
        cell("Extracted Value", true, 2160),
        cell("Model Used", true, 1560),
        cell("Status", true, 1560, AlignmentType.CENTER),
        cell("Time (s)", true, 960, AlignmentType.CENTER),
      ]
    }),
    ...fileRows
  ]
});

// ── Document Assembly ─────────────────────────────────────────────────────────
const doc = new Document({
  styles: {
    default: { document: { run: { font: "Arial", size: 20 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, font: "Arial", color: "1F4E79" },
        paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, font: "Arial", color: "2E75B6" },
        paragraph: { spacing: { before: 280, after: 120 }, outlineLevel: 1 } },
    ]
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1080, right: 1080, bottom: 1080, left: 1080 }
      }
    },
    headers: {
      default: new Header({
        children: [new Paragraph({
          border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "1F4E79", space: 1 } },
          children: [
            new TextRun({ text: "NSSF OCR Extraction Report  |  ", bold: true, font: "Arial", size: 18, color: "1F4E79" }),
            new TextRun({ text: data.run_datetime, font: "Arial", size: 18, color: "555555" })
          ]
        })]
      })
    },
    footers: {
      default: new Footer({
        children: [new Paragraph({
          border: { top: { style: BorderStyle.SINGLE, size: 6, color: "1F4E79", space: 1 } },
          alignment: AlignmentType.CENTER,
          children: [
            new TextRun({ text: "Page ", font: "Arial", size: 16, color: "555555" }),
            new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 16, color: "555555" }),
            new TextRun({ text: " of ", font: "Arial", size: 16, color: "555555" }),
            new TextRun({ children: [PageNumber.TOTAL_PAGES], font: "Arial", size: 16, color: "555555" }),
          ]
        })]
      })
    },
    children: [
      // Title
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 80 },
        children: [new TextRun({ text: "NSSF OCR Extraction Report", bold: true, size: 52, font: "Arial", color: "1F4E79" })]
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 400 },
        children: [new TextRun({ text: "Automated 3-Tier Vision Model Pipeline", size: 24, font: "Arial", color: "555555" })]
      }),

      heading1("1. GPU Hardware Specifications"),
      new Paragraph({
        spacing: { after: 160 },
        children: [new TextRun({ text: `RunPod Instance  |  ${gpu.name}  |  CUDA ${gpu.cuda_version}`, size: 20, font: "Arial", color: "555555" })]
      }),
      gpuTable,
      spacer(),

      heading1("2. Extraction Summary"),
      summaryTable,
      spacer(),

      heading1("3. Per-Document Processing Log"),
      new Paragraph({
        spacing: { after: 160 },
        children: [new TextRun({ text: `${data.files.length} documents processed. Models: ${data.models.join(" → ")}`, size: 20, font: "Arial", color: "555555" })]
      }),
      fileTable,
    ]
  }]
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(outputPath, buf);
  console.log("Report written to: " + outputPath);
});