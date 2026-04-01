const cheerio = require("cheerio");
const { launchChromium } = require("./browserLauncher");

const DEFAULT_WAIT_FOR = "#peers";

const normalizeText = (text) => (text || "").replace(/\s+/g, " ").trim();

const fetchHtmlRendered = async (url, options = {}) => {
  const waitFor = options.waitFor || DEFAULT_WAIT_FOR;
  const expandButtons = options.expandButtons !== false;
  const waitUntil = options.waitUntil || "networkidle";
  const externalBrowser = options.browser || null;

  const browser = externalBrowser || (await launchChromium());
  const page = await browser.newPage();
  page.on("pageerror", (err) => {
    const message = err?.message || String(err);
    if (message.includes("onError is not a function")) return;
    console.warn("Page error:", message);
  });
  await page.goto(url, { waitUntil });

  if (waitFor) {
    try {
      await page.waitForSelector(waitFor, { timeout: 15000 });
    } catch (err) {
      // ignore wait timeout
    }
  }

  if (expandButtons) {
    try {
      const buttons = await page.$$("button.button-plain");
      for (const btn of buttons) {
        try {
          await btn.click();
        } catch (err) {
          // ignore click failures
        }
      }
    } catch (err) {
      // ignore lookup failures
    }
  }

  const html = await page.content();
  await page.close().catch(() => {});
  if (!externalBrowser) {
    await browser.close();
  }
  return html;
};

const extractCompanyInfo = ($) => {
  const container = $(".company-info").first();
  if (!container.length) return {};

  let companyName = normalizeText(container.find("h1").first().text());
  if (!companyName) {
    companyName = normalizeText($("h1").first().text());
  }
  const about = normalizeText(
    container.find(".company-profile .about").first().text(),
  );
  const keyPoints = normalizeText(
    container.find(".company-profile .commentary").first().text(),
  );

  const links = [];
  container.find(".company-links a").each((_, el) => {
    const $el = $(el);
    const href = $el.attr("href");
    if (href) {
      links.push({
        title: normalizeText($el.text()),
        url: href,
      });
    }
  });

  const topRatios = [];
  container.find("#top-ratios li").each((_, el) => {
    const $el = $(el);
    const name = normalizeText($el.find(".name").first().text());
    const value = normalizeText($el.find(".value").first().text());
    if (name) topRatios.push({ name, value: value || null });
  });

  return {
    company_name: companyName || null,
    about: about || null,
    key_points: keyPoints || null,
    links,
    top_ratios: topRatios,
  };
};

const extractProsCons = ($section, $) => {
  const pros = [];
  const cons = [];

  $section.find(".pros ul li").each((_, el) => {
    pros.push(normalizeText($(el).text()));
  });
  $section.find(".cons ul li").each((_, el) => {
    cons.push(normalizeText($(el).text()));
  });

  return { pros, cons };
};

const extractTablesFromSection = ($section, $) => {
  const tables = [];

  $section.find("table").each((_, table) => {
    const $table = $(table);
    const tableClass = normalizeText($table.attr("class") || "");
    let headers = [];
    let title = null;
    let labelValueMode = false;

    const thead = $table.find("thead").first();
    if (thead.length) {
      headers = thead
        .find("th")
        .map((_, th) => normalizeText($(th).text()))
        .get();
      if (headers.length && headers[0] === "") headers[0] = "label";
    }

    const rows = [];
    const tbody = $table.find("tbody").first().length
      ? $table.find("tbody").first()
      : $table;
    const trList = tbody.find("tr").toArray();

    let idx = 0;
    while (idx < trList.length) {
      const tr = trList[idx];
      const $tr = $(tr);
      const ths = $tr.find("th");

      if (!headers.length && ths.length) {
        headers = ths.map((_, th) => normalizeText($(th).text())).get();
        if (headers.length && headers[0] === "") headers[0] = "label";
        idx += 1;
        continue;
      }

      const cells = $tr
        .find("th, td")
        .map((_, cell) => normalizeText($(cell).text()))
        .get();

      if (!cells.length) {
        idx += 1;
        continue;
      }

      if (idx === 0 && cells.length === 1) {
        title = cells[0];
        idx += 1;
        continue;
      }

      if (!headers.length) {
        if (cells.length === 2) {
          headers = ["label", "value"];
          labelValueMode = true;
        } else {
          headers = cells.map((_, i) => `col_${i + 1}`);
        }
      }

      if (headers.length && headers[0] === "") headers[0] = "label";

      // Handle 2-column key/value cards (e.g. "10 Years:" -> "21%").
      // Some cards define a single <th colspan="2"> title row, which leaves
      // headers as ["Compounded Sales Growth"] while data rows still have 2 cells.
      if (
        cells.length === 2 &&
        headers.length === 1 &&
        headers[0] &&
        headers[0] !== "label"
      ) {
        title = title || headers[0];
      }

      let row;
      if (
        cells.length === 2 &&
        (
          labelValueMode ||
          headers.join() === "label,value" ||
          (headers.length === 1 && headers[0] && headers[0] !== "label")
        )
      ) {
        row = { label: cells[0], value: cells[1] };
      } else {
        row = headers.reduce((acc, key, i) => {
          acc[key] = cells[i] !== undefined ? cells[i] : "";
          return acc;
        }, {});
      }

      const hasToggle =
        $tr.find("button").length > 0 || $tr.find("span.blue-icon").length > 0;

      if (hasToggle && cells.length) {
        row[headers[0]] = row[headers[0]]?.replace(/\+$/, "").trim();
      }

      if (hasToggle && idx + 1 < trList.length) {
        const children = [];
        let lookahead = idx + 1;
        while (lookahead < trList.length) {
          const cand = trList[lookahead];
          const $cand = $(cand);
          const candCells = $cand
            .find("th, td")
            .map((_, cell) => normalizeText($(cell).text()))
            .get();

          if (!candCells.length) {
            lookahead += 1;
            continue;
          }

          const candFirst = $cand.find("td.text").first();
          const candStyle = candFirst.attr("style") || "";
          const candClass = normalizeText($cand.attr("class") || "");
          const candHasToggle =
            $cand.find("button").length > 0 ||
            $cand.find("span.blue-icon").length > 0;
          const isIndented =
            candStyle.includes("padding-left") ||
            candClass.includes("indent") ||
            candClass.includes("sub") ||
            candClass.includes("child");

          if (candHasToggle || !isIndented) break;

          const childRow = headers.reduce((acc, key, i) => {
            acc[key] = candCells[i] !== undefined ? candCells[i] : "";
            return acc;
          }, {});
          children.push(childRow);
          lookahead += 1;
        }

        if (children.length) {
          row.children = children;
          idx = lookahead - 1;
        }
      }

      rows.push(row);
      idx += 1;
    }

    tables.push({ title, headers, rows, table_class: tableClass });
  });

  return tables;
};

const extractAnnouncements = ($section, $) => {
  const tabRoot = $section.find("#company-announcements-tab").first();
  if (!tabRoot.length) {
    return { title: "Announcements", items: [], all_link: null };
  }

  const allLink = tabRoot.find(".options a.button").attr("href") || null;

  const items = [];
  tabRoot.find("ul.list-links li").each((_, li) => {
    const $li = $(li);
    const a = $li.find("a").first();
    if (!a.length) return;
    const subtitle = normalizeText(a.find(".smaller").first().text());
    items.push({
      title: normalizeText(a.clone().children().remove().end().text()),
      subtitle: subtitle || null,
      url: a.attr("href") || null,
    });
  });

  return {
    title: "Announcements",
    items,
    all_link: allLink,
  };
};

const extractSimpleListCard = ($section, $, selector, title) => {
  const container = $section.find(selector).first();
  if (!container.length) return { title, items: [] };
  const items = [];
  container.find("ul.list-links li a").each((_, a) => {
    const $a = $(a);
    const subtitle = normalizeText($a.find(".smaller").first().text());
    items.push({
      title: normalizeText($a.clone().children().remove().end().text()),
      subtitle: subtitle || null,
      url: $a.attr("href") || null,
    });
  });
  return { title, items };
};

const extractConcalls = ($section, $) => {
  const container = $section.find(".documents.concalls").first();
  if (!container.length) return { title: "Concalls", items: [] };

  const items = [];
  container.find("ul.list-links > li").each((_, li) => {
    const $li = $(li);
    const label = normalizeText($li.find("div.nowrap").first().text());
    const links = [];
    let aiSummary = null;

    $li.find(".concall-link").each((__, el) => {
      const $el = $(el);
      const text = normalizeText($el.text());
      if ($el.is("a")) {
        const href = $el.attr("href");
        if (href) links.push({ title: text, url: href });
      } else if ($el.is("button") && $el.attr("data-url")) {
        aiSummary = {
          title: $el.attr("data-title") || text,
          url: $el.attr("data-url"),
        };
      }
    });

    items.push({
      label: label || null,
      links,
      ai_summary: aiSummary,
    });
  });

  return { title: "Concalls", items };
};

const pickMainTable = (tables) => {
  if (!tables || !tables.length) return null;
  return [...tables].sort(
    (a, b) => (b.headers?.length || 0) - (a.headers?.length || 0),
  )[0];
};

const pickOtherTables = (tables, mainTable) => {
  if (!tables || !tables.length) return [];
  return tables.filter((t) => t !== mainTable);
};

const extractSectionHtml = ($, selector) => {
  const node = $(selector).first();
  return node.length ? node : null;
};

const analyzeScreenerHtmlRendered = async (url, options = {}) => {
  const html = await fetchHtmlRendered(url, options);
  let $;
  try {
    $ = cheerio.load(html);
  } catch (err) {
    throw new Error(`Cheerio load failed: ${err?.message || err}`);
  }
  if (typeof $ !== "function") {
    throw new Error("Cheerio instance not initialized");
  }

  const analysisSection = extractSectionHtml($, "#analysis");
  const peersSection = extractSectionHtml($, "#peers");
  const quartersSection = extractSectionHtml($, "#quarters");
  const profitLossSection = extractSectionHtml($, "#profit-loss");
  const balanceSheetSection = extractSectionHtml($, "#balance-sheet");
  const cashFlowSection = extractSectionHtml($, "#cash-flow");
  const ratiosSection = extractSectionHtml($, "#ratios");
  const shareholdingSection = extractSectionHtml($, "#shareholding");
  const documentsSection = extractSectionHtml($, "#documents");

  const peersTables = peersSection ? extractTablesFromSection(peersSection, $) : [];
  const quartersTables = quartersSection ? extractTablesFromSection(quartersSection, $) : [];
  const profitLossTables = profitLossSection ? extractTablesFromSection(profitLossSection, $) : [];
  const balanceTables = balanceSheetSection ? extractTablesFromSection(balanceSheetSection, $) : [];
  const cashFlowTables = cashFlowSection ? extractTablesFromSection(cashFlowSection, $) : [];
  const ratiosTables = ratiosSection ? extractTablesFromSection(ratiosSection, $) : [];
  const shareholdingTables = shareholdingSection ? extractTablesFromSection(shareholdingSection, $) : [];

  const peersMain = pickMainTable(peersTables);
  const quartersMain = pickMainTable(quartersTables);
  const profitLossMain = pickMainTable(profitLossTables);
  const balanceMain = pickMainTable(balanceTables);
  const cashFlowMain = pickMainTable(cashFlowTables);
  const ratiosMain = pickMainTable(ratiosTables);
  const shareholdingMain = pickMainTable(shareholdingTables);

  const profitLossOther = pickOtherTables(profitLossTables, profitLossMain);

  return {
    company_info: extractCompanyInfo($),
    analysis: {
      pros_cons: analysisSection ? extractProsCons(analysisSection, $) : { pros: [], cons: [] },
    },
    peers: {
      main_table: peersMain || null,
    },
    quarters: {
      main_table: quartersMain || null,
    },
    profit_loss: {
      main_table: profitLossMain || null,
      other_details: profitLossOther,
    },
    balance_sheet: {
      main_table: balanceMain || null,
    },
    cash_flow: {
      main_table: cashFlowMain || null,
    },
    ratios: {
      main_table: ratiosMain || null,
    },
    shareholdings: {
      main_table: shareholdingMain || null,
    },
    documents: documentsSection
      ? {
          announcements: extractAnnouncements(documentsSection, $),
          annual_reports: extractSimpleListCard(
            documentsSection,
            $,
            ".documents.annual-reports",
            "Annual reports",
          ),
          credit_ratings: extractSimpleListCard(
            documentsSection,
            $,
            ".documents.credit-ratings",
            "Credit ratings",
          ),
          concalls: extractConcalls(documentsSection, $),
        }
      : {},
  };
};

module.exports = {
  analyzeScreenerHtmlRendered,
  fetchHtmlRendered,
};
