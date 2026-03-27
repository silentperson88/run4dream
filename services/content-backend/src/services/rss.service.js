const RSS_SOURCES = {
  toi: "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
  ht: "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
};

function decodeCdata(value) {
  const raw = String(value || "");
  const cdata = raw.match(/<!\[CDATA\[([\s\S]*?)\]\]>/i);
  return (cdata ? cdata[1] : raw).trim();
}

function stripTags(value) {
  return String(value || "").replace(/<[^>]*>/g, "").trim();
}

function extractTag(block, tag) {
  const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = block.match(regex);
  if (!match) return "";
  return stripTags(decodeCdata(match[1]));
}

function parseRss(xml) {
  const items = [];
  const parts = String(xml || "").split(/<item\b[^>]*>/i).slice(1);
  parts.forEach((part) => {
    const block = part.split(/<\/item>/i)[0] || part;
    const title = extractTag(block, "title");
    const link = extractTag(block, "link");
    const pubDate = extractTag(block, "pubDate");
    if (title && link) {
      items.push({ title, link, pubDate });
    }
  });
  return items;
}

async function fetchRss(sourceKey) {
  const url = RSS_SOURCES[sourceKey];
  if (!url) throw new Error("Invalid RSS source");
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch RSS (${res.status})`);
  const xml = await res.text();
  return parseRss(xml);
}

module.exports = {
  fetchRss,
};
