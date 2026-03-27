function buildResult({ provider, id, thumbUrl, fullUrl, author, sourceUrl }) {
  return {
    provider,
    id: String(id || ""),
    thumbUrl: String(thumbUrl || ""),
    fullUrl: String(fullUrl || thumbUrl || ""),
    author: String(author || ""),
    sourceUrl: String(sourceUrl || ""),
  };
}

async function searchUnsplash(query, perPage) {
  const key = process.env.UNSPLASH_ACCESS_KEY || "";
  if (!key) return [];
  const url = `https://api.unsplash.com/search/photos?query=${encodeURIComponent(query)}&per_page=${perPage}&orientation=landscape`;
  const res = await fetch(url, {
    headers: { Authorization: `Client-ID ${key}` },
  });
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data?.results)
    ? data.results.map((item) =>
        buildResult({
          provider: "unsplash",
          id: item?.id,
          thumbUrl: item?.urls?.small,
          fullUrl: item?.urls?.regular,
          author: item?.user?.name,
          sourceUrl: item?.links?.html,
        }),
      )
    : [];
}

async function searchPexels(query, perPage) {
  const key = process.env.PEXELS_API_KEY || "";
  if (!key) return [];
  const url = `https://api.pexels.com/v1/search?query=${encodeURIComponent(query)}&per_page=${perPage}`;
  const res = await fetch(url, {
    headers: { Authorization: key },
  });
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data?.photos)
    ? data.photos.map((item) =>
        buildResult({
          provider: "pexels",
          id: item?.id,
          thumbUrl: item?.src?.medium,
          fullUrl: item?.src?.large || item?.src?.original,
          author: item?.photographer,
          sourceUrl: item?.url,
        }),
      )
    : [];
}

async function searchPixabay(query, perPage) {
  const key = process.env.PIXABAY_API_KEY || "";
  if (!key) return [];
  const url = `https://pixabay.com/api/?key=${encodeURIComponent(key)}&q=${encodeURIComponent(query)}&per_page=${perPage}&image_type=photo&safesearch=true`;
  const res = await fetch(url);
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data?.hits)
    ? data.hits.map((item) =>
        buildResult({
          provider: "pixabay",
          id: item?.id,
          thumbUrl: item?.webformatURL,
          fullUrl: item?.largeImageURL || item?.webformatURL,
          author: item?.user,
          sourceUrl: item?.pageURL,
        }),
      )
    : [];
}

async function searchImages({ query, provider = "all", perPage = 12 }) {
  const q = String(query || "").trim();
  if (!q) return [];
  const limit = Math.max(1, Math.min(Number(perPage) || 12, 30));
  const target = String(provider || "all").toLowerCase();
  const tasks = [];
  if (target === "all" || target === "unsplash") tasks.push(searchUnsplash(q, limit));
  if (target === "all" || target === "pexels") tasks.push(searchPexels(q, limit));
  if (target === "all" || target === "pixabay") tasks.push(searchPixabay(q, limit));
  const results = await Promise.all(tasks);
  return results.flat();
}

module.exports = {
  searchImages,
};
