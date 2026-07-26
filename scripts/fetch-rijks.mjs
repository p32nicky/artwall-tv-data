// Fetch Rijksmuseum public-domain works (with images) via the keyless data API
// (data.rijksmuseum.nl) and APPEND them as new shards to the full/ catalog.
// Search enumerates object IDs; each ID is resolved with ?_profile=dc to get
// title/creator/date/medium + a IIIF image URL. Images are requested at 1920px.
//
// Env: RIJKS_LIMIT = max works to keep (0 = all ~700k). CONCURRENCY optional.

import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const FULL = "full";
const CHUNK_SIZE = 1000;
const LIMIT = Number(process.env.RIJKS_LIMIT || "0");
const CONCURRENCY = Number(process.env.CONCURRENCY || "12");
const SEARCH = "https://data.rijksmuseum.nl/search/collection?imageAvailable=true";

const clean = (s) => (s || "").replace(/\s+/g, " ").trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function getJSON(url, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { headers: { "User-Agent": "ArtWallTV/0.1", Accept: "application/json" } });
      if (r.status === 404) return null;
      if (r.status === 429) { await sleep(1500 * (i + 1)); continue; }
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    } catch (e) { if (i === tries - 1) return null; await sleep(400 * (i + 1)); }
  }
}
async function pool(items, limit, fn) {
  let idx = 0;
  await Promise.all(Array.from({ length: limit }, async () => {
    while (idx < items.length) { const my = idx++; await fn(items[my]); }
  }));
}

// pull a plain string out of Linked-Art/JSON-LD value shapes (prefer English)
function txt(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  if (Array.isArray(v)) { const en = v.find((x) => x && x["@language"] === "en"); return txt(en || v[0]); }
  if (v["@value"] != null) return String(v["@value"]);
  if (v.title != null) return txt(v.title);
  return "";
}
function findImage(rel) {
  const arr = Array.isArray(rel) ? rel : [rel];
  for (const r of arr) { const id = r?.["@id"] || r?.id; if (id && /micr\.io|iiif|\.jpe?g/i.test(id)) return id; }
  return null;
}
const sized = (u, s) => u.replace(/\/full\/[^/]+\//, `/full/${s}/`);

// ---- tagging (same rules as the rest of the pipeline) ----
const ARTIST_MOVEMENT = [[/monet|renoir|degas|pissarro|sisley|morisot/i,"Impressionism"],[/van gogh|gauguin|cezanne|cézanne|seurat|toulouse/i,"Post-Impressionism"],[/rembrandt|vermeer|hals|ruisdael|steen/i,"Dutch Golden Age"],[/da vinci|michelangelo|raphael|botticelli|titian/i,"Renaissance"],[/caravaggio|rubens|velazquez|velázquez|poussin/i,"Baroque"],[/hokusai|hiroshige|utamaro|kuniyoshi/i,"Ukiyo-e"],[/picasso|braque|gris/i,"Cubism"],[/kandinsky|mondrian|malevich/i,"Abstract"],[/turner|constable|friedrich/i,"Romanticism"],[/warhol|lichtenstein/i,"Pop Art"],[/dali|dalí|magritte|ernst|miro|miró/i,"Surrealism"],[/klimt|mucha/i,"Art Nouveau"]];
const KNOWN=["Impressionism","Post-Impressionism","Renaissance","Baroque","Rococo","Romanticism","Realism","Ukiyo-e","Cubism","Surrealism","Expressionism","Abstract","Pop Art","Art Nouveau","Art Deco","Neoclassicism","Dutch Golden Age","Symbolism","Fauvism","Minimalism","Modernism"];
function tagMovement({style,artist,medium}){const s=clean(style);if(s){const h=KNOWN.find(m=>new RegExp(m.replace(/-/g,".?"),"i").test(s));if(h)return h;}for(const[re,n]of ARTIST_MOVEMENT)if(re.test(artist||""))return n;if(/ukiyo|woodblock/i.test(medium||""))return "Ukiyo-e";return "Other";}
const SUBJECT=[[/portrait|self-portrait|bust|man|woman|lady|girl|boy|child/i,"Portrait"],[/landscape|mountain|valley|forest|field|countryside|garden/i,"Landscape"],[/still life|fruit|flower|floral|bouquet|vase/i,"Still Life"],[/sea|ocean|marine|ship|boat|harbor|coast|wave/i,"Seascape"],[/city|street|town|square|architecture|building|interior/i,"Cityscape"],[/animal|horse|dog|cat|bird|lion|tiger/i,"Animals"],[/religio|christ|madonna|saint|angel|biblical|mytholog/i,"Religious & Myth"],[/abstract|composition|geometric/i,"Abstract"]];
function tagSubject({title,classification,type,tags}){const hay=[title,classification,type,(tags||[]).join(" ")].join(" ");for(const[re,n]of SUBJECT)if(re.test(hay))return n;return "Other";}
function tagMood({title}){if(/night|dark|storm|shadow/i.test(title||""))return "Moody";if(/garden|spring|light|blue|sea/i.test(title||""))return "Calm";return "Neutral";}
function makeRecord(raw){return {id:raw.id,source:"Rijksmuseum",sourceUrl:raw.sourceUrl||null,title:clean(raw.title)||"Untitled",artist:clean(raw.artist)||"Unknown artist",year:clean(raw.year)||null,medium:clean(raw.medium)||null,image:raw.image,width:null,height:null,movement:tagMovement(raw),subject:tagSubject(raw),mood:tagMood(raw),license:raw.license};}

async function main() {
  console.log(`Rijks fetch — limit=${LIMIT || "ALL"} concurrency=${CONCURRENCY}`);
  const kept = [];
  let pageUrl = SEARCH, pages = 0;
  while (pageUrl && (LIMIT === 0 || kept.length < LIMIT)) {
    const page = await getJSON(pageUrl);
    if (!page?.orderedItems?.length) break;
    if (++pages === 1) console.log(`total matching: ${page.partOf?.totalItems}`);
    const ids = page.orderedItems.map((o) => o.id).filter(Boolean);
    await pool(ids, CONCURRENCY, async (objId) => {
      if (LIMIT > 0 && kept.length >= LIMIT) return;
      const j = await getJSON(`${objId}?_profile=dc`);
      if (!j) return;
      const rights = txt(j.rights?.["@id"] || j.rights);
      if (!/publicdomain|creativecommons\.org\/publicdomain/i.test(rights)) return; // PD/CC0 only
      const rawImg = findImage(j.relation);
      if (!rawImg) return;
      const ident = txt(j.identifier) || String(objId).split("/").pop();
      kept.push(makeRecord({
        id: `rijks_${ident}`, sourceUrl: j["@id"] || objId,
        title: txt(j.title), artist: txt(j.creator), year: txt(j.date),
        medium: txt(j.format), classification: txt(j.type), type: txt(j.type),
        image: { thumb: sized(rawImg, "400,"), display: sized(rawImg, "1920,"), full: sized(rawImg, "max") },
        license: /zero/i.test(rights) ? "CC0" : "Public Domain",
      }));
    });
    if (pages % 10 === 0) console.log(`  page ${pages}, kept ${kept.length}`);
    pageUrl = page.next?.id || null;
  }
  console.log(`Rijks kept ${kept.length} PD works with images`);
  if (!kept.length) { console.log("nothing to append"); return; }

  // dedupe within this batch by artist+title (keep generic-titled distinct)
  const seen = new Set();
  const uniq = kept.filter((r) => {
    const generic = r.artist === "Unknown artist" || r.title === "Untitled";
    const k = generic ? r.id : `${r.artist.toLowerCase()}::${r.title.toLowerCase()}`;
    return seen.has(k) ? false : (seen.add(k), true);
  });

  const manifest = JSON.parse(readFileSync(join(FULL, "manifest.json"), "utf8"));
  const existing = readdirSync(FULL).filter((f) => /^chunk-\d+\.json$/.test(f));
  let next = existing.reduce((m, f) => Math.max(m, parseInt(f.slice(6)) + 1), 0);
  const newChunks = [];
  for (let i = 0; i < uniq.length; i += CHUNK_SIZE) {
    const name = `chunk-${String(next++).padStart(3, "0")}.json`;
    writeFileSync(join(FULL, name), JSON.stringify({ artworks: uniq.slice(i, i + CHUNK_SIZE) }));
    newChunks.push(name);
  }
  manifest.chunks = [...manifest.chunks, ...newChunks];
  manifest.count = (manifest.count || 0) + uniq.length;
  manifest.sources = { ...(manifest.sources || {}), Rijksmuseum: ((manifest.sources || {}).Rijksmuseum || 0) + uniq.length };
  manifest.generatedAt = new Date().toISOString();
  writeFileSync(join(FULL, "manifest.json"), JSON.stringify(manifest, null, 2));
  console.log(`Appended ${newChunks.length} chunks (${uniq.length} unique). New total: ${manifest.count} works, ${manifest.chunks.length} chunks.`);
}
main().catch((e) => (console.error("FATAL", e), process.exit(1)));
