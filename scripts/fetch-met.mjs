// Fetch The Met's public-domain works (CC0, with images) and APPEND them as new
// shards to the existing full/ catalog. Runs on GitHub Actions (the Met API is
// reachable there). Normalizes to the same record shape; updates full/manifest.json.
//
// Env: MET_LIMIT = max works to keep (0 = all). CONCURRENCY optional.

import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const FULL = "full";
const CHUNK_SIZE = 1000;
const LIMIT = Number(process.env.MET_LIMIT || "0");        // 0 = all
const CONCURRENCY = Number(process.env.CONCURRENCY || "24");
const API = "https://collectionapi.metmuseum.org/public/v1";

const clean = (s) => (s || "").replace(/\s+/g, " ").trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function shuffle(a){for(let i=a.length-1;i>0;i--){const j=(Math.random()*(i+1))|0;[a[i],a[j]]=[a[j],a[i]];}return a;}

async function getJSON(url, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(url, { headers: { "User-Agent": "ArtWallTV/0.1" } });
      if (r.status === 404) return null;
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    } catch (e) { if (i === tries - 1) return null; await sleep(300 * (i + 1)); }
  }
}
async function pool(items, limit, fn) {
  let idx = 0;
  await Promise.all(Array.from({ length: limit }, async () => {
    while (idx < items.length) { const my = idx++; await fn(items[my], my); }
  }));
}

// ---- tagging (same rules as the rest of the pipeline) ----
const ARTIST_MOVEMENT = [
  [/monet|renoir|degas|pissarro|sisley|morisot/i,"Impressionism"],[/van gogh|gauguin|cezanne|cézanne|seurat|toulouse/i,"Post-Impressionism"],
  [/rembrandt|vermeer|hals|ruisdael|steen/i,"Dutch Golden Age"],[/da vinci|michelangelo|raphael|botticelli|titian/i,"Renaissance"],
  [/caravaggio|rubens|velazquez|velázquez|poussin/i,"Baroque"],[/hokusai|hiroshige|utamaro|kuniyoshi/i,"Ukiyo-e"],
  [/picasso|braque|gris/i,"Cubism"],[/kandinsky|mondrian|malevich/i,"Abstract"],[/turner|constable|friedrich/i,"Romanticism"],
  [/warhol|lichtenstein/i,"Pop Art"],[/dali|dalí|magritte|ernst|miro|miró/i,"Surrealism"],[/klimt|mucha/i,"Art Nouveau"],
];
const KNOWN=["Impressionism","Post-Impressionism","Renaissance","Baroque","Rococo","Romanticism","Realism","Ukiyo-e","Cubism","Surrealism","Expressionism","Abstract","Pop Art","Art Nouveau","Art Deco","Neoclassicism","Dutch Golden Age","Symbolism","Fauvism","Minimalism","Modernism"];
function tagMovement({style,artist,medium}){const s=clean(style);if(s){const h=KNOWN.find(m=>new RegExp(m.replace(/-/g,".?"),"i").test(s));if(h)return h;}for(const[re,n]of ARTIST_MOVEMENT)if(re.test(artist||""))return n;if(/ukiyo|woodblock/i.test(medium||""))return "Ukiyo-e";return "Other";}
const SUBJECT=[[/portrait|self-portrait|bust|man|woman|lady|girl|boy|child/i,"Portrait"],[/landscape|mountain|valley|forest|field|countryside|garden/i,"Landscape"],[/still life|fruit|flower|floral|bouquet|vase/i,"Still Life"],[/sea|ocean|marine|ship|boat|harbor|coast|wave/i,"Seascape"],[/city|street|town|square|architecture|building|interior/i,"Cityscape"],[/animal|horse|dog|cat|bird|lion|tiger/i,"Animals"],[/religio|christ|madonna|saint|angel|biblical|mytholog/i,"Religious & Myth"],[/abstract|composition|geometric/i,"Abstract"]];
function tagSubject({title,classification,type,tags}){const hay=[title,classification,type,(tags||[]).join(" ")].join(" ");for(const[re,n]of SUBJECT)if(re.test(hay))return n;return "Other";}
function tagMood({title}){if(/night|dark|storm|shadow/i.test(title||""))return "Moody";if(/garden|spring|light|blue|sea/i.test(title||""))return "Calm";return "Neutral";}

function makeRecord(raw){
  return {
    id:raw.id, source:"The Metropolitan Museum of Art", sourceUrl:raw.sourceUrl||null,
    title:clean(raw.title)||"Untitled", artist:clean(raw.artist)||"Unknown artist",
    year:clean(raw.year)||null, medium:clean(raw.medium)||null,
    image:raw.image, width:null, height:null,
    movement:tagMovement(raw), subject:tagSubject(raw), mood:tagMood(raw), license:"Public Domain",
  };
}

async function main(){
  console.log(`Met fetch — limit=${LIMIT||"ALL"} concurrency=${CONCURRENCY}`);
  const list = await getJSON(`${API}/objects`);
  if(!list?.objectIDs?.length){ console.error("Could not list Met objects (API unreachable?)"); process.exit(1); }
  let ids = list.objectIDs;
  console.log(`Met total objects: ${ids.length}`);
  ids = shuffle(ids.slice());                    // randomize so a limited run is representative
  if(LIMIT>0) ids = ids.slice(0, LIMIT*6);       // oversample; most objects aren't PD-with-image

  const kept=[]; let seen=0;
  await pool(ids, CONCURRENCY, async (id)=>{
    if(LIMIT>0 && kept.length>=LIMIT) return;
    if((++seen)%5000===0) console.log(`  scanned ${seen}, kept ${kept.length}`);
    const a = await getJSON(`${API}/objects/${id}`);
    if(!a || !a.isPublicDomain || !a.primaryImage) return;
    if(LIMIT>0 && kept.length>=LIMIT) return;
    kept.push(makeRecord({
      id:`met_${a.objectID}`, sourceUrl:a.objectURL||null, title:a.title, artist:a.artistDisplayName,
      year:a.objectDate, medium:a.medium, style:null, classification:a.classification, type:a.objectName,
      tags:(a.tags||[]).map(t=>t.term),
      image:{ thumb:a.primaryImageSmall||a.primaryImage, display:a.primaryImage, full:a.primaryImage },
    }));
  });
  console.log(`Met kept ${kept.length} PD works with images`);
  if(!kept.length){ console.log("nothing to append"); return; }

  // append as new chunks, continuing the existing numbering
  const manifest = JSON.parse(readFileSync(join(FULL,"manifest.json"),"utf8"));
  const existing = readdirSync(FULL).filter(f=>/^chunk-\d+\.json$/.test(f));
  let next = existing.reduce((m,f)=>Math.max(m, parseInt(f.slice(6))+1), 0);
  const newChunks=[];
  for(let i=0;i<kept.length;i+=CHUNK_SIZE){
    const name=`chunk-${String(next++).padStart(3,"0")}.json`;
    writeFileSync(join(FULL,name), JSON.stringify({artworks:kept.slice(i,i+CHUNK_SIZE)}));
    newChunks.push(name);
  }
  manifest.chunks = [...manifest.chunks, ...newChunks];
  manifest.count = (manifest.count||0) + kept.length;
  manifest.sources = { ...(manifest.sources||{}), "The Metropolitan Museum of Art": ((manifest.sources||{})["The Metropolitan Museum of Art"]||0) + kept.length };
  manifest.generatedAt = new Date().toISOString();
  writeFileSync(join(FULL,"manifest.json"), JSON.stringify(manifest,null,2));
  console.log(`Appended ${newChunks.length} chunks. New total: ${manifest.count} works, ${manifest.chunks.length} chunks.`);
}
main().catch(e=>(console.error("FATAL",e),process.exit(1)));
