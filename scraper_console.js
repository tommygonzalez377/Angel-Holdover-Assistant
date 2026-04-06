(async function flashGrossScraper() {

  const LOCATIONS = [
    { unit: "1762", theatre: "Broward Stm 12 & RPX" },
    { unit: "0684", theatre: "Gulf Coast Stm 16 & IMAX" },
    { unit: "0339", theatre: "Magnolia Place Stm 16" },
    { unit: "1613", theatre: "Pavilion Stm 14 & RPX" },
    { unit: "0723", theatre: "Town Ctr Stm 16 Port Charlotte" },
    { unit: "0709", theatre: "Westfork Stm 13" },
  ];
  const WEEK_DATE  = "2026-03-07";
  const ATTRACTION = "Solo Mio";

  const DIST_ABBREV = {
    "angel studios":"ANGEL","sony":"SNY","sony/crunchyroll":"SNY",
    "paramount":"PAR","20th century studios":"20TH","disney":"DIS",
    "warner bros.":"WB","warner bros":"WB","lionsgate":"LION",
    "a24":"A24","neon":"NEON","neon rated":"NEON",
    "universal":"UNI","universal pictures":"UNI","briarcliff":"BCLF",
    "fathom entertainment":"FTHM","trafalgar releasing":"TRAFR",
    "focus features":"FOC","amazon mgm studios":"AMZMGM",
    "amazon mgm":"AMZMGM","amazon":"AMZMGM",
    "vertical entertainment":"VERT","vertical":"VERT",
    "gkids":"GKIDS","well go usa entertainment":"WLGO","well go usa":"WLGO",
    "cmc pictures":"CMC","sony pictures classics":"SPC",
    "independent films":"IndeFilms","seismic pictures":"SEISMIC",
  };
  function abbrevDist(name) {
    const key = (name||"").toLowerCase().trim();
    return DIST_ABBREV[key] || (name.split(" ")[0]||"?").slice(0,6).toUpperCase();
  }
  function parseCSV(text) {
    const lines = text.replace(/\r/g,"").split("\n");
    const headers = lines[0].split(",").map(h=>h.trim().replace(/^"|"$/g,""));
    const rows = [];
    for (let i=1;i<lines.length;i++) {
      const vals = lines[i].split(",").map(v=>v.trim().replace(/^"|"$/g,""));
      if (vals.filter(Boolean).length<2) continue;
      const obj={};
      headers.forEach((h,j)=>obj[h]=vals[j]||"");
      rows.push(obj);
    }
    return rows;
  }

  // ── Step 1: Master list ───────────────────────────────────────────────────
  const ML_URL = "https://docs.google.com/spreadsheets/d/14VKWNE_oCsjPJ2my_EKAO8A9HdW2L4ZA/export?format=csv&gid=845899698";
  console.log("Fetching master list...");
  const mlText = await (await fetch(ML_URL)).text();
  const lookup = {};
  for (const row of parseCSV(mlText)) {
    const refId   = (row["Exhibitor's Ref ID"]||"").trim();
    const rentrak = (row["Venue Rentrak ID"]||"").trim();
    const venue   = (row["Venue"]||"").trim();
    if (refId && rentrak) lookup[refId] = {rentrak_id:rentrak, venue};
  }
  console.log("  " + Object.keys(lookup).length + " theatres loaded");

  // ── Step 2: Scrape each location ──────────────────────────────────────────
  const BASE = "https://beta.boxofficeessentials.com/reports/flash/theater_films_by_rank";
  const results = [];

  for (const loc of LOCATIONS) {
    const ml = lookup[loc.unit];
    if (!ml) {
      console.warn("Unit " + loc.unit + " (" + loc.theatre + ") not in master list");
      results.push({...loc, status:"no_rentrak_id", films:[], error:"Unit "+loc.unit+" not in master list"});
      continue;
    }
    const url = BASE + "?theater_no=" + ml.rentrak_id + "&pct_change_same_theater_or_total_gross=same_theater&day_range_rev=" + WEEK_DATE;
    console.log(loc.theatre + " (Rentrak " + ml.rentrak_id + ")");
    try {
      const doc = new DOMParser().parseFromString(await (await fetch(url,{credentials:"include"})).text(),"text/html");
      const table = doc.querySelector("main table, table");
      const films = [];
      if (table) {
        const hEls = Array.from(table.querySelectorAll("thead th,thead td"));
        const hdr = hEls.map(h=>{const c=h.cloneNode(true);c.querySelectorAll("button,a").forEach(n=>n.remove());return c.textContent.trim().toLowerCase();});
        const idx = kw => hdr.findIndex(h=>h.includes(kw));
        const tI=idx("title"), dI=idx("distributor"), wgI=idx("week gross"), weI=idx("weekend gross"), cuI=idx("cume");
        let rank=1;
        table.querySelectorAll("tbody tr").forEach(tr=>{
          const cells=Array.from(tr.querySelectorAll("td")).map(td=>{const a=td.querySelector("a");return a?a.textContent.trim():td.textContent.trim();});
          if(!cells.some(c=>c)) return;
          const title=tI>=0&&cells[tI]?cells[tI]:"Row "+rank;
          const dist=dI>=0&&cells[dI]?cells[dI]:"";
          const pg=i=>(i>=0&&cells[i])?parseInt(cells[i].replace(/[$,]/g,""))||0:0;
          films.push({rank,title,dist:abbrevDist(dist),gross:pg(wgI)||pg(weI)||0,cume:pg(cuI),is_angel:title.toLowerCase().includes(ATTRACTION.toLowerCase())});
          rank++;
        });
      }
      const validEl=doc.querySelector("h2,h3,h4");
      results.push({unit:loc.unit,theatre:loc.theatre,ml_venue:ml.venue,attraction:ATTRACTION,status:films.length?"ok":"no_data",films,valid_as_of:validEl?validEl.textContent.trim():"Week of "+WEEK_DATE,url});
      console.log("  " + films.length + " films");
    } catch(e) {
      console.error("  Error: " + e.message);
      results.push({unit:loc.unit,theatre:loc.theatre,ml_venue:ml.venue,status:"error",films:[],error:e.message,url:""});
    }
    await new Promise(r=>setTimeout(r,400));
  }

  // ── Step 3: Build dashboard ───────────────────────────────────────────────
  const now     = new Date().toLocaleDateString("en-US",{month:"long",day:"numeric",year:"numeric"});
  const weekFmt = new Date(WEEK_DATE+"T12:00:00").toLocaleDateString("en-US",{month:"2-digit",day:"2-digit",year:"2-digit"});

  const css = `
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f5f5f5;color:#222;padding:24px}
    h2{font-size:1.35rem;margin-bottom:4px}
    .subtitle{color:#666;font-size:13px;margin-bottom:20px}
    .theatre-tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px}
    .theatre-btn{padding:7px 14px;border-radius:6px;border:none;cursor:pointer;font-size:12px;background:#e8e8e8;color:#333;font-weight:400}
    .theatre-btn.active{background:#1a1a2e;color:#fff;font-weight:700}
    .theatre-btn.error{opacity:.55}
    .table-wrap{max-width:720px}
    table{width:100%;border-collapse:collapse;font-size:14px;background:#fff}
    thead tr{background:#1a1a2e;color:#fff}
    thead th{padding:10px 12px;font-weight:600;cursor:pointer;user-select:none;white-space:nowrap;font-size:13px}
    thead th:hover{background:#2a2a3e}
    thead th.center{text-align:center} thead th.right{text-align:right}
    tbody tr{border-bottom:1px solid #eee}
    tbody tr.even{background:#f9f9f9} tbody tr.odd{background:#fff}
    tbody tr.angel-row{background:#fff8e1!important}
    tbody td{padding:8px 12px;vertical-align:middle}
    .rank-cell{text-align:center;font-weight:600} .dist-cell{text-align:center}
    .dist-badge{background:#f59e0b;color:#fff;border-radius:4px;padding:2px 7px;font-weight:700;font-size:12px}
    .angel-title{font-weight:700}
    .gross-cell{text-align:right;font-family:monospace}
    tfoot tr{background:#1a1a2e;color:#fff;font-weight:700}
    tfoot td{padding:10px 12px} tfoot td.right{text-align:right;font-family:monospace}
    .sort-arrow{font-size:10px;margin-left:4px}
    .hint{font-size:12px;color:#999;margin-top:10px}
    .error-msg{padding:20px 0;color:#888;font-size:13px}
    .angel-summary{background:#fff8e1;border:1px solid #f59e0b;border-radius:8px;padding:14px 18px;margin-bottom:24px;max-width:520px}
    .angel-summary h3{font-size:12px;color:#92400e;margin-bottom:10px;font-weight:700;letter-spacing:.05em;text-transform:uppercase}
    .angel-summary table{background:transparent;font-size:13px}
    .angel-summary thead tr{background:transparent;color:#92400e}
    .angel-summary thead th{padding:3px 10px 6px;font-size:11px;border-bottom:1px solid #f59e0b;cursor:default;font-weight:600}
    .angel-summary thead th.right{text-align:right} .angel-summary thead th.center{text-align:center}
    .angel-summary tbody tr{border-bottom:1px solid #fde68a;background:transparent!important}
    .angel-summary tbody td{padding:5px 10px}
    .angel-summary tfoot tr{background:transparent;color:#78350f}
    .angel-summary tfoot td{padding:7px 10px;border-top:2px solid #f59e0b;font-size:13px;font-weight:700}
    .angel-summary tfoot td.right{text-align:right;font-family:monospace}`;

  // Inline script built with string concat — no nested template literals
  const js = [
    'const ALL_THEATRES=' + JSON.stringify(results) + ';',
    'let selectedIdx=0,sortKey="rank",sortDir="asc";',
    'const fmt=n=>n?"$"+Number(n).toLocaleString():"--";',
    'function sortData(f){return[...f].sort((a,b)=>{let v;if(sortKey==="title")v=(a.title||"").localeCompare(b.title||"");else if(sortKey==="dist")v=(a.dist||"").localeCompare(b.dist||"");else v=(a[sortKey]||0)-(b[sortKey]||0);return sortDir==="asc"?v:-v;});}',
    'function arrow(k){return sortKey!==k?"":(sortDir==="asc"?" ▲":" ▼");}',
    'function toggleSort(k){if(sortKey===k){sortDir=sortDir==="asc"?"desc":"asc";}else{sortKey=k;sortDir=k==="gross"?"desc":"asc";}renderTable();}',
    'function renderSummary(){',
    '  var box=document.getElementById("summary-box");',
    '  var attraction="";',
    '  for(var i=0;i<ALL_THEATRES.length;i++){var af=(ALL_THEATRES[i].films||[]).find(function(f){return f.is_angel;});if(af){attraction=af.title;break;}}',
    '  if(!attraction){box.innerHTML="";return;}',
    '  var rows=ALL_THEATRES.filter(function(t){return t.status==="ok";}).map(function(t){var af=(t.films||[]).find(function(f){return f.is_angel;});if(!af)return null;return{theatre:t.theatre||t.ml_venue,rank:af.rank,titles:t.films.length,gross:af.gross};}).filter(Boolean);',
    '  if(!rows.length){box.innerHTML="";return;}',
    '  var totalGross=rows.reduce(function(s,r){return s+r.gross;},0);',
    '  var nLoc=rows.length;',
    '  var tbody=rows.map(function(r){return "<tr><td>"+r.theatre+"</td><td style=\'text-align:center\'>"+r.rank+"</td><td style=\'text-align:center\'>"+r.titles+"</td><td style=\'text-align:right;font-family:monospace\'>"+fmt(r.gross)+"</td></tr>";}).join("");',
    '  box.innerHTML="<div class=\'angel-summary\'><h3>"+attraction+" — Flash Gross Summary</h3><table><thead><tr><th style=\'text-align:left\'>Theatre</th><th class=\'center\'>Rank</th><th class=\'center\'>Titles</th><th class=\'right\'>Week Gross</th></tr></thead><tbody>"+tbody+"</tbody><tfoot><tr><td colspan=\'3\'>TOTAL &nbsp;("+nLoc+" location"+(nLoc!==1?"s":"")+")</td><td class=\'right\'>"+fmt(totalGross)+"</td></tr></tfoot></table></div>";',
    '}',
    'function renderTabs(){',
    '  var c=document.getElementById("tabs");c.innerHTML="";',
    '  ALL_THEATRES.forEach(function(t,i){',
    '    var b=document.createElement("button");',
    '    b.className="theatre-btn"+(i===selectedIdx?" active":"")+(t.status!=="ok"?" error":"");',
    '    b.textContent=t.theatre||t.ml_venue||"Unit "+t.unit;',
    '    b.title=t.status!=="ok"?(t.error||t.status):t.ml_venue;',
    '    b.addEventListener("click",function(){selectedIdx=i;sortKey="rank";sortDir="asc";renderTabs();renderTable();});',
    '    c.appendChild(b);',
    '  });',
    '}',
    'function renderTable(){',
    '  var t=ALL_THEATRES[selectedIdx];',
    '  var c=document.getElementById("table-container");',
    '  if(!t.films||t.films.length===0){c.innerHTML="<p class=\'error-msg\'>"+(t.error||"No data.")+"</p>";return;}',
    '  var s=sortData(t.films);',
    '  var tot=t.films.reduce(function(a,r){return a+(r.gross||0);},0);',
    '  var cols=[{key:"rank",label:"Rank",cls:"center"},{key:"title",label:"Title",cls:""},{key:"dist",label:"Dist",cls:"center"},{key:"gross",label:"Week Gross",cls:"right"}];',
    '  var th="<thead><tr>"+cols.map(function(col){return "<th class=\'"+col.cls+"\' onclick=\'toggleSort(\\""+col.key+"\\")\'>"+col.label+"<span class=\'sort-arrow\'>"+arrow(col.key)+"</span></th>";}).join("")+"</tr></thead>";',
    '  var rows=s.map(function(r,i){',
    '    var angel=r.is_angel;',
    '    var distCell=angel?"<span class=\'dist-badge\'>"+r.dist+"</span>":r.dist;',
    '    return "<tr class=\'"+(angel?"angel-row":i%2===0?"even":"odd")+"\'><td class=\'rank-cell\'>"+r.rank+"</td><td class=\'"+(angel?"angel-title":"")+"\'>"+ r.title+"</td><td class=\'dist-cell\'>"+distCell+"</td><td class=\'gross-cell\'>"+fmt(r.gross)+"</td></tr>";',
    '  }).join("");',
    '  c.innerHTML="<table>"+th+"<tbody>"+rows+"</tbody><tfoot><tr><td colspan=\'3\'>TOTAL</td><td class=\'right\'>"+fmt(tot)+"</td></tr></tfoot></table>";',
    '}',
    'renderSummary();renderTabs();renderTable();'
  ].join('\n');

  const html = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">'
    + '<title>Box Office Flash - Week of ' + weekFmt + '</title>'
    + '<style>' + css + '</style></head><body>'
    + '<h2>Box Office Flash \u2013 Week of ' + weekFmt + '</h2>'
    + '<p class="subtitle" id="subtitle">' + ATTRACTION + ' &nbsp;&middot;&nbsp; FINAL locations &nbsp;&middot;&nbsp; Generated ' + now + '</p>'
    + '<div id="summary-box"></div>'
    + '<div class="theatre-tabs" id="tabs"></div>'
    + '<div class="table-wrap"><div id="table-container"></div><p class="hint">Click column headers to sort.</p></div>'
    + '<script>' + js + '<\/script>'
    + '</body></html>';

  // ── Step 4: Download ──────────────────────────────────────────────────────
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([html],{type:"text/html"}));
  a.download = "flash_gross_dashboard.html";
  a.click();
  console.log("Done! Dashboard downloaded.");

})();
