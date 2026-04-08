# Texas Multi-County Motivated Seller Leads

Automated scraper collecting motivated seller leads across **11 Texas counties**:
Harris, Dallas, Tarrant, Bexar, Travis, Collin, Hidalgo, Denton, Fort Bend, Montgomery, and Tyler.

## Lead types collected
`LP` · `NOFC` · `TAXDEED` · `JUD` · `CCJ` · `DRJUD` · `LNCORPTX` · `LNIRS` · `LNFED` · `LN` · `LNMECH` · `LNHOA` · `MEDLN` · `PRO` · `NOC` · `RELLP`

## Live dashboard
**https://sailakshmi01.github.io/tx-motivated-seller-leads/**

## How it works
- **Daily scraper** runs via GitHub Actions at 7am UTC
- Fetches deed/lien/judgment records from each county's public records portal
- Scores each lead 0–100 based on distress signals (foreclosure + lien combos, amounts, recency, LLC ownership)
- Commits updated `records.json` and deploys dashboard to GitHub Pages
- Exports a GHL-compatible CSV (`data/ghl_export.csv`)

## Run manually
In the **Actions** tab → **Texas Multi-County Lead Scraper** → **Run workflow**

Optionally filter counties: `tyler,harris,dallas,tarrant,bexar,travis,collin,hidalgo,denton,fortbend,montgomery`

## County portals
| County | Population | Portal |
|---|---|---|
| Harris | ~4.7M | cclerk.hctx.net (Fidlar iDOC) |
| Dallas | ~2.6M | dallas.tx.publicsearch.us |
| Tarrant | ~2.1M | tarrant.tx.publicsearch.us |
| Bexar | ~2.0M | bexar.tx.publicsearch.us |
| Travis | ~1.3M | travis.tx.publicsearch.us |
| Collin | ~1.1M | collin.tx.publicsearch.us |
| Hidalgo | ~900K | hidalgo.tx.publicsearch.us |
| Denton | ~900K | denton.tx.publicsearch.us |
| Fort Bend | ~800K | fortbend.tx.publicsearch.us |
| Montgomery | ~700K | montgomery.tx.publicsearch.us |
| Tyler | ~21K | co.tyler.tx.us/page/tyler.Forclosures |

## Seller score formula
Base 30 · +10 per distress flag · +20 LP+NOFC combo · +15 amount>$100k · +10 amount>$50k · +5 new this week · +5 has address · +10 LLC/corp owner · Max 100
