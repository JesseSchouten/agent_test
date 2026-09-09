# MediaMarkt — Omnichannel Margin Intelligence

## The Story

| | |
|---|---|
| **Company** | MediaMarkt — Europe's largest consumer electronics retailer |
| **Hero** | Anna Vermeer, Category Manager for TV & Audio |
| **Problem** | After matching a competitor's OLED price drop across all channels: units +22%, revenue +9%, but category gross profit −6% — the margin leak isn't the discount |
| **Investigation** | Anna asks three questions in Genie: weekly margin by channel, online basket composition, fulfilment cost and return rate by channel |
| **Root cause** | The price match shifted ~60% of volume online, bypassing the in-store attach engine (accessories, wall mounts, installation, extended warranty: 41% attach in-store vs 12% online) plus higher fulfilment cost and return rate on home-delivered large-format |
| **Impact** | ~€1.2M annualized margin erosion from channel-mix shift, not from the price match itself |

---

## Overview

March. Anna matched a competitor's price cut on large-format OLED TVs within 48 hours, across every channel — online, in-store, click & collect. Four weeks later the scorecard is confusing: units are up 22%, revenue is up 9%, but category gross profit is down 6%. Her assumption: the discount went too deep.

It didn't. Three questions in Genie tell a different story. In-store margin held steady — the collapse is entirely online. The online basket is thin: attach rate on accessories, wall mounts, professional installation and extended warranty is 12% versus 41% in-store. The price match pushed roughly 60% of the volume online — straight past the profit engine. On top of that, home delivery on large-format TVs and a materially higher online return rate shave another few points.

The insight: the price match was the right commercial call. The channel mix was the problem, and nobody owned that number. The action: keep matching, but route it — push click & collect on large-format, bundle attach into the online flow, and set margin floors per channel rather than per SKU.

**Duration:** 5–7 minutes

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Unit growth after price match | +22% |
| Revenue growth | +9% |
| Category gross profit change | −6% |
| In-store attach rate | ~41% |
| Online attach rate | ~12% |
| Volume shifted online | ~60% |
| Online return rate (large-format) | ~14% vs ~4% in-store |
| Annualized margin erosion | ~€1.2M |
| Price match timing | ~4 weeks ago (peak 3 weeks ago) |

---

## Demo Walkthrough

**Frame:** Monday morning category review. Anna opens the margin intelligence dashboard and sees the scorecard divergence — units and revenue up, profit down.

---

### Act 1 — The Confusing Scorecard (1 min)

**Open the AI/BI Dashboard.**

KPI cards show the divergence at a glance: units +22%, revenue +9%, gross profit −6%. The weekly margin trend shows a clear inflection point 4 weeks ago when the price match went live — in-store margin stays flat, online margin drops sharply.

> *"This is the 5-second test. Something clearly happened — and the two lines (in-store vs online) immediately tell you the discount isn't the problem. The channel is."*

---

### Act 2 — Three Questions in Genie (3 min)

**Question 1:** `Show weekly TV margin before and after the price match, split by channel.`

→ In-store margin holds. Online margin collapses. The divergence started the week of the price match.

**Question 2:** `What changed in the online basket?`

→ Attach rate on accessories, wall mounts, installation, and extended warranty is 41% in-store and 12% online. The price match pushed ~60% of volume online — past the attach engine.

**Question 3:** `What's fulfilment cost and return rate per order by channel?`

→ Home delivery on large-format TVs costs 3–4x click & collect. Online return rate on large-format is ~14% versus ~4% in-store — returns on a €2,000 TV are expensive.

> *"Three questions, no SQL, no ticket to the BI team. **Genie** translates Anna's business language into governed queries over the **Gold tables** that **SDP** built from the ERP, POS, and e-commerce feeds **Lakeflow Connect** ingested. **Unity Catalog** enforces that Anna sees only her categories."*

---

### Act 3 — The Action (1 min)

Anna now knows the price match was correct — it defended market share. The problem is unowned: channel-mix economics.

Actions:
- Push click & collect for large-format online orders (reduces fulfilment cost and return rate)
- Bundle accessories and installation into the online purchase flow (closes the attach gap)
- Set margin floors per channel, not per SKU — so the next price match auto-routes to the profitable channel

> *"The data didn't just explain the past — it changed the operating model. And it took three questions, not three weeks."*

---

### Closing

> The margin leak wasn't in the pricing decision — it was in the channel mix nobody was watching. **Lakeflow Connect** brought POS, e-commerce, logistics, and returns data together. **SDP** shaped it into a medallion architecture with channel-level margin as a first-class entity. From there, a dashboard showed the divergence at a glance, and **Genie** let a category manager — not an analyst — trace it to root cause in three questions. **Unity Catalog** governed every step. **The insight that saved €1.2M/year lived in the join across four systems. No single one held the answer.**

---

## Products Showcased

"Build" = a resource we provision in the workspace. "Talk track" = a platform capability we mention live but don't build per-demo.

| Product | Mode | What it does in this demo |
|---------|------|---------------------------|
| **SDP Pipeline** | Build | Bronze → Silver → Gold medallion architecture: ingests raw POS transactions, e-commerce orders, logistics/fulfilment, and returns data; produces Gold tables with channel-level margin, attach rates, fulfilment cost, and return rates |
| **Synthetic Data Generation** | Build | Generates realistic MediaMarkt transaction data with the channel-mix story baked in: in-store attach at 41%, online at 12%, volume shift after price match, differential return rates and fulfilment costs |
| **Lakeflow Connect** | Talk track | Pulls POS, e-commerce (webshop), ERP, and logistics data into the lakehouse — the feeds that make the channel-level join possible |
| **Unity Catalog** | Talk track | One permission model from raw POS data through Gold margin tables — Anna sees only her categories, finance sees the P&L roll-up |
| **Genie One** | Talk track | The business-user entry point — Anna asks her three margin questions without writing SQL |
| **Genie Code** | Talk track | The development copilot that accelerated building the SDP pipeline and dashboard |
