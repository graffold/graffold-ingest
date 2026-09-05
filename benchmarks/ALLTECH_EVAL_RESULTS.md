# Alltech Gold-Standard Evaluation

**Question:** Given only Alltech's high-level direction (block mucin-degrading
enzymes: sialidases / chitinases / collagenases), can graffold's 1000-paper
literature enrichment independently recover the specific targets and compounds
their expert team identified — and add new ones?

**Scenario B (guided):** the blinded seed keeps the aim + three enzyme *classes*
but strips the specific NanI/H/J proteins, all UniProt IDs, and the ~27 named
inhibitor compounds. Same 1000-paper literature corpus as the full run.

## Result

| Metric | Blinded (literature only) | Full (seeded w/ answers) |
|--------|:-------------------------:|:------------------------:|
| Enzyme classes | **4/4** | 4/4 |
| Specific targets | **6/9** | 8/9 |
| Named compounds | **8/27** | 13/27 |
| New targets surfaced | **91** | 97 |
| Evidence citations | 287 | 313 |
| Harmonized entities | 2,351 | 2,489 |

### What the blinded graph recovered on its own

**Targets (6/9):** all three C. perfringens sialidases (NanI, NanH, NanJ) by
name, C. perfringens collagenase, C. perfringens chitinase, E. coli chitinase
ChiA — the core mucin-degradation machinery Alltech prioritized.

**Compounds (8/27):** the marquee neuraminidase inhibitors (oseltamivir,
zanamivir, peramivir), top antisialidase flavonoids (quercetin, apigenin,
kaempferol), plus **siastatin** — which the *full seeded* run did not surface.

**Missed (3):** Salmonella chitinase, Salmonella collagenase, influenza
neuraminidase — thinner in the pig/poultry-gut literature the queries pulled.
Honest gaps, addressable with Salmonella/influenza-specific queries.

### Augmentation — 91 new candidate targets Alltech didn't list

Including beta-N-acetylglucosaminidase, GH29 glycosyl hydrolases,
hemagglutinin/protease, carbohydrate-binding metalloprotease, gelatinase —
several directly on-thesis (mucin/glycan-degrading enzymes) and worth a look.

## Takeaway

Given a one-paragraph direction, graffold recovered the majority of an expert
team's specific target list **and their headline compounds**, backed each with
literature citations, and proposed ~90 additional candidates — in ~35 minutes
for ~$5 of compute. It did not need the answers to find them.

*Reproduce:*
```bash
python benchmarks/multi_program.py enrich alltech-blinded --papers 1000
python benchmarks/alltech_eval.py ~/.graffold/parquet/alltech-blinded-harmonized
```

*INTERNAL — contains named-prospect evaluation. Not for publication.*


## Scenario A (fully blinded) — added after B

Seed: aim only ("protect gut mucin, block pathogen adhesion"). NO enzyme
classes, NO targets, NO compounds. Thesis-agnostic queries (no sialidase/
oseltamivir in query strings — that would leak the answer).

| Metric | A (aim only) | B (enzyme classes) |
|--------|:---:|:---:|
| Enzyme classes | 2/4 | 4/4 |
| Specific targets | 5/9 | 6/9 |
| Named compounds | 3/27 | 8/27 |
| New targets | 75 | 91 |

Even fully blinded, A recovered all three C. perfringens sialidases
(NanI/H/J) by name + chitinase from just "protect mucin." The B>A gap is
the honest, expected result: the enzyme-class hint steers the search into
the right neighborhood. Story: "point us in the general direction and we
recover most of it; zero hints and we still find half."
