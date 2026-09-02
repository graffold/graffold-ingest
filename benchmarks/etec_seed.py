"""ETEC-pigs program: seed the institutional-knowledge backbone.

Encodes the intake form as graph entities:
- Program, disease forms, pathogens (F18, F4)
- Targets/mechanisms (FedF, FaeG, LT/ST enterotoxins, GM1, tight junctions)
- Internal active programs (differentiate, don't duplicate)
- KILLED nodes (dead ends — do not re-propose)
- Constraints (feed-additive-only, pellet-stable, no-vaccine/small-molecule)
- Assays (FITC-D, TEER, IPEC-1/J2, adhesion)
- Hypotheses to test (LTB+FedF combo)
- State-of-art benchmark (ZnO)

Run once before literature enrichment.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from graffold_ingest.connectors.base import ExtractionResult
from graffold_ingest.pipeline.publish_parquet import publish_to_parquet

OUTPUT_DIR = Path.home() / ".graffold" / "parquet" / "etec-pigs"

# ─── Entities ────────────────────────────────────────────────────────────────

NODES = [
    # Program
    {"id": "program:etec-pigs", "name": "Post-weaning ETEC in pigs", "label": "Program",
     "description": "Reduce post-weaning diarrhea from F18/F4 ETEC by disarming virulence, feed-additive pathway, delivered at weaning (19-24d)"},

    # Disease forms
    {"id": "disease:pwd", "name": "Post-weaning diarrhea", "label": "Disease",
     "description": "Primary target: F18-driven, onset ~21d (weaning)"},
    {"id": "disease:edema", "name": "Edema disease", "label": "Disease",
     "description": "F18 STx2e-mediated, secondary scope"},
    {"id": "disease:neonatal-scours", "name": "Neonatal scours", "label": "Disease",
     "description": "F4-associated, pre-weaning"},

    # Pathogens / strains
    {"id": "pathogen:f18-etec", "name": "F18 ETEC", "label": "Organism",
     "description": "Enterotoxigenic E. coli, F18 fimbriae, primary target, impacts pigs >=21d"},
    {"id": "pathogen:f4-etec", "name": "F4 ETEC", "label": "Organism",
     "description": "Enterotoxigenic E. coli, F4 (K88) fimbriae"},

    # Adhesins / targets
    {"id": "target:fedf", "name": "FedF", "label": "Target",
     "description": "F18 fimbrial adhesin tip subunit — mediates F18 attachment to porcine gut"},
    {"id": "target:faeg", "name": "FaeG", "label": "Target",
     "description": "F4 fimbrial adhesin subunit — structural/functional analog of FedF"},

    # Enterotoxins (untapped area)
    {"id": "target:lt", "name": "Heat-labile enterotoxin (LT)", "label": "Target",
     "description": "ETEC LT toxin; LTB subunit binds GM1 ganglioside"},
    {"id": "target:st", "name": "Heat-stable enterotoxin (ST)", "label": "Target",
     "description": "ETEC STa/STb toxins — drive secretory diarrhea"},
    {"id": "target:ltb", "name": "LTB (LT B subunit)", "label": "Target",
     "description": "Non-toxic B subunit; binds GM1 on porcine epithelium; potential adjuvant+adhesion enhancer"},
    {"id": "target:stx2e", "name": "Shiga toxin STx2e", "label": "Target",
     "description": "Edema disease toxin"},

    # Receptors / host factors
    {"id": "target:gm1", "name": "GM1 ganglioside", "label": "Target",
     "description": "Apical porcine epithelial receptor for LTB"},
    {"id": "target:tight-junctions", "name": "Tight junction proteins", "label": "Target",
     "description": "Barrier integrity readout (occludin, ZO-1); ETEC increases permeability"},
    {"id": "target:apn", "name": "Aminopeptidase N (APN)", "label": "Target",
     "description": "Candidate F4 ETEC receptor on enterocytes"},

    # Mechanisms
    {"id": "mech:adhesion", "name": "Fimbrial adhesion", "label": "Mechanism",
     "description": "F18/F4 attachment to intestinal epithelium — primary intervention point"},
    {"id": "mech:enterotoxin-neutralization", "name": "Enterotoxin neutralization", "label": "Mechanism",
     "description": "UNTAPPED: bind/inactivate LT/ST toxins via feed additive"},
    {"id": "mech:competitive-exclusion", "name": "Competitive exclusion", "label": "Mechanism",
     "description": "Block receptor engagement (physical) + immune exclusion at mucosa"},
    {"id": "mech:barrier-protection", "name": "Barrier protection", "label": "Mechanism",
     "description": "Reduce cell permeability / preserve tight junctions"},

    # Internal ACTIVE programs (differentiate)
    {"id": "internal:gm-bacillus-fedf", "name": "GM Bacillus expressing FedF", "label": "InternalProgram",
     "description": "ACTIVE: Bacillus modified to display FedF, competes for F18 binding; swine challenge showed improved performance; IPEC-1 adhesion test planned"},
    {"id": "internal:soluble-mannans", "name": "Soluble mannans (yeast culture)", "label": "InternalProgram",
     "description": "ACTIVE: dose-dependent F4 adhesion reduction in IPEC-J2; screening with FedF Bacillus fall"},
    {"id": "internal:mcfa-blends", "name": "Custom MCFA blends", "label": "InternalProgram",
     "description": "PARTNER: no MIC below 0.62% vs F18 strains (inclusion rate too high)"},

    # KILLED / dead ends (do NOT re-propose)
    {"id": "killed:organic-acids", "name": "Organic acids", "label": "Killed",
     "description": "KILLED: decades on market, cannot control F18 alone"},
    {"id": "killed:carvacrol", "name": "Carvacrol", "label": "Killed",
     "description": "KILLED: extensively studied, not efficacious vs F18; MIC too low at acceptable inclusion"},
    {"id": "killed:eo-blends", "name": "Essential oil blends", "label": "Killed",
     "description": "KILLED: most not efficacious enough to replace ZnO vs F18"},
    {"id": "killed:bacillus-blends", "name": "Standard Bacillus probiotic blends", "label": "Killed",
     "description": "KILLED: not effective vs F18 ETEC"},
    {"id": "killed:chelated-zinc", "name": "Chelated zinc / zinc nanoparticles", "label": "Killed",
     "description": "KILLED: well-covered, redundant — do not spend cycles"},
    {"id": "killed:insoluble-fiber", "name": "Insoluble fiber (wheat bran)", "label": "Killed",
     "description": "KILLED: not proprietary, no differentiation"},
    {"id": "killed:gut-health-generic", "name": "Generic gut-health / pathogen-killing", "label": "Killed",
     "description": "KILLED: broad approaches haven't worked vs F18 ETEC"},

    # Benchmark
    {"id": "benchmark:zno", "name": "Zinc oxide (ZnO)", "label": "Benchmark",
     "description": "STATE OF THE ART — the bar to beat, even where banned"},

    # Constraints
    {"id": "constraint:feed-additive", "name": "Feed-additive pathway only", "label": "Constraint",
     "description": "Zootechnical/feed-additive approval; no FDA drug pathway; no PK/withdrawal"},
    {"id": "constraint:pellet-stable", "name": "Pellet-stable to 71C", "label": "Constraint",
     "description": "Nursery diets pelleted; product must survive 160F/71C OR be water-soluble"},
    {"id": "constraint:no-vaccine", "name": "No vaccine", "label": "Constraint",
     "description": "Excluded modality (US: autogenous only)"},
    {"id": "constraint:no-small-molecule", "name": "No small molecule", "label": "Constraint",
     "description": "Excluded modality (5-7yr, $10-15M dev cost)"},
    {"id": "constraint:2-3-molecules", "name": "Max 2-3 molecules per submission", "label": "Constraint",
     "description": "If specific molecules (not multi-ingredient formula)"},

    # Modalities (ranked)
    {"id": "modality:dfm", "name": "Direct-fed microbial / probiotic", "label": "Modality",
     "description": "RANK 1 preferred modality"},
    {"id": "modality:peptide", "name": "Peptide", "label": "Modality",
     "description": "RANK 2 preferred modality"},
    {"id": "modality:natural-product", "name": "Natural product / botanical", "label": "Modality",
     "description": "RANK 3 preferred modality"},

    # Assays (efficacy readouts they trust)
    {"id": "assay:fitc-d", "name": "FITC-D permeability", "label": "Assay",
     "description": "Cell permeability (leakiness) readout"},
    {"id": "assay:teer", "name": "TEER", "label": "Assay",
     "description": "Transepithelial electrical resistance — barrier integrity"},
    {"id": "assay:ipec-1", "name": "IPEC-1 cells", "label": "Assay",
     "description": "Porcine intestinal epithelial model — trusted for F18"},
    {"id": "assay:ipec-j2", "name": "IPEC-J2 cells", "label": "Assay",
     "description": "Porcine model — used for F4 adhesion (F18 if attachment shown)"},
    {"id": "assay:adhesion", "name": "E. coli adhesion assay", "label": "Assay",
     "description": "Fimbrial attachment readout"},
    {"id": "assay:tight-junction-expr", "name": "Tight junction protein expression", "label": "Assay",
     "description": "Occludin/ZO-1 expression readout"},

    # Hypothesis to test
    {"id": "hypothesis:ltb-fedf", "name": "LTB + FedF-displaying platform combo", "label": "Hypothesis",
     "description": "TO TEST: LTB binds GM1 → enhances adhesion + mucosal immunogenicity of FedF construct (analogous to FaeG+LTB F4 work, Yu 2017/2024)"},
    {"id": "hypothesis:toxin-binding-additive", "name": "Feed-borne enterotoxin binder", "label": "Hypothesis",
     "description": "TO TEST: something fed that binds/inactivates LT/ST toxins"},
]

# ─── Relationships ───────────────────────────────────────────────────────────

EDGES = [
    # Pathogen → disease
    {"source_id": "pathogen:f18-etec", "target_id": "disease:pwd", "type": "CAUSES"},
    {"source_id": "pathogen:f18-etec", "target_id": "disease:edema", "type": "CAUSES"},
    {"source_id": "pathogen:f4-etec", "target_id": "disease:neonatal-scours", "type": "CAUSES"},
    {"source_id": "pathogen:f4-etec", "target_id": "disease:pwd", "type": "CAUSES"},

    # Adhesins belong to pathogens
    {"source_id": "target:fedf", "target_id": "pathogen:f18-etec", "type": "PART_OF"},
    {"source_id": "target:faeg", "target_id": "pathogen:f4-etec", "type": "PART_OF"},
    {"source_id": "target:fedf", "target_id": "mech:adhesion", "type": "MEDIATES"},
    {"source_id": "target:faeg", "target_id": "mech:adhesion", "type": "MEDIATES"},

    # Toxins
    {"source_id": "target:lt", "target_id": "pathogen:f18-etec", "type": "PRODUCED_BY"},
    {"source_id": "target:st", "target_id": "pathogen:f18-etec", "type": "PRODUCED_BY"},
    {"source_id": "target:ltb", "target_id": "target:lt", "type": "PART_OF"},
    {"source_id": "target:ltb", "target_id": "target:gm1", "type": "BINDS"},
    {"source_id": "target:stx2e", "target_id": "disease:edema", "type": "CAUSES"},
    {"source_id": "target:apn", "target_id": "pathogen:f4-etec", "type": "RECEPTOR_FOR"},

    # Internal programs → mechanisms
    {"source_id": "internal:gm-bacillus-fedf", "target_id": "target:fedf", "type": "USES"},
    {"source_id": "internal:gm-bacillus-fedf", "target_id": "mech:competitive-exclusion", "type": "MECHANISM"},
    {"source_id": "internal:soluble-mannans", "target_id": "mech:adhesion", "type": "REDUCES"},
    {"source_id": "internal:mcfa-blends", "target_id": "pathogen:f18-etec", "type": "TARGETS"},

    # Kills → what they tried against
    {"source_id": "killed:organic-acids", "target_id": "pathogen:f18-etec", "type": "FAILED_AGAINST"},
    {"source_id": "killed:carvacrol", "target_id": "pathogen:f18-etec", "type": "FAILED_AGAINST"},
    {"source_id": "killed:eo-blends", "target_id": "pathogen:f18-etec", "type": "FAILED_AGAINST"},
    {"source_id": "killed:bacillus-blends", "target_id": "pathogen:f18-etec", "type": "FAILED_AGAINST"},

    # Benchmark
    {"source_id": "benchmark:zno", "target_id": "disease:pwd", "type": "TREATS"},

    # Assays read out mechanisms
    {"source_id": "assay:fitc-d", "target_id": "mech:barrier-protection", "type": "MEASURES"},
    {"source_id": "assay:teer", "target_id": "mech:barrier-protection", "type": "MEASURES"},
    {"source_id": "assay:adhesion", "target_id": "mech:adhesion", "type": "MEASURES"},
    {"source_id": "assay:tight-junction-expr", "target_id": "target:tight-junctions", "type": "MEASURES"},

    # Hypotheses
    {"source_id": "hypothesis:ltb-fedf", "target_id": "target:ltb", "type": "USES"},
    {"source_id": "hypothesis:ltb-fedf", "target_id": "target:fedf", "type": "USES"},
    {"source_id": "hypothesis:ltb-fedf", "target_id": "mech:competitive-exclusion", "type": "MECHANISM"},
    {"source_id": "hypothesis:toxin-binding-additive", "target_id": "mech:enterotoxin-neutralization", "type": "MECHANISM"},
    {"source_id": "hypothesis:toxin-binding-additive", "target_id": "target:lt", "type": "TARGETS"},

    # Program scope
    {"source_id": "program:etec-pigs", "target_id": "disease:pwd", "type": "ADDRESSES"},
    {"source_id": "program:etec-pigs", "target_id": "constraint:feed-additive", "type": "CONSTRAINED_BY"},
    {"source_id": "program:etec-pigs", "target_id": "constraint:pellet-stable", "type": "CONSTRAINED_BY"},
    {"source_id": "program:etec-pigs", "target_id": "mech:enterotoxin-neutralization", "type": "PRIORITIZES"},
]


async def main():
    results = [ExtractionResult(nodes=NODES, edges=EDGES, source_doc_id="etec:intake-form:v1")]
    counts = await publish_to_parquet(results, output_dir=OUTPUT_DIR, run_id="seed-intake")
    print(f"Seeded ETEC backbone: {counts['entities_written']} entities, {counts['relationships_written']} relationships")
    print(f"  → {OUTPUT_DIR}")
    print(f"  KILLED nodes: 7  |  Internal programs: 3  |  Hypotheses: 2  |  Constraints: 5")


if __name__ == "__main__":
    asyncio.run(main())
