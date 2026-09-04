"""Multi-program registry for the internal cross-species KG demo.

Each program is a prospect intake (pig / poultry / cattle). Programs share the
same intake schema, so one builder seeds them all. Literature enrichment +
harmonization then run per-program, and a master-merge unions them into a
cross-species graph where shared entities (C. perfringens, sialidase, mucin)
become single nodes linked to multiple programs.

INTERNAL ONLY — contains named-prospect strategy. Never publish.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Program:
    slug: str
    company: str
    disease: str
    species: str
    problem: str
    # Literature search queries
    queries: list[str] = field(default_factory=list)
    # Seed entities from the intake (targets, pathogens, constraints, killed, etc.)
    seed_nodes: list[dict] = field(default_factory=list)
    seed_edges: list[dict] = field(default_factory=list)


# ─── Program definitions ─────────────────────────────────────────────────────

PROGRAMS: dict[str, Program] = {}


def _register(p: Program) -> None:
    PROGRAMS[p.slug] = p


# Elanco — broiler coccidiosis (Eimeria)
_register(Program(
    slug="elanco-coccidiosis",
    company="Elanco",
    disease="Broiler coccidiosis (Eimeria)",
    species="poultry",
    problem="Broad-spectrum non-ionophore anticoccidial, NAE-compatible, pellet-stable, covering E. acervulina/maxima/tenella.",
    queries=[
        "Eimeria broiler coccidiosis anticoccidial",
        "Eimeria tenella acervulina maxima chicken",
        "ionophore anticoccidial resistance poultry",
        "non-ionophore anticoccidial mechanism",
        "Eimeria oocyst sporozoite invasion",
        "coccidiosis vaccine broiler live attenuated",
        "anticoccidial natural product botanical poultry",
        "Eimeria apical complex rhoptry microneme",
        "lesion scoring anticoccidial index broiler",
        "coccidiosis gut integrity necrotic enteritis link",
        "Eimeria calcium signaling egress",
        "poultry feed additive coccidiostat pelleting",
        "Eimeria drug target apicoplast",
        "sporozoite invasion inhibitor Eimeria",
        "Eimeria immune evasion host cell",
        "anticoccidial peptide antimicrobial poultry",
        "Eimeria energy metabolism mitochondrion target",
        "broiler gut microbiome coccidiosis",
        "Eimeria genome drug discovery target",
        "quorum sensing gut pathogen poultry",
    ],
    seed_nodes=[
        {"id": "program:elanco-coccidiosis", "name": "Broiler coccidiosis program", "label": "Program",
         "description": "Elanco — non-ionophore broad-spectrum anticoccidial, NAE-compatible, pellet-stable"},
        {"id": "disease:coccidiosis", "name": "Broiler coccidiosis", "label": "Disease"},
        {"id": "pathogen:eimeria-acervulina", "name": "Eimeria acervulina", "label": "Organism"},
        {"id": "pathogen:eimeria-maxima", "name": "Eimeria maxima", "label": "Organism"},
        {"id": "pathogen:eimeria-tenella", "name": "Eimeria tenella", "label": "Organism"},
        {"id": "constraint:non-ionophore", "name": "Non-ionophore mechanism", "label": "Constraint"},
        {"id": "constraint:nae", "name": "NAE-compatible (non-antibiotic)", "label": "Constraint"},
        {"id": "constraint:pellet-stable", "name": "Pellet-stable", "label": "Constraint"},
        {"id": "benchmark:ionophores", "name": "Ionophores (state of art)", "label": "Benchmark"},
        {"id": "assay:lesion-scoring", "name": "Lesion scoring", "label": "Assay"},
        {"id": "assay:oocyst-output", "name": "Oocyst output", "label": "Assay"},
        {"id": "assay:anticoccidial-index", "name": "Anticoccidial index", "label": "Assay"},
    ],
    seed_edges=[
        {"source_id": "pathogen:eimeria-acervulina", "target_id": "disease:coccidiosis", "type": "CAUSES"},
        {"source_id": "pathogen:eimeria-maxima", "target_id": "disease:coccidiosis", "type": "CAUSES"},
        {"source_id": "pathogen:eimeria-tenella", "target_id": "disease:coccidiosis", "type": "CAUSES"},
        {"source_id": "program:elanco-coccidiosis", "target_id": "disease:coccidiosis", "type": "ADDRESSES"},
        {"source_id": "program:elanco-coccidiosis", "target_id": "constraint:non-ionophore", "type": "CONSTRAINED_BY"},
    ],
))

# Elanco — broiler necrotic enteritis (C. perfringens)
_register(Program(
    slug="elanco-necrotic-enteritis",
    company="Elanco",
    disease="Broiler necrotic enteritis (C. perfringens)",
    species="poultry",
    problem="Control C. perfringens-driven necrotic enteritis; linked to coccidiosis as predisposing factor.",
    queries=[
        "Clostridium perfringens necrotic enteritis broiler",
        "C perfringens alpha toxin NetB poultry",
        "necrotic enteritis chicken gut lesion",
        "C perfringens virulence factor toxin",
        "necrotic enteritis vaccine toxoid broiler",
        "C perfringens collagenase mucinase",
        "clostridial toxin neutralization gut",
        "necrotic enteritis predisposing coccidiosis",
        "C perfringens quorum sensing Agr",
        "poultry gut health Clostridium control",
        "NetB toxin pore-forming mechanism",
        "C perfringens sporulation enterotoxin",
        "probiotic Clostridium perfringens exclusion poultry",
        "necrotic enteritis feed additive alternative antibiotic",
        "C perfringens adhesion intestinal mucin",
        "clostridial collagenase inhibitor",
        "necrotic enteritis butyrate short chain fatty acid",
        "C perfringens iron acquisition target",
        "bacteriophage Clostridium perfringens poultry",
        "necrotic enteritis immune modulation broiler",
    ],
    seed_nodes=[
        {"id": "program:elanco-necrotic-enteritis", "name": "Necrotic enteritis program", "label": "Program",
         "description": "Elanco — C. perfringens control, linked to coccidiosis"},
        {"id": "disease:necrotic-enteritis", "name": "Necrotic enteritis", "label": "Disease"},
        {"id": "pathogen:c-perfringens", "name": "Clostridium perfringens", "label": "Organism"},
        {"id": "target:netb", "name": "NetB toxin", "label": "Target"},
        {"id": "target:cp-alpha-toxin", "name": "C. perfringens alpha toxin", "label": "Target"},
        {"id": "target:cp-collagenase", "name": "C. perfringens collagenase", "label": "Target",
         "description": "UniProt P43153"},
    ],
    seed_edges=[
        {"source_id": "pathogen:c-perfringens", "target_id": "disease:necrotic-enteritis", "type": "CAUSES"},
        {"source_id": "target:netb", "target_id": "pathogen:c-perfringens", "type": "PART_OF"},
        {"source_id": "disease:coccidiosis", "target_id": "disease:necrotic-enteritis", "type": "PREDISPOSES"},
        {"source_id": "program:elanco-necrotic-enteritis", "target_id": "disease:necrotic-enteritis", "type": "ADDRESSES"},
    ],
))

# Zoetis — bovine mastitis
_register(Program(
    slug="zoetis-mastitis",
    company="Zoetis",
    disease="Bovine mastitis",
    species="cattle",
    problem="Cure bovine mastitis across gram-positive, gram-negative, and intracellular reservoirs, ideally without direct bactericidal action.",
    queries=[
        "bovine mastitis treatment dairy cattle",
        "Staphylococcus aureus mastitis intracellular",
        "mastitis gram-negative E coli Klebsiella",
        "mastitis Streptococcus uberis dysgalactiae",
        "bovine mammary gland immune response infection",
        "mastitis biofilm intramammary",
        "anti-virulence mastitis non-bactericidal",
        "mastitis host-directed therapy immunomodulation",
        "bovine mastitis vaccine Staph aureus",
        "mastitis somatic cell count biomarker",
        "intramammary infusion antibiotic alternative",
        "S aureus small colony variant persistence",
        "mastitis neutrophil recruitment mammary",
        "bovine mastitis phage therapy",
        "mastitis quorum sensing agr Staph",
        "mammary epithelial cell infection model",
        "mastitis antimicrobial peptide defensin",
        "bovine mastitis economic dairy dry cow",
        "intracellular pathogen clearance macrophage udder",
        "mastitis toxin leukocidin neutralization",
    ],
    seed_nodes=[
        {"id": "program:zoetis-mastitis", "name": "Bovine mastitis program", "label": "Program",
         "description": "Zoetis — cross-reservoir cure, ideally non-bactericidal"},
        {"id": "disease:mastitis", "name": "Bovine mastitis", "label": "Disease"},
        {"id": "pathogen:s-aureus", "name": "Staphylococcus aureus", "label": "Organism"},
        {"id": "pathogen:e-coli-mastitis", "name": "E. coli (mastitis)", "label": "Organism"},
        {"id": "pathogen:s-uberis", "name": "Streptococcus uberis", "label": "Organism"},
        {"id": "constraint:non-bactericidal", "name": "Non-bactericidal preferred", "label": "Constraint"},
        {"id": "assay:somatic-cell-count", "name": "Somatic cell count", "label": "Assay"},
    ],
    seed_edges=[
        {"source_id": "pathogen:s-aureus", "target_id": "disease:mastitis", "type": "CAUSES"},
        {"source_id": "pathogen:e-coli-mastitis", "target_id": "disease:mastitis", "type": "CAUSES"},
        {"source_id": "pathogen:s-uberis", "target_id": "disease:mastitis", "type": "CAUSES"},
        {"source_id": "program:zoetis-mastitis", "target_id": "disease:mastitis", "type": "ADDRESSES"},
    ],
))

# Alltech BLINDED-B — keeps aim + enzyme CLASSES, strips specific proteins +
# named compounds. Eval: does literature enrichment recover NanI/H/J, the
# UniProt-IDd enzymes, and the ~40 inhibitor compounds on its own?
_register(Program(
    slug="alltech-blinded",
    company="Alltech (blinded eval)",
    disease="GI mucin-layer protection (swine/poultry)",
    species="swine,poultry",
    problem="Block microbial/viral enzymes (sialidases, chitinases, collagenases) that hydrolyze GI mucin and enable pathogen adhesion.",
    queries=[
        # Same query set as the full program — the LITERATURE is identical;
        # only the SEED differs (no specific proteins/compounds).
        "bacterial sialidase neuraminidase inhibitor",
        "Clostridium perfringens sialidase NanI NanH NanJ",
        "chitinase inhibitor allosamidin argifin",
        "bacterial collagenase inhibitor",
        "intestinal mucin degradation pathogen adhesion",
        "sialic acid mucin host pathogen interaction",
        "neuraminidase inhibitor oseltamivir zanamivir",
        "flavonoid sialidase inhibition quercetin luteolin",
        "mucin O-glycosylation intestinal barrier",
        "influenza neuraminidase drug resistance mutation",
        "mucin layer gut protection feed additive",
        "sialidase virulence factor gut bacteria",
        "chitinase bacterial pathogenesis intestinal",
        "collagenase Clostridium tissue degradation",
        "natural product neuraminidase inhibitor plant",
        "curcumin capsaicin collagenase inhibition",
        "mucinase pathogen intestinal colonization",
        "sialoglycan pathogen receptor gut",
        "Salmonella chitinase host colonization",
        "PRRS influenza swine sialic acid receptor",
    ],
    seed_nodes=[
        {"id": "program:alltech-blinded", "name": "Mucin-protection program (blinded)", "label": "Program",
         "description": "Alltech blinded eval — aim + enzyme classes only, no specific targets/compounds"},
        {"id": "mech:mucin-protection-b", "name": "Mucin-layer protection", "label": "Mechanism"},
        # Enzyme CLASSES kept (Scenario B), but NO specific NanI/H/J, NO UniProt IDs
        {"id": "class:sialidase-b", "name": "Sialidase (neuraminidase)", "label": "Mechanism"},
        {"id": "class:chitinase-b", "name": "Chitinase", "label": "Mechanism"},
        {"id": "class:collagenase-b", "name": "Collagenase", "label": "Mechanism"},
        # Constraints from the intake (non-infringing, selectivity)
        {"id": "constraint:non-infringing", "name": "Non-infringing composition", "label": "Constraint"},
        {"id": "constraint:selectivity", "name": "Avoid off-target metalloprotease inhibition", "label": "Constraint"},
        {"id": "constraint:resistance", "name": "Overcome sialidase-inhibitor resistance", "label": "Constraint"},
    ],
    seed_edges=[
        {"source_id": "program:alltech-blinded", "target_id": "mech:mucin-protection-b", "type": "PRIORITIZES"},
        {"source_id": "class:sialidase-b", "target_id": "mech:mucin-protection-b", "type": "PART_OF"},
        {"source_id": "class:chitinase-b", "target_id": "mech:mucin-protection-b", "type": "PART_OF"},
        {"source_id": "class:collagenase-b", "target_id": "mech:mucin-protection-b", "type": "PART_OF"},
    ],
))
_register(Program(
    slug="alltech-mucin",
    company="Alltech",
    disease="GI mucin-layer protection (swine/poultry)",
    species="swine,poultry",
    problem="Block microbial/viral enzymes (sialidases, chitinases, collagenases) that hydrolyze GI mucin and enable pathogen adhesion.",
    queries=[
        "bacterial sialidase neuraminidase inhibitor",
        "Clostridium perfringens sialidase NanI NanH NanJ",
        "chitinase inhibitor allosamidin argifin",
        "bacterial collagenase inhibitor",
        "intestinal mucin degradation pathogen adhesion",
        "sialic acid mucin host pathogen interaction",
        "neuraminidase inhibitor oseltamivir zanamivir",
        "flavonoid sialidase inhibition quercetin luteolin",
        "mucin O-glycosylation intestinal barrier",
        "influenza neuraminidase drug resistance mutation",
        "mucin layer gut protection feed additive",
        "sialidase virulence factor gut bacteria",
        "chitinase bacterial pathogenesis intestinal",
        "collagenase Clostridium tissue degradation",
        "natural product neuraminidase inhibitor plant",
        "curcumin capsaicin collagenase inhibition",
        "mucinase pathogen intestinal colonization",
        "sialoglycan pathogen receptor gut",
        "Salmonella chitinase host colonization",
        "PRRS influenza swine sialic acid receptor",
    ],
    seed_nodes=[
        {"id": "program:alltech-mucin", "name": "Mucin-protection program", "label": "Program",
         "description": "Alltech — inhibit mucin-degrading enzymes to block pathogen adhesion"},
        {"id": "mech:mucin-protection", "name": "Mucin-layer protection", "label": "Mechanism"},
        # Enzyme targets (with UniProt from the intake)
        {"id": "target:cp-sialidase-nani", "name": "C. perfringens sialidase NanI", "label": "Target", "description": "UniProt A0A0H2YQR1"},
        {"id": "target:cp-sialidase-nanh", "name": "C. perfringens sialidase NanH", "label": "Target", "description": "UniProt A0A0H2YSM2"},
        {"id": "target:cp-sialidase-nanj", "name": "C. perfringens sialidase NanJ", "label": "Target", "description": "UniProt A0A0H2YT71"},
        {"id": "target:cp-collagenase", "name": "C. perfringens collagenase", "label": "Target", "description": "UniProt P43153"},
        {"id": "target:cp-chitinase", "name": "C. perfringens chitinase ChiA/ChiB", "label": "Target", "description": "UniProt A0AB37C3C8"},
        {"id": "target:ecoli-chitinase", "name": "E. coli chitinase ChiA", "label": "Target", "description": "UniProt P13656"},
        {"id": "target:salmonella-chitinase", "name": "Salmonella chitinase ChiA", "label": "Target", "description": "UniProt A0A379TN84"},
        {"id": "target:salmonella-collagenase", "name": "Salmonella collagenase", "label": "Target", "description": "UniProt Q8Z5Q0"},
        {"id": "target:flu-neuraminidase", "name": "Influenza A neuraminidase", "label": "Target", "description": "UniProt Q710U6 / B3EUQ9"},
        # Enzyme classes
        {"id": "class:sialidase", "name": "Sialidase (neuraminidase)", "label": "Mechanism"},
        {"id": "class:chitinase", "name": "Chitinase", "label": "Mechanism"},
        {"id": "class:collagenase", "name": "Collagenase", "label": "Mechanism"},
        # Named inhibitor compounds (the gold-standard candidate list)
        {"id": "cpd:oseltamivir", "name": "Oseltamivir", "label": "Compound"},
        {"id": "cpd:zanamivir", "name": "Zanamivir", "label": "Compound"},
        {"id": "cpd:quercetin", "name": "Quercetin", "label": "Compound"},
        {"id": "cpd:luteolin", "name": "Luteolin", "label": "Compound"},
        {"id": "cpd:apigenin", "name": "Apigenin", "label": "Compound"},
        {"id": "cpd:artocarpin", "name": "Artocarpin", "label": "Compound"},
        {"id": "cpd:allosamidin", "name": "Allosamidin", "label": "Compound"},
        {"id": "cpd:argifin", "name": "Argifin", "label": "Compound"},
        {"id": "cpd:curcumin", "name": "Curcumin", "label": "Compound"},
        {"id": "cpd:capsaicin", "name": "Capsaicin", "label": "Compound"},
    ],
    seed_edges=[
        {"source_id": "program:alltech-mucin", "target_id": "mech:mucin-protection", "type": "PRIORITIZES"},
        # sialidase inhibitors
        {"source_id": "cpd:oseltamivir", "target_id": "class:sialidase", "type": "INHIBITS"},
        {"source_id": "cpd:zanamivir", "target_id": "class:sialidase", "type": "INHIBITS"},
        {"source_id": "cpd:quercetin", "target_id": "class:sialidase", "type": "INHIBITS"},
        {"source_id": "cpd:luteolin", "target_id": "class:sialidase", "type": "INHIBITS"},
        {"source_id": "cpd:apigenin", "target_id": "class:sialidase", "type": "INHIBITS"},
        {"source_id": "cpd:artocarpin", "target_id": "class:sialidase", "type": "INHIBITS"},
        # chitinase inhibitors
        {"source_id": "cpd:allosamidin", "target_id": "class:chitinase", "type": "INHIBITS"},
        {"source_id": "cpd:argifin", "target_id": "class:chitinase", "type": "INHIBITS"},
        # collagenase inhibitors
        {"source_id": "cpd:curcumin", "target_id": "class:collagenase", "type": "INHIBITS"},
        {"source_id": "cpd:capsaicin", "target_id": "class:collagenase", "type": "INHIBITS"},
        # enzyme class membership
        {"source_id": "target:cp-sialidase-nani", "target_id": "class:sialidase", "type": "PART_OF"},
        {"source_id": "target:cp-sialidase-nanh", "target_id": "class:sialidase", "type": "PART_OF"},
        {"source_id": "target:cp-sialidase-nanj", "target_id": "class:sialidase", "type": "PART_OF"},
        {"source_id": "target:cp-collagenase", "target_id": "class:collagenase", "type": "PART_OF"},
        {"source_id": "target:salmonella-collagenase", "target_id": "class:collagenase", "type": "PART_OF"},
        {"source_id": "target:cp-chitinase", "target_id": "class:chitinase", "type": "PART_OF"},
        {"source_id": "target:ecoli-chitinase", "target_id": "class:chitinase", "type": "PART_OF"},
        {"source_id": "target:salmonella-chitinase", "target_id": "class:chitinase", "type": "PART_OF"},
        {"source_id": "target:flu-neuraminidase", "target_id": "class:sialidase", "type": "PART_OF"},
        # C. perfringens links to necrotic enteritis (CROSS-PROGRAM edge)
        {"source_id": "target:cp-collagenase", "target_id": "pathogen:c-perfringens", "type": "PART_OF"},
    ],
))
