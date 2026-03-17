# Stage 4 Answer — EU AI Act (Regulation (EU) 2024/1689)

**Question**: Are the obligations of a provider of a general purpose model limited if they only fine-tune it?

**Q&A mode**: standard
**Facts collected**: 14, from Articles 1, 2, 3, 10, 11, 16, 25, 50, 51, 53, 54, 55 and Annexes XI, XII, XIII

---

## Answer

No. The provider obligations for a general-purpose AI ("GPAI") model are not *generally* limited just because the provider "only" fine-tunes an existing model rather than training from scratch.

### 1) Obligations turn on "provider" status and market/service triggers — not on scratch training vs. fine-tuning

The Regulation's applicability and operator obligations primarily depend on whether an entity qualifies as a relevant operator (notably a "provider") and whether it places a GPAI model/AI system on the Union market or puts it into service (including certain extraterritorial situations tied to use of outputs in the Union). That analysis does **not** hinge on whether the model was trained from scratch or fine-tuned. (Article 1; Article 2)

A "provider" includes an entity that develops (or has developed) a GPAI model and places it on the market or puts it into service under its own name/trademark; an entity that fine-tunes a GPAI model and then makes the resulting model/system available under its own name can therefore qualify as a provider. Fine-tuning **by itself** does not automatically trigger provider obligations unless the fine-tuner meets the provider definition and the placing/putting-into-service trigger. (Article 3)

### 2) If the fine-tuner is the provider, the baseline GPAI provider obligations apply in full

Where the fine-tuner is a "provider of a GPAI model," the baseline obligations in Article 53(1) apply (e.g., technical documentation; downstream information/documentation; copyright compliance policy; public summary of training content; plus cooperation duties). These obligations are not reduced merely because the provider's work consisted of fine-tuning rather than end-to-end training. (Article 53)

The documentation obligations also contemplate and can capture fine-tuning: Annex XI requires technical documentation appropriate to the model's size/risk profile, and for systemic-risk GPAI models it expressly includes documentation of "model adaptations, including alignment and fine-tuning" (where applicable). (Annex XI) Downstream documentation to integrators is governed by Annex XII (with certain dataset-related elements only "where applicable"). (Annex XII)

### 3) The Regulation provides *specific* limitation carve-outs — but they are not "fine-tuning" carve-outs

Provider obligations can be limited in certain defined cases — most notably for qualifying free/open-source releases. In particular, Article 53(1)(a) and (b) do **not** apply to providers releasing AI models under a qualifying free and open-source licence; and the EU authorised representative requirement for third-country providers is similarly waived for such qualifying open-source releases. These limitations are **not** tied to whether the provider fine-tuned; they are tied to the licensing/release conditions. (Article 53; Article 54)

These carve-outs also do **not** apply to GPAI models with systemic risk. (Article 53; Article 54)

### 4) If the fine-tuned model has "systemic risk", obligations may increase (not decrease)

If the fine-tuned model qualifies as a GPAI model with "systemic risk," the provider has additional duties (evaluation/adversarial testing, systemic risk mitigation, serious incident reporting, cybersecurity). Again, these depend on systemic-risk status and being the provider — not on whether the provider trained from scratch or fine-tuned. (Article 55)

**Uncertainty to note:** the presumption of "high impact capabilities" references "cumulative amount of computation used for its training" (>10^25 FLOPs), but the materials do not clarify whether post-training fine-tuning compute counts toward that metric. This matters because systemic-risk classification drives additional obligations. (Article 51; Annex XI; Annex XIII)

### 5) Fine-tuning can shift "provider" status for *high-risk AI systems* via "substantial modification"

If the fine-tuning occurs in the context of a **high-risk AI system** and amounts to a "substantial modification," the modifying actor can be deemed the provider and becomes subject to the high-risk provider obligations. Fine-tuning can be relevant if it affects compliance or intended purpose. (Article 3; Article 25)

### Bottom line

Fine-tuning does **not** create a reduced obligation track. If, after fine-tuning, the actor qualifies as a "provider" and places the resulting GPAI model/system on the market or puts it into service, the standard GPAI provider obligations apply (and may expand if the model is systemic-risk). Any limitation comes from specific statutory carve-outs (e.g., qualifying open-source releases) or scope exclusions (e.g., R&D prior to placing on the market), not from the fact of fine-tuning itself. (Article 3; Article 53; Article 54; Article 2)
