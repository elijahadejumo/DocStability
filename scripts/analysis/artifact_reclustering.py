from pathlib import Path

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score

OUT = str(Path(__file__).resolve().parents[2] / "analysis_outputs")
cmp_df = pd.read_csv(f"{OUT}/artifact_stratified_vs_combined.csv", index_col="repo")

X = cmp_df[["entropy_living", "awr_living"]].values
sil_scores = {}
for k in range(2, 7):
    km = KMeans(n_clusters=k, n_init=10, random_state=42)
    labels = km.fit_predict(X)
    sil_scores[k] = silhouette_score(X, labels)
print("Silhouette scores for living-only re-clustering:")
for k, s in sil_scores.items():
    print(f"  k={k}: {s:.3f}")

best_k = max(sil_scores, key=sil_scores.get)
km = KMeans(n_clusters=3, n_init=10, random_state=42)
labels3 = km.fit_predict(X)
cmp_df["cluster_living_k3"] = labels3

# rank clusters by mean entropy to assign archetype names, matching original convention
order = cmp_df.groupby("cluster_living_k3")["entropy_living"].mean().sort_values(ascending=False).index.tolist()
name_map = {order[0]: "Consistent", order[1]: "Occasional", order[2]: "Sparse"}
cmp_df["archetype_living"] = cmp_df["cluster_living_k3"].map(name_map)

print(f"\nBest silhouette k = {best_k}")
print("\nLiving-only archetype distribution:")
print(cmp_df["archetype_living"].value_counts())
print("\nOriginal (combined) archetype distribution:")
print(cmp_df["archetype_all"].value_counts())

ari = adjusted_rand_score(cmp_df["archetype_all"], cmp_df["archetype_living"])
agree = (cmp_df["archetype_all"] == cmp_df["archetype_living"]).mean()
print(f"\nAdjusted Rand Index (combined vs living-only archetypes): {ari:.3f}")
print(f"Raw label agreement: {agree:.1%}")

print("\nCrosstab:")
print(pd.crosstab(cmp_df["archetype_all"], cmp_df["archetype_living"]))

disagreements = cmp_df[cmp_df["archetype_all"] != cmp_df["archetype_living"]]
print(f"\n{len(disagreements)} repos changed archetype label:")
print(disagreements[["entropy_all","entropy_living","archetype_all","archetype_living","n_static","n_living"]].sort_values("n_static", ascending=False))

# outlier: highest static-doc share
cmp_df["static_share"] = cmp_df["n_static"].fillna(0) / (cmp_df["n_living"].fillna(0) + cmp_df["n_static"].fillna(0))
print("\nTop 5 repos by static-doc share of doc commits:")
print(cmp_df.sort_values("static_share", ascending=False)[["static_share","n_living","n_static","archetype_all"]].head())

cmp_df.to_csv(f"{OUT}/artifact_reclustering_result.csv")
