"""
Silhouette Heatmap — Entropy + Active Window Rate Combined
===========================================================
Paper: Beyond Code: Characterizing Documentation Governance
       and Coordination in Open Source Projects

PURPOSE
-------
Displays silhouette scores for the combined Entropy +
Active Window Rate feature set across k = 2 to k = 6.
Justifies the selection of k=3 as the number of archetypes.

Only the combined feature set is shown because the
methodology section already establishes the theoretical
and empirical rationale for combining both metrics.
The individual metric rows are intentionally excluded
to keep the figure focused on a single argument:
k=3 is the peak for this combination.

OUTPUTS
-------
  silhouette_heatmap_combined_v2.pdf
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
DATA_PATH   = "combined_doc_done.csv"   # update path if needed
OUTPUT_PATH = "silhouette_heatmap_combined_v2.pdf"
FEATURES    = ['entropy_norm', 'active_window_rate']
K_RANGE     = list(range(2, 7))         # k = 2, 3, 4, 5, 6
SELECTED_K  = 3
RANDOM_SEED = 42

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
df = pd.read_csv(DATA_PATH)
print(f"Loaded {len(df)} repositories.\n")

# ─────────────────────────────────────────────
# COMPUTE SILHOUETTE SCORES
# ─────────────────────────────────────────────
scaler = StandardScaler()
X      = scaler.fit_transform(df[FEATURES])

scores = []
for k in K_RANGE:
    km = KMeans(n_clusters=k, random_state=RANDOM_SEED, n_init=10)
    km.fit(X)
    scores.append(round(silhouette_score(X, km.labels_), 4))

print("Entropy + ActiveWin silhouette scores:")
for k, s in zip(K_RANGE, scores):
    marker = "  ← selected" if k == SELECTED_K else ""
    print(f"  k={k}  {s:.4f}{marker}")

sel_col = K_RANGE.index(SELECTED_K)
best_k  = K_RANGE[scores.index(max(scores))]

# ─────────────────────────────────────────────
# FIGURE
# ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 2.6))
fig.patch.set_facecolor('white')
ax.set_facecolor('white')

matrix = np.array([scores])

# Blues colormap — professional, clean, print-friendly
im = ax.imshow(matrix, cmap='Blues', aspect='auto',
               vmin=min(scores) - 0.02,
               vmax=max(scores) + 0.02)

# ── Axis labels ──
ax.set_xticks(range(len(K_RANGE)))
ax.set_xticklabels([f'k = {k}' for k in K_RANGE],
                   color='#1a1a1a', fontsize=13, fontweight='bold')
ax.set_yticks([0])
ax.set_yticklabels(['Entropy + Active\nWindow Rate'],
                   color='#1a1a1a', fontsize=11, fontweight='bold')
ax.tick_params(colors='#1a1a1a', length=0, pad=12)
for spine in ax.spines.values():
    spine.set_edgecolor('#DDDDDD')

# ── Annotate cells ──
for j, score in enumerate(scores):
    is_selected = (j == sel_col)
    is_best     = (K_RANGE[j] == best_k)
    symbol      = ' ★' if is_best else ''
    weight      = 'bold' if is_selected else 'normal'
    fontsize    = 15 if is_selected else 12
    # White text on darker cells, dark text on lighter cells
    text_color  = 'white' if score >= 0.58 else '#1a1a1a'

    ax.text(j, 0, f'{score:.3f}{symbol}',
            ha='center', va='center',
            color=text_color,
            fontsize=fontsize,
            fontweight=weight)

# ── Selected cell border (k=3) ──
ax.add_patch(plt.Rectangle(
    (sel_col - 0.5, -0.5), 1, 1,
    linewidth=4, edgecolor='#1F4E79',
    facecolor='none', zorder=6
))

# ── Colorbar ──
cbar = plt.colorbar(im, ax=ax, pad=0.015,
                    shrink=0.88, aspect=10)
cbar.ax.yaxis.set_tick_params(color='#333333', labelsize=9)
cbar.ax.set_ylabel('Silhouette Score',
                   color='#333333', fontsize=9)
plt.setp(cbar.ax.yaxis.get_ticklabels(), color='#333333')
cbar.outline.set_edgecolor('#CCCCCC')

# ── Legend ──
legend_elements = [
    mpatches.Patch(facecolor='#1F4E79', edgecolor='#1F4E79',
                   label=f'Selected: k=3  '
                         f'(silhouette = {scores[sel_col]:.3f})'),
    plt.Line2D([0], [0], marker='*', color='#1a1a1a',
               linestyle='None', markersize=12,
               label='★  peak silhouette'),
]
ax.legend(handles=legend_elements,
          loc='lower left',
          bbox_to_anchor=(0.0, -0.62),
          facecolor='white', labelcolor='#1a1a1a',
          framealpha=0.95, fontsize=10.5, ncol=2,
          edgecolor='#CCCCCC')

plt.tight_layout()
plt.savefig(OUTPUT_PATH, dpi=300, bbox_inches='tight',
            facecolor='white', format='pdf')
plt.close()
print(f"\nSaved: {OUTPUT_PATH}")