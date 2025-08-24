from conductor_sdk import ctx
import numpy as np
import matplotlib.pyplot as plt

# Load by basename (auto-resolves under request_root/data/**)
df = ctx.read_csv("data/Operetta_objectresults.csv")
df["Row"] = df["Row"].astype(str)
df["Column"] = df["Column"].astype(int)

plate = (
    df.groupby(["Row", "Column"])["Nuclei - Cas9_Intensity Mean"]
      .mean()
      .unstack("Column")
      .sort_index()
      .sort_index(axis=1)
)

fig, ax = plt.subplots(figsize=(plate.shape[1]*0.5+2, plate.shape[0]*0.5+2))
im = ax.imshow(np.ma.masked_invalid(plate.values), aspect="equal", origin="upper")
ax.set_title("Average Nuclei - Cas9_Intensity Mean")
ax.set_xlabel("Column"); ax.set_ylabel("Row")
ax.set_xticks(range(len(plate.columns))); ax.set_xticklabels(plate.columns.astype(int))
ax.set_yticks(range(len(plate.index)));  ax.set_yticklabels(plate.index)
ax.grid(False)
cbar = plt.colorbar(im, ax=ax); cbar.set_label("Nuclei - Cas9_Intensity Mean")
plt.tight_layout()

ctx.publish_figure(fig, "plate_heatmap.png",
                   title="Cas9 intensity heatmap",
                   description="Average Nuclei - Cas9_Intensity Mean per well")
plt.close(fig)
