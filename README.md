# EdgeFlowManager

**The Complete Guide to Edge Flow Management** — by Steven Matison.

> Start here: **[guide/index.md](guide/index.md)** — the full guide (intro, table of contents, and every chapter).

Edge Flow Management is the central manager for MiNiFi agent Classes, Resources, and Edge Flows.
NiFi in the datacenter is well documented; EFM is not. This guide is the map for what happens at the
edge — binary delivery, agent enrollment, which processors exist in which build, custom processors,
Site-to-Site, AI at the edge, and real-world demos. Every chapter is built and run on real hardware.

## Layout

- **`guide/`** — the published guide: chapters + `guide/assets/` figures.
- **`files/`** — runnable artifacts the guide references: EFM/MiNiFi flow exports (`files/efm/`),
  custom Python processors (`files/efm-python-processor-*`), the Site-to-Site lab (`files/site-to-site/`),
  and agent / TensorRT inference scripts.
- **`images/`** — figures used across the guide.
