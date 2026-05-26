"""Zero-shot text prompts for Body Part QC seeding.

Prompts are intentionally short and use plain English (BiomedCLIP was
trained on PubMed image captions, which are usually short clinical
phrases). For each user-defined category we keep:

- ``positive``  prompts that describe the category
- ``negative`` prompts that describe other body parts the cohort might
  contain — these stabilize the softmax against generic features.

The default catalogue covers the four V1 categories (Brain, Brain-Neck,
Spine, Chest); custom user categories fall back to two simple prompts
("MRI of the {category}" / "axial MRI scan of the {category}") plus the
union of all default negatives, so the seeder still works without code
changes when the user adds a category.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CategoryPrompts:
    positive: tuple[str, ...]
    negative: tuple[str, ...]


# Negative prompts shared across all categories — describe the most
# common confusable body parts.
_GENERIC_NEGATIVES: tuple[str, ...] = (
    "MRI of the abdomen",
    "MRI of the pelvis",
    "MRI of the knee",
    "MRI of the breast",
    "x-ray scout image",
    "localizer image",
)


DEFAULT_PROMPTS: dict[str, CategoryPrompts] = {
    "Brain": CategoryPrompts(
        positive=(
            "axial MRI scan of the brain",
            "sagittal MRI scan of the brain",
            "coronal MRI scan of the brain",
            "MRI showing brain parenchyma and skull",
            "head MRI showing the cerebrum",
        ),
        negative=(
            "MRI of the cervical spine",
            "MRI of the thoracic spine",
            "MRI of the lumbar spine",
            "MRI of the chest",
            "MRI showing the neck and trachea",
        ) + _GENERIC_NEGATIVES,
    ),
    "Brain-Neck": CategoryPrompts(
        positive=(
            "sagittal MRI showing the brain and the cervical spine",
            "MRI showing the brain stem and the upper neck",
            "MRI showing the head and the cervical region",
            "MRI of the head and neck",
        ),
        negative=(
            "axial MRI of the brain only",
            "MRI of the lumbar spine",
            "MRI of the thoracic spine",
            "MRI of the chest",
        ) + _GENERIC_NEGATIVES,
    ),
    "Spine": CategoryPrompts(
        positive=(
            "sagittal MRI of the spine",
            "MRI of the cervical spine",
            "MRI of the thoracic spine",
            "MRI of the lumbar spine",
            "MRI showing vertebrae and spinal cord",
        ),
        negative=(
            "axial MRI of the brain",
            "MRI of the head only",
            "MRI of the chest",
        ) + _GENERIC_NEGATIVES,
    ),
    "Chest": CategoryPrompts(
        positive=(
            "MRI of the chest",
            "thoracic MRI showing the heart and lungs",
            "axial MRI of the thorax",
        ),
        negative=(
            "MRI of the brain",
            "MRI of the cervical spine",
            "MRI of the lumbar spine",
            "MRI of the abdomen",
        ) + _GENERIC_NEGATIVES,
    ),
}


def prompts_for_category(category: str) -> CategoryPrompts:
    """Return the prompt pack for a category.

    For unknown (user-defined) categories, returns a generic pack that
    interpolates the category name and reuses the union of all default
    negatives.
    """
    if category in DEFAULT_PROMPTS:
        return DEFAULT_PROMPTS[category]

    cat = category.strip()
    pos = (
        f"MRI of the {cat}",
        f"axial MRI scan of the {cat}",
        f"MRI showing the {cat}",
    )
    # Union of all default negatives (deduped, preserving order).
    seen: set[str] = set()
    neg: list[str] = []
    for pack in DEFAULT_PROMPTS.values():
        for n in pack.negative:
            if n not in seen:
                seen.add(n)
                neg.append(n)
    return CategoryPrompts(positive=pos, negative=tuple(neg))


def all_categories() -> list[str]:
    return list(DEFAULT_PROMPTS.keys())
