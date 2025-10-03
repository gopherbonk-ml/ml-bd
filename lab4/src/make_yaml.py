import argparse, os
from pathlib import Path
import yaml

CLASSES = ["whole", "broken"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--images", required=True)
    ap.add_argument("--labels", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--val-split", type=float, default=0.15)
    args = ap.parse_args()

    images = Path(args.images).resolve()
    labels = Path(args.labels).resolve()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    data = {
        'path': str(images.parent),
        'train': str(images.relative_to(images.parent)),
        'val': str(images.relative_to(images.parent)),
        'names': {i: n for i, n in enumerate(CLASSES)}
    }
    with open(out, 'w') as f:
        yaml.safe_dump(data, f, sort_keys=False)
    print(f"Wrote {out}")

if __name__ == "__main__":
    main()