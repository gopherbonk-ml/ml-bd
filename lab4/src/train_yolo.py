import argparse
from ultralytics import YOLO
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", required=True, help="yolo/data.yaml")
    ap.add_argument("--epochs", type=int, default=50)
    ap.add_argument("--imgsz", type=int, default=640)
    ap.add_argument("--name", default="skeet-yolo")
    ap.add_argument("--model", default="yolov8n.pt", help="base model (n/s/m/l/x)")
    args = ap.parse_args()

    model = YOLO(args.model)
    res = model.train(data=args.cfg, epochs=args.epochs, imgsz=args.imgsz, name=args.name)

    runs_dir = Path("runs/detect") / args.name
    best = next((runs_dir / "weights" / "best.pt").glob("*") if (runs_dir/"weights").exists() else [])
    out = Path("yolo/weights"); out.mkdir(parents=True, exist_ok=True)
    try:
        (runs_dir / "weights" / "best.pt").replace(out / "best.pt")
    except Exception:
        pass
    print("Training complete. Weights at", out / "best.pt")

if __name__ == "__main__":
    main()