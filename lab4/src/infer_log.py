import argparse, os, csv, glob
from pathlib import Path
import cv2
import numpy as np
from ultralytics import YOLO

CLASS_NAMES = {0: "whole", 1: "broken"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--videos", required=True)
    ap.add_argument("--weights", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--every", type=float, default=0.5)
    ap.add_argument("--conf", type=float, default=0.25)
    ap.add_argument("--target-type", default="unknown", choices=["skeet","trap","unknown"])
    args = ap.parse_args()

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    model = YOLO(args.weights)

    vids = sorted(glob.glob(os.path.join(args.videos, "**/*.MP4"), recursive=True))
    for v in vids:
        cap = cv2.VideoCapture(v)
        if not cap.isOpened():
            print("Cannot open", v); continue
        fps = cap.get(cv2.CAP_PROP_FPS) or 30
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        step = max(1, int(round(fps * args.every)))

        csv_path = outdir / (Path(v).stem + ".csv")
        with open(csv_path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(["video_file","t_sec","target_type","x_rel","y_rel","state","conf"])

            for fidx in range(0, total, step):
                cap.set(cv2.CAP_PROP_POS_FRAMES, fidx)
                ok, frame = cap.read()
                if not ok:
                    break
                h, w0 = frame.shape[:2]

                res = model.predict(frame, conf=args.conf, verbose=False)[0]
                best = None
                for b in res.boxes:
                    cls = int(b.cls.item()); conf = float(b.conf.item())
                    if cls not in CLASS_NAMES: continue
                    x1,y1,x2,y2 = b.xyxy[0].tolist()
                    cx = (x1+x2)/2.0; cy = (y1+y2)/2.0
                    # choose highest conf
                    if (best is None) or (conf > best['conf']):
                        best = {
                            'cls': cls,
                            'conf': conf,
                            'cx_rel': cx / w0,
                            'cy_rel': cy / h
                        }

                t_sec = fidx / fps
                if best is None:
                    w.writerow([os.path.basename(v), f"{t_sec:.2f}", args.target_type, -1, -1, "unknown", 0.0])
                else:
                    w.writerow([os.path.basename(v), f"{t_sec:.2f}", args.target_type,
                                f"{best['cx_rel']:.4f}", f"{best['cy_rel']:.4f}", CLASS_NAMES[best['cls']],
                                f"{best['conf']:.3f}"])
        cap.release()
        print("Wrote", csv_path)

if __name__ == "__main__":
    main()